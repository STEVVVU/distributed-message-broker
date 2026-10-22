import unittest
from unittest import mock
from pathlib import Path
import tempfile
from types import SimpleNamespace
import verify
import io
import urllib.error
import benchmark
from benchmark import payload, cells, confirmed_rpc, readiness_probe, record_request_event, post_fault_confirmations
from benchmark_support import quantiles


class BenchmarkTests(unittest.TestCase):
    def test_payload_size_and_identity(self):
        for size in [64, 1024, 16384]:
            value=payload(421,size)
            self.assertEqual(len(value.encode()),size)
            self.assertEqual(int(value[:10]),421)
            self.assertEqual(value,payload(421,size))

    def test_axis_coverage(self):
        self.assertEqual({c['consumers'] for c in cells()},{1,4,16})
        self.assertEqual({c['producers'] for c in cells()},{1,4,16})
        self.assertEqual(quantiles([2,1,3])['p99'],3)

    def test_posix_cluster_launch_owns_the_group_used_by_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            cluster=verify.Cluster.__new__(verify.Cluster)
            cluster.path=Path(directory)
            cluster.processes=[]
            cluster.record=lambda *args,**kwargs:None
            with mock.patch.object(verify,'os',SimpleNamespace(name='posix')), mock.patch.object(verify.subprocess,'Popen') as popen:
                popen.return_value.pid=123
                cluster.start({'name':'node'})
                self.assertIs(popen.call_args.kwargs['start_new_session'],True)

    def test_lost_publish_reply_retries_same_id_and_keeps_uncertainty(self):
        events=[];identities=[]
        def publish():
            identities.append('same-publisher-id')
            if len(identities)==1:
                raise urllib.error.HTTPError('http://127.0.0.1/messages',503,'unavailable',{},io.BytesIO(b'{"detail":"nats: timeout","capability":"synthetic-secret"}'))
            return dict(sequence=7,duplicate=True)
        with mock.patch.object(verify.time,'sleep'):
            ack,attempts=confirmed_rpc(publish,events.append,'publish',retry=True,index=1,publisher_id='same-publisher-id')
        self.assertEqual(identities,['same-publisher-id','same-publisher-id'])
        self.assertEqual((ack['sequence'],ack['duplicate'],attempts),(7,True,2))
        self.assertEqual([e['phase'] for e in events],['publish_attempted','publish_uncertain','publish_attempted','publish_confirmed'])
        self.assertEqual(events[1]['detail_code'],'nats_timeout')
        self.assertNotIn('synthetic-secret',str(events))

    def test_ack_retry_keeps_receipt_and_confirmation_is_distinct(self):
        events=[];receipts=[]
        def ack():
            receipts.append('synthetic-signed-receipt')
            if len(receipts)==1:raise TimeoutError('lost ACK reply')
            return dict(acknowledged=True)
        with mock.patch.object(verify.time,'sleep'):
            result,attempts=confirmed_rpc(ack,events.append,'ack',retry=True,index=28,receipt='synthetic-signed-receipt')
        self.assertTrue(result['acknowledged']);self.assertEqual(attempts,2)
        self.assertEqual(receipts,['synthetic-signed-receipt']*2)
        self.assertNotIn('synthetic-signed-receipt',str(events))
        self.assertEqual(events[-1]['phase'],'ack_confirmed')
        self.assertEqual(len({e['receipt_sha256'] for e in events}),1)

    def test_permanent_rejection_is_not_retried(self):
        events=[]
        with mock.patch('benchmark.eventually',side_effect=lambda action,seconds:action()), mock.patch('benchmark.time.sleep') as sleep:
            action=mock.Mock(side_effect=urllib.error.HTTPError('http://127.0.0.1/ack',401,'unauthorized',{},io.BytesIO(b'{}')))
            with self.assertRaisesRegex(RuntimeError,'permanent HTTP rejection') as rejected:
                confirmed_rpc(action,events.append,'ack',retry=True,receipt='synthetic-signed-receipt')
        self.assertEqual(action.call_count,1);sleep.assert_not_called()
        self.assertEqual(rejected.exception.code,401)
        self.assertEqual(events[-1]['phase'],'ack_rejected')

    def test_unconfirmed_ack_is_not_recorded_as_confirmed(self):
        events=[]
        with self.assertRaisesRegex(ValueError,'invalid ACK confirmation'):
            confirmed_rpc(lambda:dict(acknowledged=False),events.append,'ack',receipt='synthetic-receipt')
        self.assertEqual([e['phase'] for e in events],['ack_attempted','ack_uncertain'])

    def test_exhausted_retry_stays_an_uncertain_timeout(self):
        with mock.patch('benchmark.eventually',side_effect=AssertionError('bounded helper expired')):
            with self.assertRaisesRegex(TimeoutError,'outcome remains uncertain'):
                confirmed_rpc(lambda:None,lambda event:None,'publish',retry=True,publisher_id='same-id')

    def test_ordinary_success_uses_counters_without_consuming_event_budget(self):
        events=[];counts=dict(readiness={},measurement={})
        phases=['publish_attempted','publish_confirmed','pull_attempted','pull_confirmed','received','ack_attempted','ack_confirmed']
        with mock.patch.dict(benchmark.WORKLOAD,event_limit=1):
            for phase in phases:record_request_event(events,counts,dict(phase=phase),'measurement',False)
            record_request_event(events,counts,dict(phase='publish_uncertain',http_status=503),'measurement',False)
        self.assertEqual(counts['measurement'],dict.fromkeys(phases+['publish_uncertain'],1))
        self.assertEqual([event['phase'] for event in events],['publish_uncertain'])

    def test_fault_and_readiness_details_preserve_overflow_failure(self):
        for scope,detailed in [('readiness',False),('measurement',True),('measurement',False)]:
            events=[];counts=dict(readiness={},measurement={})
            phase='publish_rejected' if scope=='measurement' and not detailed else 'publish_attempted'
            with self.subTest(scope=scope,detailed=detailed),mock.patch.dict(benchmark.WORKLOAD,event_limit=1):
                record_request_event(events,counts,dict(phase=phase),scope,detailed)
                with self.assertRaisesRegex(RuntimeError,'trace overflow'):
                    record_request_event(events,counts,dict(phase=phase),scope,detailed)
                self.assertEqual(counts[scope][phase],2)
                self.assertEqual(len(events),1)

    def test_readiness_resumes_same_id_and_receipt_after_uncertain_replies(self):
        events=[];peer=mock.Mock();proof=dict(publisher_id='captured-probe')
        peer.publish.side_effect=[TimeoutError('lost publish reply'),dict(sequence=1,duplicate=True)]
        peer.pull.return_value=[dict(data='readiness|captured-probe',sequence=1,receipt='synthetic-secret-receipt')]
        peer.ack.side_effect=[TimeoutError('lost ACK reply'),dict(acknowledged=True)]
        with mock.patch.object(verify.time,'sleep'):
            self.assertIs(readiness_probe(peer,events.append,proof),proof)
        self.assertEqual(peer.publish.call_args_list,[mock.call('orders','readiness|captured-probe','captured-probe')]*2)
        peer.pull.assert_called_once_with('orders','bench')
        self.assertEqual(peer.ack.call_args_list,[mock.call('orders','bench','synthetic-secret-receipt')]*2)
        self.assertEqual((proof['sequence'],proof['received'],proof['acknowledged'],proof['publish_attempts']),(1,1,True,2))
        self.assertEqual([e['phase'] for e in events].count('publish_uncertain'),1)
        self.assertEqual([e['phase'] for e in events].count('ack_uncertain'),1)
        self.assertNotIn('synthetic-secret-receipt',str(events))

    def test_readiness_wrong_payload_is_fatal(self):
        peer=mock.Mock()
        peer.publish.return_value=dict(sequence=1,duplicate=False)
        peer.pull.return_value=[dict(data='wrong',sequence=1,receipt='synthetic-secret-receipt')]
        with self.assertRaisesRegex(RuntimeError,'payload or sequence mismatch'):
            readiness_probe(peer,lambda event:None,dict(publisher_id='probe'))
        peer.pull.assert_called_once();peer.ack.assert_not_called()

    def test_readiness_permanent_rejection_is_not_retried(self):
        peer=mock.Mock();events=[]
        peer.publish.side_effect=urllib.error.HTTPError('http://127.0.0.1/messages',401,'unauthorized',{},io.BytesIO(b'{}'))
        with mock.patch.object(verify.time,'sleep') as sleep:
            with self.assertRaisesRegex(RuntimeError,'permanent HTTP rejection'):
                readiness_probe(peer,events.append,dict(publisher_id='captured-probe'))
        peer.publish.assert_called_once();peer.pull.assert_not_called();sleep.assert_not_called()
        self.assertEqual(events[-1]['phase'],'publish_rejected')

    def test_known_no_stream_error_keeps_safe_code(self):
        events=[]
        action=mock.Mock(side_effect=urllib.error.HTTPError('http://127.0.0.1/messages',503,'unavailable',{},io.BytesIO(b'{"detail":"nats: no response from stream","error":"broker operation unavailable"}\n')))
        with self.assertRaises(urllib.error.HTTPError):confirmed_rpc(action,events.append,'publish')
        self.assertEqual(events[-1]['detail_code'],'no_stream_response')
        self.assertEqual(events[-1]['body_sha256'],'106679462f4b667a6ff79e42ca9e4b7ebbc68ef98517d3cd21a6263e5dccf2d1')

    def test_recovery_excludes_pre_kill_invocations_even_when_retried_after_kill(self):
        def event(phase,index,at):return dict(scope='measurement',phase=phase,index=index,publisher_id=f'bench-{index}',at=at)
        fault=367.069567542
        events=[event('publish_attempted',31,fault-.01),event('ack_confirmed',31,fault+.002688),
                event('publish_attempted',33,367.068916823),event('publish_attempted',33,fault+1),
                event('publish_confirmed',33,fault+2),event('ack_confirmed',33,fault+2.1),
                event('publish_attempted',34,367.069688499),event('publish_attempted',34,fault+1),
                event('publish_confirmed',34,372.990074703),event('ack_confirmed',34,372.991258291)]
        result=post_fault_confirmations(events,fault)
        self.assertEqual(result['indices'],[34])
        self.assertAlmostEqual(result['first_publish_confirmation_ms'],5920.507161)
        self.assertAlmostEqual(result['first_ack_confirmation_ms'],5921.690749)
