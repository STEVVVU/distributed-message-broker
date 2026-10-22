import unittest
from unittest import mock
from pathlib import Path
import tempfile
from types import SimpleNamespace
import verify
import io
import urllib.error
from benchmark import payload, cells, confirmed_rpc
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
