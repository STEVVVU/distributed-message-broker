"""Real replicated topic/channel load; every payload and ACK is checked."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time
import urllib.request
import urllib.error

from benchmark_support import Run, environment, load, quantiles, save
from client import Broker
from verify import Cluster, ROOT, eventually, healthy, port, stream_state

WORKLOAD = dict(version=3, seed=421, payload=[64, 1024, 16384], producers=[1, 4, 16], consumers=[1, 4, 16], repeats=5, count=64, rates=[10, 100, 500], sweep_seconds=10, stability_seconds=120, outstanding=64, fault_retry_seconds=30, event_limit=131072, readiness_probe_messages=1)


class HTTPRejection(RuntimeError):
    def __init__(self,status):
        super().__init__('permanent HTTP rejection; inspect safe request events')
        self.code=status


def safe_error(error):
    """Keep diagnostic classes, never capability/receipt strings or arbitrary server text."""
    detail=dict(error_type=type(error).__name__,http_status=getattr(error,'code',None))
    if isinstance(error,urllib.error.HTTPError):
        try:
            body=error.read(4096)
            detail['body_sha256']=hashlib.sha256(body).hexdigest()
            text=json.loads(body).get('detail','')
            detail['detail_code']={'nats: timeout':'nats_timeout','nats: no responders available for request':'no_responders','nats: no response from stream':'no_stream_response','nats: disconnected':'disconnected'}.get(text,'other')
        except (OSError,ValueError,AttributeError):
            detail['detail_code']='unparsed'
        finally:error.close()
    return detail


def confirmed_rpc(action,record,phase,*,retry=False,index=None,publisher_id=None,receipt=None):
    """Retry the captured immutable request only; reuse verifier's existing 30-second bound."""
    attempts=0
    identity=dict(index=index,publisher_id=publisher_id)
    if receipt is not None:identity['receipt_sha256']=hashlib.sha256(receipt.encode()).hexdigest()
    def attempt():
        nonlocal attempts
        attempts+=1
        record(dict(phase=phase+'_attempted',at=time.perf_counter(),attempt=attempts,**identity))
        try:
            result=action()
            if phase=='ack' and (not isinstance(result,dict) or result.get('acknowledged') is not True):
                raise ValueError('invalid ACK confirmation')
            if phase=='publish' and (not isinstance(result,dict) or type(result.get('sequence')) is not int or result['sequence']<1 or type(result.get('duplicate')) is not bool):
                raise ValueError('invalid publish acknowledgement')
        except (OSError,ValueError) as error:
            detail=safe_error(error)
            status=detail['http_status']
            permanent=status is not None and 400<=status<500 and status!=429
            record(dict(phase=phase+('_rejected' if permanent else '_uncertain'),at=time.perf_counter(),attempt=attempts,**identity,**detail))
            if permanent:raise HTTPRejection(status) from error
            raise
        record(dict(phase=phase+'_confirmed',at=time.perf_counter(),attempt=attempts,**identity))
        return result
    try:result=eventually(attempt,seconds=WORKLOAD['fault_retry_seconds']) if retry else attempt()
    except AssertionError as error:raise TimeoutError('same-request retry deadline expired; outcome remains uncertain') from error
    return result,attempts


def readiness_probe(client,record,proof):
    """Resume one captured publish/pull/ACK journey within the existing readiness bound."""
    data='readiness|'+proof['publisher_id']
    proof.update(payload_sha256=hashlib.sha256(data.encode()).hexdigest(),publish_attempts=0,received=0,acknowledged=False)
    held={}
    def attempt():
        if 'sequence' not in proof:
            proof['publish_attempts']+=1
            ack,_=confirmed_rpc(lambda:client.publish('orders',data,proof['publisher_id']),record,'publish',publisher_id=proof['publisher_id'])
            if ack['duplicate'] and proof['publish_attempts']==1:raise RuntimeError('unexpected readiness publisher ID collision')
            proof['sequence']=ack['sequence']
        if 'message' not in held:
            messages,_=confirmed_rpc(lambda:client.pull('orders','bench'),record,'pull')
            if not messages:raise ValueError('readiness message not yet available')
            if len(messages)!=1 or messages[0]['data']!=data or messages[0]['sequence']!=proof['sequence']:
                raise RuntimeError('readiness payload or sequence mismatch')
            held['message']=messages[0]
            proof['received']=1
            record(dict(phase='received',at=time.perf_counter(),sequence=proof['sequence'],sha256=proof['payload_sha256']))
        receipt=held['message']['receipt']
        confirmed_rpc(lambda:client.ack('orders','bench',receipt),record,'ack',receipt=receipt)
        proof['acknowledged']=True
        return proof
    return eventually(attempt,seconds=30)


def record_request_event(events,phase_counts,event,scope,detailed):
    counts=phase_counts[scope];phase=event['phase']
    counts[phase]=counts.get(phase,0)+1
    if not (scope=='readiness' or detailed or phase.endswith(('_uncertain','_rejected'))):return
    if len(events)>=WORKLOAD['event_limit']:raise RuntimeError('bounded request event trace overflow')
    events.append(dict(event,scope=scope))


def post_fault_confirmations(events,fault_at):
    first={}
    identities={}
    for event in events:
        if event.get('scope')=='measurement' and event['phase']=='publish_attempted':
            key=(event['index'],event['publisher_id'])
            first[key]=min(first.get(key,event['at']),event['at'])
            identities.setdefault(event['index'],set()).add(event['publisher_id'])
    assert all(len(values)==1 for values in identities.values()),'publisher identity changed for one index'
    eligible={key for key,at in first.items() if at>=fault_at}
    indices={key[0] for key in eligible}
    published=[event['at'] for event in events if event.get('scope')=='measurement' and event['phase']=='publish_confirmed' and (event['index'],event['publisher_id']) in eligible]
    acked=[event['at'] for event in events if event.get('scope')=='measurement' and event['phase']=='ack_confirmed' and event['index'] in indices]
    return dict(scope='First publish invocation at or after leader kill; repeated attempts retain the same index/publisher identity. ACK index follows exact verified payload receipt.',indices=sorted(indices),first_publish_confirmation_ms=1000*(min(published)-fault_at) if published else None,first_ack_confirmation_ms=1000*(min(acked)-fault_at) if acked else None)


def payload(index, size):
    prefix = f'{index:010d}|'
    return prefix + ('abcdefghijklmnopqrstuvwxyz' * ((size+25)//26))[:size-len(prefix)]


def cells():
    base = dict(payload=1024, producers=4, consumers=4)
    result = []
    for axis in ['payload', 'producers', 'consumers']:
        for value in WORKLOAD[axis]:
            cell = dict(base, **{axis:value})
            if cell not in result:
                result.append(cell)
    result.insert(0, dict(payload=1024, producers=1, consumers=1))
    return result


def trial(cell, count, rate=None, fault=False, backlog=False, profile=False):
    cluster = Cluster()
    run = Run(cluster.path / 'benchmark')
    result = dict(passed=False, cell=cell, rate=rate, fault=fault, backlog=backlog, evidence=str(run.path))
    stop, gate = threading.Event(), threading.Event()
    if not backlog:
        gate.set()
    condition = threading.Condition()
    deliveries, consumer_errors, threads, profiles = {}, [], [], []
    accepted, uncertain = {}, []
    events=[]
    phase_counts=dict(readiness={},measurement={})
    warming=True
    def record(event):
        with condition:
            scope='readiness' if warming else 'measurement'
            try:record_request_event(events,phase_counts,event,scope,fault)
            except RuntimeError:
                stop.set()
                raise
    try:
        for node in cluster.nodes:
            if profile:
                node['profile'] = port()
                config_path = cluster.path / (node['name']+'.conf')
                with config_path.open('a') as config_file:
                    config_file.write(f'\nprof_port: {node["profile"]}\n')
            cluster.start(node)
            run.processes.append((node['name'], node['process']))
        eventually(cluster.doctor)
        capability, http = secrets.token_urlsafe(32), port()
        adapter = run.launch('adapter', [str(ROOT / '.local/topic-channel.exe'), '-listen', f'127.0.0.1:{http}', '-servers', ','.join(f'nats://127.0.0.1:{n["client"]}' for n in cluster.nodes)], dict(os.environ, BROKER_TOKEN=capability, NATS_TOKEN=cluster.token))
        client = Broker(f'http://127.0.0.1:{http}', capability)
        eventually(lambda: client.topic('orders'))
        eventually(lambda: client.channel('orders', 'bench'))
        eventually(lambda: healthy(cluster))
        assert all(p.poll() is None for _, p in run.processes)
        result['readiness']=dict(publisher_id='readiness-'+secrets.token_hex(16),scope='untimed; included in native stream/consumer counters, excluded from measured offers and latency')
        readiness_probe(client,record,result['readiness'])
        warming=False
        result['before'] = stream_state(cluster)
        def consume():
            peer = Broker(client.url, capability)
            gate.wait()
            while not stop.is_set():
                try:
                    messages,_ = confirmed_rpc(lambda:peer.pull('orders','bench'),record,'pull')
                    for message in messages:
                        index = int(message['data'][:10])
                        assert message['data'] == payload(index, cell['payload']), 'payload corruption'
                        record(dict(phase='received',index=index,at=time.perf_counter(),sequence=message['sequence'],delivery_attempt=message['attempt'],sha256=hashlib.sha256(message['data'].encode()).hexdigest()))
                        ack,_ = confirmed_rpc(lambda:peer.ack('orders','bench',message['receipt']),record,'ack',retry=fault,index=index,receipt=message['receipt'])
                        assert ack['acknowledged'] is True
                        with condition:
                            delivery = dict(at=time.perf_counter(), sequence=message['sequence'], attempt=message['attempt'], sha256=hashlib.sha256(message['data'].encode()).hexdigest())
                            deliveries.setdefault(index, []).append(delivery)
                            condition.notify_all()
                except Exception as error:
                    with condition:
                        consumer_errors.append(dict(at=time.perf_counter(), **safe_error(error)))
                    if not fault:
                        stop.set()
        for _ in range(cell['consumers']):
            thread = threading.Thread(target=consume, daemon=True)
            thread.start(); threads.append(thread)
        def publish(index):
            if fault and index == count//2:
                state = stream_state(cluster)
                leader = state['cluster']['leader']
                result['fault_at'] = time.perf_counter()
                cluster.stop(next(n for n in cluster.nodes if n['name'] == leader))
                result['killed_leader'] = leader
            try:
                ack,attempts = confirmed_rpc(lambda:client.publish('orders',payload(index,cell['payload']),f'bench-{index}'),record,'publish',retry=fault,index=index,publisher_id=f'bench-{index}')
            except Exception as error:
                if not isinstance(error,HTTPRejection):
                    with condition:
                        uncertain.append(dict(index=index, at=time.perf_counter(), error=repr(error), publisher_id=f'bench-{index}'))
                raise
            assert not ack['duplicate'] or attempts>1, ack
            proof = dict(sequence=ack['sequence'], publish_ack_at=time.perf_counter())
            with condition:
                accepted[index] = proof
            if not backlog:
                deadline = time.perf_counter()+30
                with condition:
                    while index not in deliveries:
                        remaining = deadline-time.perf_counter()
                        if remaining <= 0:
                            raise TimeoutError('accepted publish not delivered/ACKed by deadline')
                        condition.wait(min(.1, remaining))
                    assert deliveries[index][0]['sequence'] == ack['sequence']
                    proof['delivery'] = deliveries[index][0]
            return proof
        if profile:
            def capture(node):
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{node["profile"]}/debug/pprof/profile?seconds=5',timeout=10) as response:
                        data=response.read()
                    assert data, 'empty CPU profile'
                    (run.path/(node['name']+'.pprof')).write_bytes(data)
                except Exception as error:
                    profiles.append(dict(node=node['name'],error=repr(error)))
            for node in cluster.nodes:
                thread=threading.Thread(target=capture,args=(node,));thread.start();profiles.append(thread)
            result['profile_scope'] = 'Native five-second NATS CPU pprof on loopback-only profiler ports plus generator cProfile. Adapter CPU hot-path remains unprofiled; capture it before adapter-specific optimization.'
        result['load'] = load(publish, count, cell['producers'] if not rate else WORKLOAD['outstanding'], rate)
        for thread in [value for value in profiles if isinstance(value,threading.Thread)]:
            thread.join(timeout=11)
        if profile:
            result['profile_errors']=[value for value in profiles if not isinstance(value,threading.Thread)]
            assert not result['profile_errors'], result['profile_errors']
        if backlog:
            result['backlog_before_drain'] = stream_state(cluster)
            gate.set()
            deadline = time.perf_counter()+60
            accepted_indices = {s['index'] for s in result['load']['samples'] if s['outcome'] == 'finished'}
            with condition:
                while not accepted_indices.issubset(deliveries):
                    remaining = deadline-time.perf_counter()
                    if remaining <= 0:
                        raise TimeoutError('backlog did not drain')
                    condition.wait(min(.1, remaining))
        for sample in result['load']['samples']:
            if sample['index'] in deliveries:
                sample['delivery_ack_latency_ms'] = 1000*(deliveries[sample['index']][0]['at']-sample['scheduled'])
        result['delivery_ack_latency_ms'] = quantiles([s['delivery_ack_latency_ms'] for s in result['load']['samples'] if 'delivery_ack_latency_ms' in s])
        result['deliveries'] = deliveries
        result['accepted_publish_ledger'] = accepted
        result['uncertain_publish_attempts'] = [event for event in events if event['scope']=='measurement' and event['phase']=='publish_uncertain']
        result['unresolved_publish_attempts'] = uncertain
        result['consumer_errors'] = consumer_errors
        result['duplicates'] = sum(len(v)-1 for v in deliveries.values())
        result['redeliveries'] = sum(d['attempt'] > 1 for v in deliveries.values() for d in v)
        result['after'] = stream_state(cluster)
        result['useful_bytes'] = len(deliveries)*cell['payload']
        result['passed'] = result['load']['finished'] == count and len(deliveries) == count and not consumer_errors
        if fault:
            after = [d['at'] for v in deliveries.values() for d in v if d['at'] >= result['fault_at']]
            result['first_post_fault_ack_ms'] = 1000*(min(after)-result['fault_at']) if after else None
            result['first_post_fault_ack_scope']='Any ACK after the kill, including publishes already in flight before the kill; does not measure newly invoked work recovery.'
            result['newly_invoked_post_fault']=post_fault_confirmations(events,result['fault_at'])
    except Exception as error:
        result['error'] = repr(error)
    finally:
        stop.set(); gate.set()
        for thread in threads:
            thread.join(timeout=37 if fault else 7)
        result['consumer_threads_stopped'] = all(not t.is_alive() for t in threads)
        result['cleanup'] = run.cleanup()
        save(run.path/'request-events.json',events)
        result['request_phase_counts']=phase_counts
        result['request_event_scope']='Detailed readiness and fault events; ordinary measurement retains phase counts and bounded uncertain/rejected events.'
        result['request_event_count']=len(events)
        result['request_event_overflow']=len(events)>=WORKLOAD['event_limit']
        result['passed'] = result['passed'] and not result['request_event_overflow'] and result['consumer_threads_stopped'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
        save(run.path / 'result.json', result)
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('mode', choices=['smoke','benchmark','profile'], nargs='?', default='smoke')
    args=parser.parse_args()
    assert args.mode == 'smoke' or sys.platform == 'linux', 'isolated Linux runner required'
    directory=ROOT/'.local'/('benchmark-'+args.mode+'-'+str(time.time_ns())); directory.mkdir()
    summary=dict(passed=False, environment=environment(ROOT), workload=WORKLOAD, mode=args.mode, trials=[])
    def run(cell, count=WORKLOAD['count'], rate=None, **flags):
        value=trial(cell,count,rate,profile=args.mode=='profile',**flags)
        summary['trials'].append(value); save(directory/'summary.json',summary)
        print(json.dumps(dict(passed=value['passed'],evidence=value['evidence'])),flush=True)
        if value.get('error') or not value['cleanup']['all_exited']:
            raise RuntimeError(value.get('error','cleanup failure'))
        return value
    try:
        if args.mode=='smoke':
            run(dict(payload=64,producers=1,consumers=1),2)
            run(dict(payload=64,producers=1,consumers=1),2,backlog=True)
        elif args.mode=='profile':
            import cProfile
            profiler=cProfile.Profile(); profiler.enable()
            run(dict(payload=1024,producers=4,consumers=4))
            profiler.disable(); profiler.dump_stats(str(directory/'generator.pstats'))
        else:
            matrix=cells()
            for repeat in range(WORKLOAD['repeats']):
                for cell in matrix if repeat%2==0 else reversed(matrix):
                    run(dict(cell,repeat=repeat))
            base=dict(payload=1024,producers=4,consumers=4)
            sweeps=[run(base,int(rate*WORKLOAD['sweep_seconds']),rate) for rate in WORKLOAD['rates']]
            stable=[v for v in sweeps if v['passed'] and v['load']['dispatch_lag_ms']['p99']<100 and v['delivery_ack_latency_ms']['p99']<5000]
            if stable:
                rate=stable[-1]['rate']; run(base,int(rate*WORKLOAD['stability_seconds']),rate)
            else:
                summary['stability_blocked']='No zero-loss rate with dispatch-p99<100ms and delivery+ACK-p99<5s; expand lower rates.'
            run(base,64,backlog=True)
            run(base,64,fault=True)
        summary['passed']=all(v['passed'] for v in summary['trials']) and not summary.get('stability_blocked')
    except Exception as error:
        summary['error']=repr(error)
    finally:
        save(directory/'summary.json',summary)
    print(directory/'summary.json')
    return 0 if summary['passed'] else 1


if __name__=='__main__':
    raise SystemExit(main())
