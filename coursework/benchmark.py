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

from benchmark_support import Run, environment, load, quantiles, save
from client import Broker
from verify import Cluster, ROOT, eventually, healthy, port, stream_state

WORKLOAD = dict(version=1, seed=421, payload=[64, 1024, 16384], producers=[1, 4, 16], consumers=[1, 4, 16], repeats=5, count=64, rates=[10, 100, 500], sweep_seconds=10, stability_seconds=120, outstanding=64)


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
        result['before'] = stream_state(cluster)
        def consume():
            peer = Broker(client.url, capability)
            gate.wait()
            while not stop.is_set():
                try:
                    messages = peer.pull('orders', 'bench')
                    for message in messages:
                        index = int(message['data'][:10])
                        assert message['data'] == payload(index, cell['payload']), 'payload corruption'
                        ack = peer.ack('orders', 'bench', message['receipt'])
                        assert ack['acknowledged'] is True
                        with condition:
                            delivery = dict(at=time.perf_counter(), sequence=message['sequence'], attempt=message['attempt'], sha256=hashlib.sha256(message['data'].encode()).hexdigest())
                            deliveries.setdefault(index, []).append(delivery)
                            condition.notify_all()
                except Exception as error:
                    with condition:
                        consumer_errors.append(dict(at=time.perf_counter(), error=repr(error)))
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
                ack = client.publish('orders', payload(index, cell['payload']), f'bench-{index}')
            except Exception as error:
                with condition:
                    uncertain.append(dict(index=index, at=time.perf_counter(), error=repr(error), publisher_id=f'bench-{index}'))
                raise
            assert not ack['duplicate'], ack
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
        result['uncertain_publish_attempts'] = uncertain
        result['consumer_errors'] = consumer_errors
        result['duplicates'] = sum(len(v)-1 for v in deliveries.values())
        result['redeliveries'] = sum(d['attempt'] > 1 for v in deliveries.values() for d in v)
        result['after'] = stream_state(cluster)
        result['useful_bytes'] = len(deliveries)*cell['payload']
        result['passed'] = result['load']['finished'] == count and len(deliveries) == count and not consumer_errors
        if fault:
            after = [d['at'] for v in deliveries.values() for d in v if d['at'] >= result['fault_at']]
            result['first_post_fault_ack_ms'] = 1000*(min(after)-result['fault_at']) if after else None
    except Exception as error:
        result['error'] = repr(error)
    finally:
        stop.set(); gate.set()
        for thread in threads:
            thread.join(timeout=7)
        result['consumer_threads_stopped'] = all(not t.is_alive() for t in threads)
        result['cleanup'] = run.cleanup()
        result['passed'] = result['passed'] and result['consumer_threads_stopped'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
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
