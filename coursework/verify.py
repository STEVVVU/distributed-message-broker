"""Owned-process verification. Evidence survives success, failure and cleanup."""
import argparse
import json
import os
import pathlib
import secrets
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.error

from client import Broker

ROOT = pathlib.Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def eventually(action, seconds=30):
    until = time.monotonic() + seconds
    last = None
    while time.monotonic() < until:
        try:
            return action()
        except (OSError, ValueError, AssertionError) as error:
            last = error
            time.sleep(.1)
    raise AssertionError(f'timed out: {last}')


class Cluster:
    def __init__(self):
        self.path = ROOT / '.local' / 'evidence' / (time.strftime('%Y%m%d-%H%M%S') + '-' + secrets.token_hex(3))
        self.path.mkdir(parents=True)
        self.nodes = [dict(name=f'node{i}', client=port(), route=port(), monitor=port()) for i in range(3)]
        self.processes = []
        self.events = []
        self.lock = threading.Lock()
        self.token = secrets.token_urlsafe(32)
        for node in self.nodes:
            routes = ','.join(f'"nats://route:{self.token}@127.0.0.1:{other["route"]}"' for other in self.nodes if other != node)
            config = f'''server_name: {node['name']}
host: 127.0.0.1
port: {node['client']}
http: 127.0.0.1:{node['monitor']}
authorization {{ token: "{self.token}" }}
jetstream {{ store_dir: "{(self.path / node['name']).as_posix()}"; max_memory_store: 67108864; max_file_store: 134217728 }}
cluster {{ name: coursework; host: 127.0.0.1; port: {node['route']}; authorization {{ user: route; password: "{self.token}" }}; routes: [{routes}] }}
'''
            (self.path / (node['name'] + '.conf')).write_text(config)

    def record(self, action, **values):
        with self.lock:
            self.events.append(dict(action=action, at=time.time(), **values))
            (self.path / 'events.json').write_text(json.dumps(self.events, indent=2))

    def start(self, node):
        log = open(self.path / (node['name'] + '.log'), 'ab')
        process = subprocess.Popen([str(ROOT / '.local/nats-server.exe'), '-c', str(self.path / (node['name'] + '.conf'))], stdout=log, stderr=log, start_new_session=os.name != 'nt')
        log.close()
        node['process'] = process
        self.processes.append(process)
        self.record('start', node=node['name'], pid=process.pid)

    def stop(self, node):
        process = node['process']
        process.kill()
        process.wait(timeout=10)
        self.record('kill', node=node['name'], pid=process.pid)

    def monitor(self, node, endpoint='/healthz?js-enabled-only=true'):
        assert node['process'].poll() is None
        with urllib.request.urlopen(f'http://127.0.0.1:{node["monitor"]}{endpoint}', timeout=2) as response:
            return json.load(response)

    def doctor(self):
        for node in self.nodes:
            assert self.monitor(node)['status'] == 'ok'
        self.record('doctor', nodes=[n['name'] for n in self.nodes])

    def cleanup(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
        self.record('cleanup', all_exited=all(p.poll() is not None for p in self.processes))


def run(baseline=False, inject_failure=False):
    cluster = Cluster()
    result = dict(passed=False, baseline=baseline)
    try:
        for node in cluster.nodes:
            cluster.start(node)
        eventually(cluster.doctor)
        if inject_failure:
            raise AssertionError('deliberate driver failure after launch')
        if not baseline:
            journey(cluster)
        result['passed'] = True
    except BaseException as error:
        result['error'] = repr(error)
        raise
    finally:
        cluster.cleanup()
        (cluster.path / 'result.json').write_text(json.dumps(result, indent=2))
        assert (cluster.path / 'events.json').exists()
        print(cluster.path, json.dumps(result), flush=True)


def stream_state(cluster):
    for node in cluster.nodes:
        if node['process'].poll() is not None:
            continue
        data = cluster.monitor(node, '/jsz?accounts=true&streams=true&consumers=true')
        for account in data.get('account_details', []):
            for stream in account.get('stream_detail', []):
                if stream['name'] == 'B_orders' and stream.get('cluster', {}).get('leader') == node['name']:
                    return stream
    raise AssertionError('stream not visible')


def healthy(cluster):
    state = stream_state(cluster)
    group = state.get('cluster', {})
    replicas = group.get('replicas', [])
    assert group.get('leader') and len(replicas) == 2 and all(r.get('current') and not r.get('offline') for r in replicas), state
    cluster.record('replica_health', state=state)
    return state


def one(client, channel):
    messages = client.pull('orders', channel)
    assert len(messages) == 1, messages
    return messages[0]


def journey(cluster):
    http_port = port()
    capability = secrets.token_urlsafe(32)
    environment = dict(os.environ, BROKER_TOKEN=capability, NATS_TOKEN=cluster.token)
    log = open(cluster.path / 'adapter.log', 'ab')
    process = subprocess.Popen([str(ROOT / '.local/topic-channel.exe'), '-listen', f'127.0.0.1:{http_port}', '-servers', ','.join(f'nats://127.0.0.1:{n["client"]}' for n in cluster.nodes)], env=environment, stdout=log, stderr=log)
    log.close()
    cluster.processes.append(process)
    cluster.record('adapter_start', pid=process.pid, port=http_port)
    client = Broker(f'http://127.0.0.1:{http_port}', capability, cluster.record)
    other = Broker(client.url, capability, cluster.record)
    eventually(lambda: client.topic('orders'))
    client.topic('orders')
    for channel in ['billing', 'shipping']:
        eventually(lambda: client.channel('orders', channel))
        client.channel('orders', channel)
    ledger = []

    def publish(label):
        ack = client.publish('orders', label, label)
        assert not ack['duplicate'], ack
        ledger.append(dict(sequence=ack['sequence'], data=label))
        (cluster.path / 'accepted-ledger.json').write_text(json.dumps(ledger, indent=2))
        return ack

    first = publish('first')
    duplicate = client.publish('orders', 'first', 'first')
    assert duplicate['duplicate'] and duplicate['sequence'] == first['sequence']
    abandoned = subprocess.run([sys.executable, str(ROOT / 'coursework/client.py'), 'pull', client.url, 'orders', 'billing'], env=environment, capture_output=True, text=True, timeout=8, check=True)
    held = json.loads(abandoned.stdout)[0]
    cluster.record('consumer_process_exit_without_ack', exit_code=abandoned.returncode, delivered=held)
    assert held['attempt'] == 1
    def redelivery():
        message = one(other, 'billing')
        assert message['sequence'] == held['sequence'] and message['attempt'] >= 2, message
        return message
    retried = eventually(redelivery, 10)
    other.ack('orders', 'billing', retried['receipt'])
    try:
        client.ack('orders', 'shipping', retried['receipt'])
        raise AssertionError('cross-channel receipt accepted')
    except urllib.error.HTTPError as error:
        assert error.code == 400
        cluster.record('cross_channel_receipt_rejected', status=error.code)
    sent = one(client, 'shipping')
    client.ack('orders', 'shipping', sent['receipt'])
    publish('second')
    publish('third')
    billing = [held['sequence']]
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as workers:
        shared = list(workers.map(lambda consumer: one(consumer, 'billing'), [client, other]))
    assert len({message['sequence'] for message in shared}) == 2
    for message in sorted(shared, key=lambda m: m['sequence']):
        billing.append(message['sequence'])
        client.ack('orders', 'billing', message['receipt'])
    assert billing == [item['sequence'] for item in ledger]
    state = eventually(lambda: healthy(cluster))
    leader = state['cluster']['leader']
    failed = next(n for n in cluster.nodes if n['name'] == leader)
    cluster.stop(failed)

    # Retry the same publisher ID so uncertain acknowledgments cannot create extra records.
    def quorum_publish():
        ack = client.publish('orders', 'after-leader-loss', 'after-leader-loss')
        return ack
    ack = eventually(quorum_publish)
    ledger.append(dict(sequence=ack['sequence'], data='after-leader-loss'))
    (cluster.path / 'accepted-ledger.json').write_text(json.dumps(ledger, indent=2))
    cluster.record('quorum_write_accepted', ack=ack)
    second_failed = next(n for n in cluster.nodes if n != failed)
    cluster.stop(second_failed)
    started = time.monotonic()
    try:
        client.publish('orders', 'uncertain-no-quorum', 'uncertain-no-quorum')
        raise AssertionError('write acknowledged without quorum')
    except urllib.error.HTTPError as error:
        assert error.code == 503
        cluster.record('no_quorum_write_rejected', status=error.code, elapsed=time.monotonic()-started, body=error.read().decode())
    assert time.monotonic()-started < 6
    for node in [failed, second_failed]:
        cluster.start(node)
    eventually(cluster.doctor)
    eventually(lambda: healthy(cluster))
    cluster.record('partial_restart_complete')
    for node in cluster.nodes:
        cluster.stop(node)
    for node in cluster.nodes:
        cluster.start(node)
    eventually(cluster.doctor)
    eventually(lambda: healthy(cluster))
    observed = {}
    for channel in ['billing', 'shipping']:
        messages = []
        empty = 0
        while empty < 2:
            batch = client.pull('orders', channel)
            if not batch:
                empty += 1
                continue
            empty = 0
            message = batch[0]
            messages.append(message)
            client.ack('orders', channel, message['receipt'])
        expected = ledger[3:] if channel == 'billing' else ledger[1:]
        accepted = [dict(sequence=m['sequence'], data=m['data']) for m in messages if m['data'] != 'uncertain-no-quorum']
        assert accepted == expected, (channel, accepted, expected)
        sequences = [m['sequence'] for m in messages]
        assert sequences == sorted(set(sequences)), sequences
        observed[channel] = messages
    cluster.record('durable_oracle_passed', accepted=ledger, observed=observed, uncertain_publish='uncertain-no-quorum may or may not have committed')
    eventually(lambda: healthy(cluster))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', action='store_true')
    parser.add_argument('--inject-failure', action='store_true')
    arguments = parser.parse_args()
    run(arguments.baseline, arguments.inject_failure)
