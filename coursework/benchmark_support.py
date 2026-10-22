"""Bounded benchmark evidence for real product drivers; no simulated work."""
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import threading
import time


def quantiles(values):
    import math
    if not values:
        return None
    data = sorted(values)
    return dict(count=len(data), min=data[0], p50=data[math.ceil(.5*len(data))-1], p95=data[math.ceil(.95*len(data))-1], p99=data[math.ceil(.99*len(data))-1], max=data[-1])


def save(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def environment(root):
    return dict(source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip(), dirty=bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=root, text=True).strip()), python=platform.python_version(), platform=platform.platform(), cores=os.cpu_count(), clock_ticks=os.sysconf('SC_CLK_TCK') if os.name != 'nt' else None)


class Run:
    def __init__(self, path):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=False)
        assert shutil.disk_usage(self.path).free >= 10 * 1024**3, '10GiB free disk required'
        self.processes, self.logs, self.resources = [], [], []
        self.done = threading.Event()
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()

    def launch(self, name, command, env=None):
        log = (self.path / (name + '.log')).open('ab')
        self.logs.append(log)
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=os.name != 'nt', creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
        self.processes.append((name, process))
        return process

    def stop(self, process):
        if process.poll() is not None:
            return
        if os.name == 'nt':
            subprocess.run(['taskkill', '/PID', str(process.pid), '/T', '/F'], check=True, capture_output=True)
        else:
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            if os.name == 'nt':
                raise
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)

    @staticmethod
    def proc(pid):
        if os.name == 'nt':
            return None
        try:
            stat = Path(f'/proc/{pid}/stat').read_text().rsplit(') ', 1)[1].split()
            status = Path(f'/proc/{pid}/status').read_text().splitlines()
            rss = next((int(line.split()[1])*1024 for line in status if line.startswith('VmRSS:')), 0)
            return dict(pid=pid, cpu_ticks=int(stat[11])+int(stat[12]), rss_bytes=rss)
        except (FileNotFoundError, ProcessLookupError):
            return None

    def monitor(self):
        while not self.done.is_set():
            self.resources.append(dict(at=time.perf_counter(), generator=self.proc(os.getpid()), processes=[dict(name=name, process=self.proc(p.pid)) for name, p in list(self.processes)]))
            self.done.wait(.1)

    def cleanup(self):
        errors = []
        for name, process in reversed(self.processes):
            try:
                self.stop(process)
            except Exception as error:
                errors.append(dict(name=name, error=repr(error)))
        self.done.set()
        self.thread.join(timeout=2)
        for log in self.logs:
            log.close()
        save(self.path / 'resources.json', self.resources)
        return dict(all_exited=all(p.poll() is not None for _, p in self.processes), sampler_stopped=not self.thread.is_alive(), errors=errors)


def load(action, count, concurrency, rate=None):
    """Start latency at intended arrival; record every offer, including generator declines."""
    start = time.perf_counter()
    samples, pending = [], set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        for index in range(count):
            scheduled = start + index/rate if rate else time.perf_counter()
            if rate:
                time.sleep(max(0, scheduled-time.perf_counter()))
            pending = {future for future in pending if not future.done()}
            sample = dict(index=index, scheduled=scheduled)
            samples.append(sample)
            if rate and len(pending) >= concurrency:
                sample['outcome'] = 'generator_declined'
                continue
            def one(item=sample):
                item['started'] = time.perf_counter()
                try:
                    item['proof'] = action(item['index'])
                    item['outcome'] = 'finished'
                except TimeoutError as error:
                    item.update(outcome='timed_out', error=repr(error))
                except Exception as error:
                    code = getattr(error, 'code', None)
                    item.update(outcome='rejected' if isinstance(code, int) and 400 <= code < 500 else 'failed', error=repr(error))
                item['finished'] = time.perf_counter()
                item['latency_ms'] = 1000*(item['finished']-item['scheduled'])
                item['dispatch_lag_ms'] = 1000*(item['started']-item['scheduled'])
            pending.add(pool.submit(one))
    end = time.perf_counter()
    outcomes = {name: sum(s['outcome'] == name for s in samples) for name in ['finished', 'failed', 'rejected', 'timed_out', 'generator_declined']}
    assert sum(outcomes.values()) == count
    return dict(offered=count, **outcomes, samples=samples, start=start, end=end, elapsed_seconds=end-start, throughput=outcomes['finished']/(end-start), latency_ms=quantiles([s['latency_ms'] for s in samples if s['outcome'] == 'finished']), dispatch_lag_ms=quantiles([s['dispatch_lag_ms'] for s in samples if 'started' in s]))

