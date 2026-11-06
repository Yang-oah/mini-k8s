#!/usr/bin/env python3
"""Measure a bounded zero-replica RS API baseline using a private API and etcd.

This is a partial capacity experiment, not a production limit or R1 verdict.
It never contacts the live cluster and does not measure worker convergence.
"""
import concurrent.futures
import hashlib
import json
import math
import os
import platform
import secrets
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from pathlib import Path

EXPECTED = os.environ.get('R1_EXPECTED_SHA', '4d6bcb2b61e61797a9329a67a389df46176922debce847c02ca7a7e60a222f5a')
SOURCE = Path(os.environ.get('R1_SOURCE_DIR', '/home/lihaoyang/mini-k8s-g4/evidence/r1-integration-v9-lTK16nB5'))
os.umask(0o077)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def sha_source():
    files = sorted([p for d in ('cmd', 'internal') for p in (SOURCE / d).rglob('*') if p.is_file()],
                   key=lambda p: p.relative_to(SOURCE).as_posix().encode())
    files.extend(SOURCE / n for n in ('go.mod', 'go.sum'))
    lines = b''.join((hashlib.sha256(p.read_bytes()).hexdigest() + '  ' + p.relative_to(SOURCE).as_posix() + '\n').encode() for p in files)
    return hashlib.sha256(lines).hexdigest()


def percentile(values, fraction):
    values = sorted(values)
    return round(values[math.ceil(len(values) * fraction) - 1], 3) if values else None


def process_sample(pid):
    try:
        status = Path(f'/proc/{pid}/status').read_text()
        stat = Path(f'/proc/{pid}/stat').read_text().split()
        return {'at': time.time(), 'pid': pid, 'cpu_ticks': int(stat[13]) + int(stat[14]),
                'status': {line.split(':', 1)[0]: line.split(':', 1)[1].strip()
                           for line in status.splitlines() if line.startswith(('VmRSS:', 'VmSize:', 'Threads:'))}}
    except (OSError, ValueError, IndexError):
        return {'at': time.time(), 'pid': pid, 'error': 'process unavailable'}


def resource_constraints():
    """Record each readable cgroup-v2 ancestor: a parent may impose the limit."""
    result = {'proc_self_cgroup': None, 'cgroup_v2_ancestors': [],
              'note': 'Only readable cgroup v2 limits are included; missing fields are not unlimited.'}
    try:
        result['proc_self_cgroup'] = Path('/proc/self/cgroup').read_text().strip()
        line = next((line for line in result['proc_self_cgroup'].splitlines() if line.startswith('0::')), None)
        if not line:
            return result
        root = Path('/sys/fs/cgroup').resolve()
        current = (root / line[3:].lstrip('/')).resolve()
        if not current.is_relative_to(root):
            result['error'] = 'cgroup path is outside the expected mount'
            return result
        while True:
            fields = {}
            for name in ('cpu.max', 'memory.max', 'cpuset.cpus.effective'):
                try:
                    fields[name] = (current / name).read_text().strip()
                except OSError:
                    fields[name] = None
            result['cgroup_v2_ancestors'].append({'path': str(current), 'limits': fields})
            if current == root:
                break
            current = current.parent
    except OSError as error:
        result['error'] = str(error)
    return result


def disk_sample(path):
    """Measure isolated etcd files, including WAL; filesystem free space is contextual."""
    total = 0
    files = 0
    errors = []
    def onerror(error):
        errors.append(str(error))
    for directory, _, names in os.walk(path, onerror=onerror):
        for name in names:
            try:
                total += (Path(directory) / name).stat().st_size
                files += 1
            except OSError as error:
                errors.append(str(error))
    try:
        stats = os.statvfs(path)
        available = stats.f_bavail * stats.f_frsize
    except OSError as error:
        available = None
        errors.append(str(error))
    return {'at': time.time(), 'etcd_data_logical_bytes': total, 'files': files,
            'filesystem_available_bytes': available, 'errors': errors}


def metric_delta(before, after):
    """Keep labelled series intact; gauges are snapshots, not request totals."""
    delta = {}
    for key in before.keys() & after.keys():
        try:
            first, last = float(before[key]), float(after[key])
            if math.isfinite(first) and math.isfinite(last):
                delta[key] = last - first
        except ValueError:
            pass
    return delta


def run():
    for tool in ('go', 'etcd', 'openssl'):
        if shutil.which(tool) is None:
            raise RuntimeError('missing tool: ' + tool)
    if sha_source() != EXPECTED:
        raise RuntimeError('source checksum mismatch')
    out = Path(tempfile.mkdtemp(prefix='r1-capacity-current-', dir=SOURCE))
    print('Evidence:', out, flush=True)
    private = out / 'private'
    private.mkdir(mode=0o700)
    token = secrets.token_hex(32)
    (private / 'auth.json').write_text(json.dumps({'admin': token, 'scheduler': secrets.token_hex(32)}))
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                    '-keyout', str(private / 'key.pem'), '-out', str(private / 'cert.pem'),
                    '-subj', '/CN=127.0.0.1', '-addext', 'subjectAltName=IP:127.0.0.1'],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    with (out / 'build.log').open('w') as log:
        subprocess.run(['go', 'build', '-o', str(out / 'mini-api'), './cmd/mini-api'], cwd=SOURCE,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    etcd_port, peer_port, api_port = [free_port() for _ in range(3)]
    if len({etcd_port, peer_port, api_port}) != 3:
        raise RuntimeError('port allocation collision; retry in a new directory')
    endpoint = f'http://127.0.0.1:{etcd_port}'
    server = f'https://127.0.0.1:{api_port}'
    procs, handles = [], []
    ctx = ssl.create_default_context(cafile=str(private / 'cert.pem'))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=ctx))

    def start(name, argv):
        handle = (out / (name + '.log')).open('w')
        handles.append(handle)
        proc = subprocess.Popen(argv, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
        procs.append(proc)
        return proc

    def request(method, path, body=None):
        before = time.monotonic()
        req = urllib.request.Request(server + '/api/mini/v1/' + path,
            data=None if body is None else json.dumps(body).encode(),
            headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method=method)
        result = {'at': time.time(), 'method': method, 'path': path.split('?', 1)[0]}
        if body is not None:
            result['name'] = body.get('metadata', {}).get('name')
        try:
            with opener.open(req, timeout=8) as res:
                result['status'] = res.status
                result['body'] = json.loads(res.read())
        except urllib.error.HTTPError as exc:
            result['status'] = exc.code
            result['error_body'] = exc.read(4096).decode('utf-8', 'replace')
        except (OSError, ValueError, urllib.error.URLError) as exc:
            result['error'] = type(exc).__name__
        result['ms'] = round((time.monotonic() - before) * 1000, 3)
        return result

    def fixture(name):
        labels = {'test-run': name}
        spec = {'image': 'test:v1', 'restartPolicy': 'Always', 'resources': {
            'requests': {'cpuMilli': 100, 'memoryBytes': 64}, 'limits': {'cpuMilli': 100, 'memoryBytes': 64}}}
        return {'apiVersion': 'mini.k8s/v1', 'kind': 'ReplicaSet', 'metadata': {'name': name},
                'spec': {'replicas': 0, 'selector': labels, 'template': {'labels': labels, 'spec': spec}}}

    def metrics(label):
        # Keep all raw series so absent families or histogram buckets are auditable.
        names = ('etcd_server_proposals_committed_total', 'etcd_debugging_mvcc_db_total_size_in_bytes',
                 'etcd_mvcc_db_total_size_in_bytes', 'process_resident_memory_bytes', 'process_cpu_seconds_total',
                 'etcd_debugging_mvcc_range_total', 'etcd_debugging_mvcc_put_total', 'etcd_debugging_mvcc_txn_total',
                 'etcd_debugging_mvcc_watcher_total', 'etcd_mvcc_watcher_total',
                 'etcd_debugging_mvcc_watch_stream_total', 'etcd_mvcc_watch_stream_total',
                 'grpc_server_handling_seconds', 'grpc_server_started_total', 'grpc_server_handled_total',
                 'etcd_disk_backend_commit_duration_seconds', 'etcd_disk_wal_fsync_duration_seconds')
        with urllib.request.urlopen(endpoint + '/metrics', timeout=3) as response:
            raw = response.read().decode()
        raw_path = 'metrics-' + label + '.prom'
        (out / raw_path).write_text(raw, encoding='utf-8')
        values = {}
        for line in raw.splitlines():
            parts = line.split()
            if len(parts) < 2 or line.startswith('#'):
                continue
            name = parts[0].split('{', 1)[0]
            if any(name == item or name.startswith(item + '_') for item in names):
                values[parts[0]] = parts[1]
        present = {name for name in names if any(key.split('{', 1)[0] == name or
                   key.split('{', 1)[0].startswith(name + '_') for key in values)}
        return {'at': time.time(), 'raw_file': raw_path, 'values': values,
                'unavailable_families': [name for name in names if name not in present]}

    def summarise(rows, begin, finish):
        values = [r['ms'] for r in rows if r.get('status') in (200, 201)]
        return {'requests': len(rows), 'statuses': dict(Counter(str(r.get('status', r.get('error', 'unknown'))) for r in rows)),
                'elapsed_seconds': round(finish - begin, 3), 'requests_per_second': round(len(rows) / max(finish - begin, .001), 3),
                'success_latency_ms': {f'p{int(f * 100)}': percentile(values, f) for f in (.5, .95, .99)}}

    try:
        etcd = start('etcd', ['etcd', '--name', 'r1-capacity', '--data-dir', str(out / 'etcd-data'),
            '--listen-client-urls', endpoint, '--advertise-client-urls', endpoint,
            '--listen-peer-urls', f'http://127.0.0.1:{peer_port}', '--initial-advertise-peer-urls', f'http://127.0.0.1:{peer_port}',
            '--initial-cluster', f'r1-capacity=http://127.0.0.1:{peer_port}'])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if etcd.poll() is not None:
                raise RuntimeError('isolated etcd exited')
            try:
                if json.load(urllib.request.urlopen(endpoint + '/health', timeout=1)).get('health') in ('true', True):
                    break
            except (OSError, ValueError, urllib.error.URLError):
                time.sleep(.2)
        else:
            raise RuntimeError('isolated etcd health timed out')
        api = start('api', [str(out / 'mini-api'), '-listen', f'127.0.0.1:{api_port}', '-etcd', endpoint,
            '-tls-cert', str(private / 'cert.pem'), '-tls-key', str(private / 'key.pem'), '-auth-file', str(private / 'auth.json')])
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if api.poll() is not None:
                raise RuntimeError('isolated API exited')
            if request('GET', 'replicasets').get('status') == 200:
                break
            time.sleep(.2)
        else:
            raise RuntimeError('isolated API readiness timed out')
        metadata = {'source_sha256': EXPECTED, 'api_binary_sha256': hashlib.sha256((out / 'mini-api').read_bytes()).hexdigest(),
            'go': subprocess.check_output(['go', 'version'], text=True).strip(),
            'etcd': subprocess.check_output(['etcd', '--version'], text=True).splitlines()[0],
            'platform': platform.platform(), 'cpu_count': os.cpu_count(),
            'meminfo': Path('/proc/meminfo').read_text().splitlines()[:3],
            'resource_constraints': resource_constraints(),
            'measurement_method': {'warmup': 'one API readiness GET; no additional warmup',
                'sampling': 'one snapshot before writes and one after reads per workload',
                'duration': 'per-stage elapsed_seconds; no fixed-duration soak test',
                'metrics': 'raw etcd Prometheus snapshots plus labelled differences; no watch consumers created'},
            'limitations': ['isolated single-host API and etcd', 'zero replica ReplicaSets only',
                            'no Controller, scheduler, Worker or Pod convergence',
                            'etcd read/write/transaction latency histogram families may not be exposed by etcd',
                            'no watch load; watcher gauges are point-in-time only', 'not an SLA or R1 PASS']}
        (out / 'metadata.json').write_text(json.dumps(metadata, indent=2))
        for size in (20, 40):
            for clients in (1, 8, 32):
                print(f'objects={size} clients={clients}', flush=True)
                prefix = f'r1c-{size}-{clients}-{secrets.token_hex(3)}'
                label = f'{size}-{clients}'
                before = {'api': process_sample(api.pid), 'etcd': process_sample(etcd.pid),
                          'metrics': metrics(label + '-before'), 'disk': disk_sample(out / 'etcd-data')}
                start_time = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
                    writes = list(pool.map(lambda i: request('POST', 'replicasets', fixture(f'{prefix}-{i:03d}')), range(size)))
                write_end = time.monotonic()
                expected_names = {f'{prefix}-{i:03d}' for i in range(size)}
                if any(row.get('status') not in (201, 409) or (row.get('status') == 201 and not row.get('body', {}).get('metadata', {}).get('uid')) for row in writes):
                    (out / f'failure-{size}-{clients}.json').write_text(json.dumps(writes, indent=2))
                    raise RuntimeError('write failure; preserving isolated data')
                # Preserve initial conflicts; serial retries only prepare the exact read fixture.
                retries = []
                for row in writes:
                    if row.get('status') != 409:
                        continue
                    retry = request('POST', 'replicasets', fixture(row['name']))
                    retries.append(retry)
                    if retry.get('status') != 201 or not retry.get('body', {}).get('metadata', {}).get('uid'):
                        (out / f'failure-{size}-{clients}.json').write_text(json.dumps({'writes': writes, 'retries': retries}, indent=2))
                        raise RuntimeError('fixture retry failed; preserving isolated data')
                check = request('GET', 'replicasets')
                observed = {x['metadata']['name'] for x in check.get('body', {}).get('items', [])}
                if check.get('status') != 200 or observed != expected_names:
                    raise RuntimeError('object count differs from fixture')
                read_start = time.monotonic()
                with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
                    reads = list(pool.map(lambda _: request('GET', 'replicasets'), range(clients * 20)))
                read_end = time.monotonic()
                after = {'api': process_sample(api.pid), 'etcd': process_sample(etcd.pid),
                         'metrics': metrics(label + '-after'), 'disk': disk_sample(out / 'etcd-data')}
                for row in reads:
                    row['item_count'] = len(row.get('body', {}).get('items', []))
                    row.pop('body', None)
                identities = [(row['body']['metadata']['name'], row['body']['metadata']['uid']) for row in writes + retries if row.get('status') == 201]
                for row in writes:
                    row.pop('body', None)
                for row in retries:
                    row.pop('body', None)
                result = {'object_count': size, 'concurrency': clients, 'writes': summarise(writes, start_time, write_end),
                          'reads': summarise(reads, read_start, read_end), 'before': before, 'after': after,
                          'metrics_delta': metric_delta(before['metrics']['values'], after['metrics']['values']),
                          'etcd_data_logical_bytes_delta': (after['disk']['etcd_data_logical_bytes'] - before['disk']['etcd_data_logical_bytes'])
                              if not before['disk']['errors'] and not after['disk']['errors'] else None,
                          'write_samples': writes, 'fixture_setup_retries': retries, 'read_samples': reads, 'fixture': identities}
                (out / f'{size}-{clients}.json').write_text(json.dumps(result, indent=2))
                if any(r.get('status') != 200 or r['item_count'] != size for r in reads):
                    raise RuntimeError('read failure; preserving isolated data')
                for name, uid in identities:
                    deletion = request('DELETE', 'replicasets/' + name + '?uid=' + urllib.parse.quote(uid, safe=''))
                    if deletion.get('status') != 200:
                        raise RuntimeError('fixture deletion failed; preserving isolated data')
                if request('GET', 'replicasets').get('body', {}).get('items'):
                    raise RuntimeError('fixture cleanup incomplete')
        print('Measurement completed: inspect each status and error; NOT a complete R1 capacity verdict.', flush=True)
    finally:
        for proc in reversed(procs):
            if proc.poll() is None:
                proc.terminate()
        for proc in reversed(procs):
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        for handle in handles:
            handle.close()
        shutil.rmtree(private)


if __name__ == '__main__':
    try:
        run()
    except Exception as error:
        print('FAIL:', error, file=sys.stderr)
        sys.exit(1)
