#!/usr/bin/env python3
"""Repeatable small-graph / mechanism checks. Output must be a new directory on NVMe."""
import argparse
import csv
import os
from pathlib import Path
import re
import struct
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
exe = Path(__file__).resolve().parents[1] / 'bin' / 'xpgraph_s_oneway'

def run(name, cmd, env=None):
    logfile = args.output / (name + '.txt')
    with logfile.open('w') as log:
        subprocess.run(['timeout', '120s', *map(str,cmd)], check=True,
                       stdout=log, stderr=subprocess.STDOUT, env=env)
    return logfile.read_text()

# Both adaptive branches must occur at the same threshold using actual overlapping reads.
env = os.environ.copy()
for key in list(env):
    if key.startswith('XPGRAPH_S_') or key.startswith('SG_BENCH_'):
        del env[key]
text = run('selftest', [exe, '--self-test', args.output/'selftest'], env)
assert 'stage=1 vertices=1024 multiplicity,bfs,pr,weak_cc=PASS' in text
assert 'stage=2 vertices=1024 multiplicity,bfs,pr,weak_cc=PASS' in text
stats = re.findall(r'peak=(\d+) threshold=8 low=(\d+) high=(\d+) merged=(\d+)', text)
assert stats and all(int(value) > 0 for value in stats[-1]), stats
buffers = re.findall(r'allocations=(\d+) upgrades=(\d+) max_bytes=(\d+)', text)
assert buffers and all(int(value) > 0 for value in buffers[-1]), buffers
for stage in (1,2):
    with (args.output/'selftest'/f'per-vertex-stage-{stage}.csv').open() as source:
        rows = list(csv.DictReader(source))
    assert len(rows) == 1024

# Large configurable hierarchy (>65535 buffer capacity), unchanged public query semantics.
wide = env.copy(); wide['XPGRAPH_S_MAX_VBUF'] = '1048576'; wide['XPGRAPH_S_SSD_GRANULARITY'] = '16384'
text = run('wide-buffer', [exe, '--self-test', args.output/'wide-buffer'], wide)
assert 'incremental-tail-and-large-list=PASS' in text
assert int(re.findall(r'max_bytes=(\d+)',text)[-1]) > 65535*4

# Five-argument production path, short final batch, duplicate edge and empty graph.
for name,edges,vertices,checksum,components in [
    ('tiny',[(7,3),(3,2),(2,2),(7,3),(1,0)],9,3,6),
    ('empty',[],7,1,7),
]:
    input_path = args.output/(name+'.bin')
    with input_path.open('wb') as dst:
        for edge in edges:
            dst.write(struct.pack('<II',*edge))
    small = env.copy(); small['XPGRAPH_S_ELOG_SHIFT'] = '15'; small['XPGRAPH_S_BATCH_SHIFT'] = '8'; small['XPGRAPH_S_VBUF_MB'] = '64'
    text = run(name,[exe,input_path,args.output/name,16,vertices,len(edges)],small)
    result = re.search(r'\[result\] edges=(\d+) bfs_checksum=(\d+) pr_sum=([0-9.]+) cc_components=(\d+)',text)
    assert result, text[-1000:]
    got_edges,got_bfs,pr,cc = result.groups()
    assert int(got_edges)==len(edges) and int(got_bfs)==checksum and int(cc)==components
    assert abs(float(pr)-1.0)<2e-6
    assert '[EXPOUT] RSS_Ingest:' in text

print('PASS: native adjacency multisets, per-vertex BFS/PR/weak CC, incremental tail, hierarchy,')
print('      low/high actual-concurrency merge, SSD file readback, five-argument driver and empty graph')
print(args.output)
