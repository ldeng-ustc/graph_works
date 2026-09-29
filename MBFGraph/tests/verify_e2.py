#!/usr/bin/env python3
"""Native E2 regression with independent small-graph references; no external packages."""
import argparse, collections, json, math, os, pathlib, struct, subprocess, time
P=pathlib.Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser();parser.add_argument('--output',required=True);args=parser.parse_args()
D=pathlib.Path(args.output).resolve();D.mkdir(parents=True,exist_ok=False)
BIN=P/'bin'

def run(cmd,cwd,name,env=None):
    with (D/(name+'.log')).open('w') as f:
        subprocess.run(['timeout','30s',*map(str,cmd)],cwd=cwd,stdout=f,stderr=subprocess.STDOUT,check=True,env=env)
    return (D/(name+'.log')).read_text()
run(['g++','-O2','-msse4.2','-o',D/'check_bf',P/'tests/check_bf.cpp'],D,'build-check')
def f32(x):return struct.unpack('<f',struct.pack('<f',x))[0]
def reference(edges,n):
    adj=[[] for _ in range(n)];par=list(range(n))
    def find(a):
        while par[a]!=a: a=par[a]
        return a
    for a,b in edges:
        adj[a].append(b);a,b=find(a),find(b);par[max(a,b)]=min(a,b)
    initial=f32(1/n);damp=f32(.85)
    dangling=sum(initial for x in adj if not x)
    base=f32(f32(f32(1-damp)+f32(damp*f32(dangling)))/f32(n))
    scores=[base]*n
    for a,row in enumerate(adj):
        if row:
            delta=f32(f32(damp*initial)/f32(len(row)))
            for b in row:scores[b]=f32(scores[b]+delta)
    return adj,scores,[find(v) for v in range(n)]
def reachable(adj,r):
    seen={r};todo=[r]
    while todo:
        for d in adj[todo.pop()]:
            if d not in seen:seen.add(d);todo.append(d)
    return bytes(int(v in seen) for v in range(len(adj)))

def case(name,n,first,second,superparts=1,processors=2):
    d=D/name;d.mkdir();edges=first+second
    source=d/'input';source.write_bytes(b''.join(struct.pack('<II',*e) for e in edges))
    common=['-g','graph','-p',str(processors),'--physical_memory',str(64<<20),'--page_size','2048','--bfs_per_page','256','--bits_per_BF','256','--chip_v_once','4','--max_check_v','600000','--kernel','CPU','--partitions',str(16*superparts),'--fanout','16','--super_partitions',str(superparts)]
    for stage,batch in enumerate([first,second]):
        if not batch:continue
        run([BIN/'e2_append',source,n,0 if stage==0 else len(first),len(batch)],d,f'{name}-{stage}-append')
        mode='create' if stage==0 else 'update'
        index_log=run([BIN/'e2_driver',*common,'-b','e2_index','--bf_mode',mode],d,f'{name}-{stage}-index')
        if stage and len(first)>65536:
            expected_start=65536 if superparts==1 else len(first)
            assert f'source_start_edge={expected_start} ' in index_log
        run([BIN/'e2_driver',*common,'-b','e2_index','--bf_mode','verify'],d,f'{name}-{stage}-bf-verify')
        current=first if stage==0 else edges
        assert (d/'graph').read_bytes()==b''.join(struct.pack('<II',*e) for e in current)
        if superparts==1:pairs=[('graph','bf')]
        else:pairs=[(f'edges.{superparts+i}',f'bf.{superparts+i}') for i in range(superparts)]
        for i,(e,bf) in enumerate(pairs):run([D/'check_bf',e,bf,256,256],d,f'{name}-{stage}-false-neg-{i}')
        adj,scores,labels=reference(current,n)
        (d/f'reference-pr-{stage}').write_bytes(struct.pack('<'+'f'*n,*scores))
        (d/f'reference-cc-{stage}').write_bytes(struct.pack('<'+'I'*n,*labels))
        for root in [0,1,2,7,10,n-1]:
            env=os.environ.copy();env['MBF_E2_DUMP']=str(d/f'bfs-{stage}-{root}')
            log=run([BIN/'e2_driver',*common,'-b','e2_bfs','--bf_mode','use','--bfs::root',root],d,f'{name}-{stage}-bfs-{root}',env)
            reference_bitmap=reachable(adj,root)
            (d/f'reference-bfs-{stage}-{root}').write_bytes(reference_bitmap)
            assert (d/f'bfs-{stage}-{root}').read_bytes()==reference_bitmap,(name,stage,root)
            assert '[mbf] query active=' in log
        for algo,width,expected in [('pr','f',scores),('cc','I',labels)]:
            env=os.environ.copy();env['MBF_E2_DUMP']=str(d/f'{algo}-{stage}')
            run([BIN/'e2_driver',*common,'-b',f'e2_{algo}','--bf_mode','use'],d,f'{name}-{stage}-{algo}',env)
            values=struct.unpack('<'+width*n,(d/f'{algo}-{stage}').read_bytes())
            if algo=='pr':assert all(math.isclose(a,b,rel_tol=2e-5,abs_tol=1e-7) for a,b in zip(values,expected)),max(abs(a-b) for a,b in zip(values,expected))
            else:assert list(values)==expected
    return dict(name=name,vertices=n,edges=len(edges),superpartitions=superparts,status='PASS')

small=[(0,1),(1,2),(2,3),(3,3),(0,1),(10,7),(7,2),(15,10)]
more=[(3,4),(4,8),(20,21),(21,20),(7,7),(0,1)]
high=[(0,1+i%1023) for i in range(66001)]+[(4096,7),(7,2048),(2048,3000)]
added=[(3000,4000),(4000,4001),(0,4001),(4095,4094),(4094,3000)]*57
results=[case('small',32,small,more),case('cross-block',4097,high,added),case('superpartition-tail',4097,high,added,2),case('sixteen-threads',4097,high,added,4,16)]
(D/'summary.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps(results,indent=2))
