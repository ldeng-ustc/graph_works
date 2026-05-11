[OPEN] LSMGraph ingest hang runtime debug

Session ID: `lsmgraph-ingest-hang`

Symptom:
- `lsmgraph_bin32_bench` ingest phase sometimes hangs or appears stalled midway.
- Current live process to inspect: `1654862`.

Falsifiable Hypotheses:
1. The process is blocked waiting for background compaction to finish inside `GetCompactionState()` polling or `AwaitWrite()`.
2. The process is stalled in `put_edge()` because memtable capacity is exhausted and producer threads are waiting on `memtable_cv_`.
3. A background flush/compaction thread is stuck in disk I/O or SST file creation, causing the foreground ingest loop to stop making progress.
4. The bench is deadlocking or starving because two graph instances alternate batches while shared global flags/path-dependent code is still being used by background workers.
5. The process is not deadlocked, but is spinning in a busy-wait path with no useful forward progress.

Planned Evidence Collection:
- Inspect process state with `ps` and `/proc`.
- Attach `gdb` to capture all-thread backtraces.
- Classify blocked threads by wait primitive and owning code path.

Status:
- Initialized. No business-logic code modified in this session.

Evidence Collected:
- `thread 11` stack shows `lsmg::LSMGraph::get_newmemTable()` -> `lsmg::LSMGraph::put_edge()` -> OpenMP ingest worker.
- `thread 1` is blocked inside `libgomp`, consistent with the main thread waiting at an OpenMP barrier for worker completion.
- `thread 2` and `thread 58` are idle on `pthread_cond_wait` inside `ThreadPool` worker threads, not actively doing foreground ingest work.

Current Assessment:
- Hypothesis 1: partially related but not the primary block at sample time.
- Hypothesis 2: supported. Foreground ingest is blocked waiting for a reusable/free memtable.
- Hypothesis 3: not directly supported by the captured stacks.
- Hypothesis 4: still plausible as an amplifier, but the immediate wait point is memtable acquisition.
- Hypothesis 5: rejected for the sampled moment; the process is sleeping/waiting, not pure busy-spin.
