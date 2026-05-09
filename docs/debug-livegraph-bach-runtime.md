# Debug Session: livegraph-bach-runtime [OPEN]

## Context
- Goal: verify `LiveGraph` and `BACH` benchmarks on `../bubble-datasets/data/bin32/shuffled/livejournal/livejournal.bin`
- Constraint: run one program at a time, avoid concurrent machine-wide saturation
- Symptom: prior user observation says CPU is idle, suggesting the program may be blocked or waiting rather than computing

## Hypotheses
1. The program is blocked during storage initialization or filesystem access before actual ingest starts.
2. The program is spending a long time in bin32 loading or preprocessing with no visible progress logs, creating the impression of a hang.
3. The program is blocked inside a transaction commit or edge ingestion path because the external-storage backend hits a lock or slow path.
4. The program enters the algorithm phase, but the current benchmark emits no progress logs between algorithm start and finish, so long-running compute looks idle from the CLI.
5. The chosen dataset path or storage directory state triggers an early wait, retry, or pathological branch that does not surface as an error.

## Plan
1. Reproduce with `LiveGraph` only.
2. Add minimal runtime logs to benchmark entry points, without changing algorithm logic.
3. Re-run `LiveGraph` and determine the blocking stage from evidence.
4. Repeat the same process for `BACH` only after `LiveGraph` is understood.

## Evidence
- `LiveGraph` on `livejournal.bin` starts successfully.
- Bin32 loading completes quickly: about 2 seconds for `vertices=4847571`, `edges=68993773`.
- Vertex ingest also completes quickly.
- Edge ingest progresses normally through at least `20971520 / 68993773 (30.4%)`.
- The process then exits with code `139` before printing further progress, indicating a segmentation fault during edge ingest.

## Hypothesis Status
- H1: blocked in storage initialization. Rejected.
- H2: blocked in bin32 loading/preprocessing. Rejected.
- H3: crash or block in edge ingest / transaction commit path. Supported.
- H4: blocked in algorithm phase. Rejected for current run, because algorithms never start.
- H5: dataset or storage state triggers a bad ingest path. Still plausible.

## Refined Evidence
- With per-batch instrumentation, `LiveGraph` completes commit for all batches up to `[19922944, 20971520)`.
- It then prints `edge batch begin: [20971520, 22020096)` and crashes before `edge batch commit start`.
- Therefore the current best-supported sub-hypothesis is: crash happens during repeated `put_edge()` calls inside that batch, not in `commit()`.

## Next Experiment
1. Re-run `LiveGraph` only.
2. Reduce `--batch-size`.
3. Check whether crash follows the same global edge interval or disappears.
