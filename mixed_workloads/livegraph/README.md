# LiveGraph E12 一般更新适配

独立于旧 E2 的私有入口；不改 LiveGraph 上游、SG 原有类、论文或已有结果。
协议直接包含 `sg/mixed_workload/workload.h`，算法直接包含共同的 `analytics.h`。
这里只实现更新完成后的最终图分析。本入口不提供更新/查询并发。

## 原生操作与适配语义

- Native `put_edge(..., force_insert=false)` / `del_edge` 保存边与删除版本。
  插入前点查当前可见边，重复插入和不存在边删除 no-op，自环保留。
- 初始 `[0,base_vertices)` 存活；其余物理 ID 槽在 preload 预留，逻辑上保持 dead。
  后续用 native `put_vertex` 激活槽，删除调用普通事务 `del_vertex(v,false)`。
  不调用 native ID recycle，避免改变共同 trace 的逻辑 ID。
- 每槽 alive 1 B / birth 8 B，每条 native 边版本追加 8 B 插入序号属性。
  查询同时要求两端存活且 `edge_seq >= max(birth[src],birth[dst])`，因此删点后
  入出边不可见，重建不会复活历史边。原生删点本身没有级联删除这些边。
  旧边继续占物理空间；不建完整 RAM 边副本，不扫描全图处理单次删点。
- 混合 chunk 内按完整 trace 原序执行，默认每 512 条共用一个普通原生事务。
  事务内可读到自己的修改，alive/birth 同步推进。执行异常使整个进程失败，
  不发布部分结果。原生 WAL 开启，每个 commit group 自带 fdatasync。
  这样避免每次顶点操作产生一个独立同步；本入口的混合更新为单写线程。
  纯边 chunk 可按 source 分到互不冲突的 worker；同 source 保持原序。
- Preload 和 analytics 使用请求的线程数。运行配置及事务数进入 JSON，不能将
  本适配器的执行策略写成 LiveGraph 只能单线程更新的系统能力限制。

## 计时和输出

`E12_JSON mixed_summary` 与 Diorite 同名字段：

- `load_s = update_apply_s + final_drain_s`。不含 preload 和 trace 读取；原生点查、
  生存状态更新、事务组织、原生提交/同步均计入 apply。Final drain 同步 block/WAL 文件。
- `initial_load_s` 包含槽预留、native 图初始化、原始边读取、checksum、幂等插入
  和 base drain。`input_read_s`、`input_validation_s`、`base_checksum_s`、`base_add_s`、
  `graph_init_s` 单列且互不重叠；端点验证从 base_add 中扣出独立计时。
- `trace_read_s` 包含共享 reader 的格式、计数和 checksum 验证。
  `updates_wall_s` 为读 trace 到最终 drain 的墙钟；`preload_and_update_wall_s`
  为 preload 加上述墙钟。metadata 整图扫描独立为 `metadata_s`。
- BFS 距离摘要、PR 固定迭代、CC 最小代表摘要全部使用共享实现；每个算法新建
  view，每个调用线程自己创建 native 只读事务。没有写入重叠，全部读取同一最终图。
- `--bfs-source` 可指定共同源点；显式源点必须存活且扫描至少一条边。
  正式 FR 固定为 `7994434`，不使用容易被 mandatory probe 改变的最小 ID。
- 四类操作 attempted/effective/no-op、物理 st_blocks、逻辑文件容量、state/scratch
  和进程 peak RSS 进入 summary。`mixed_process_end` 另记录析构同步时间和完整墙钟。
  只有完整读取、校验、应用和全部算法结束后才输出 summary；进程还须正常结束才可验收。

## 构建与正确性

从 `graph_works/` 执行：

```sh
python3 mixed_workloads/livegraph/run_smoke.py
```

脚本非阻塞取得 `scatter_graph/output/tmp/nvme-benchmark.lock`；忙时返回 75。
独占锁覆盖私有 Release 构建、原生语义检查和完整 CLI 小图。输出为
`output/tmp/<YYYYMMDD-HHmmss>-livegraph-mixed-smoke/`，源码/共同头文件快照、SHA256、
命令、所有原始 stdout 和状态均保留；每轮新目录，旧运行数据拒绝覆盖。
配置 60 秒、构建 180 秒、每个小图进程不超过 120 秒；均使用 timeout 并禁用 core。

交叉验收使用已构建的 `build/mixed-workloads/app/sg/mixed_workload/sg_mixed_workload_reference`，
可用 `--cpu-reference PATH` 指定；调用共同 `validate.py` 并设 `require_direct=False`，
不将 O_DIRECT 要求施加给原生 mmap 系统。

检查包括每个边界 probe 前缀对照 RAM reference、随机生命周期、纯边并行事务、
混合事务内的删除/重建、空图、共同算法摘要、1/4 线程 CLI、显式 BFS root、损坏 trace
拒绝、已有 runtime 拒绝及零扫描拒绝。RAM reference 只存在于这些小图检查。
脚本清理 OMP_PLACES/GOMP_CPU_AFFINITY 并设置 OMP_PROC_BIND=FALSE。

2026-10-02 05:12最终验收为PASS，原始目录
`scatter_graph/output/tmp/20261002-051228-livegraph-mixed-smoke/`，binary位于其中
`build/livegraph_mixed_bench`，SHA256为
`cb0ecb7f7d95659e366c16fca3d9d365ec7f7996a69e0492ad54ca77b68a0556`。
共同validator对1/4线程均返回空错误列表；`shared-validation.json`记录CPU对照路径。
此轮已释放公共锁。正式未启动，完整交接见
`scatter_graph/comments/exp-plan/livegraph-e12-adapter-handoff-20261002.md`。

## 正式运行接口与预算（待管理 runner 调用）

完整 FR base 后 10,000,000 条共同记录，seed 20261002、容量 70,002,789，counts 为
`[5775179,1921556,1653322,649943]`，22 条 mandatory probe 已计入。
这是 LinkBench 四操作重新归一的 synthetic subset，不是完整 LinkBench 复现。

```sh
timeout --kill-after=10s 3600s BUILD/livegraph_mixed_bench \
  --input FULL_FR.bin --trace COMMON_TRACE.bin --data NEW_RUNTIME \
  --threads 16 --cpus COMMON_CPU_LIST --chunk-edges 2097152 \
  --transaction-records 512 --pr-iterations 10 --bfs-source 7994434
```

由共同 runner 在公共锁内设置环境并执行；示例中的路径/CPU 需用正式冻结值。
本目录脚本不会自行启动 FR。首轮全部验收后才可进入第二重复。

3600 秒是拟定硬上限，不是已验证的完成时间。旧 FR native 摄入 1321.83 秒仅作
起点：8 B seq 属性对 2,586,147,869 条原始记录增加约 20.69 GB 有效负载（物理块
增量须实测），alive/birth 共 630,025,101 B。512 条事务使约 230 万顶点操作与边
操作共同打包，10M trace 约需 19,532 次 commit，避免数百万次串行 WAL 同步。
仍须考虑原生点查、块扩容、12 次全图扫描（metadata、PR10、CC，另有 BFS）
和生命周期过滤。按旧边结构约 24 B/边到增加 8 B 的有效负载比例，preload 的
一阶模型约 1762 秒，非测量；分配级别、缓存和校验会使实际值偏离。
余下约 1838 秒需覆盖更新、metadata、BFS、PR10、CC 和退出同步，不能保证。
超时保留完整 raw/阶段进度，不将部分 summary 当正式通过，不扩大 trace 或自动续时。
