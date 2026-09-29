# E2 单向适配

源码固定导入自 https://github.com/liu225269/MBFGraph ，commit
`e2d55841bc76a96f2d850f939719d16e0c8fa935`。目录是父仓库的普通源码目录，
没有嵌套 `.git`。保留上游 BSD-2-Clause `LICENSE`，以及 X-Stream 等文件原有
Apache-2.0 版权声明。上游原生入口与算法仍在，新增 E2 入口独立构建。

设计核对使用 SC23 论文 DOI `10.1145/3581784.3607070`（用户 Zotero
`A7R3HHLD` 附件，PDF SHA256
`da3185980d0d8f32ccf0f7f2287a3a3fcabcf207a8fe954c6186c20f680aae5a`），
重点为 §4、§5.1、§5.2、§6.1 和 artifact。公开会议页：
https://sc23.supercomputing.org/proceedings/tech_paper/tech_paper_pages/pap324.html 。

## 构建与运行

依赖 GCC/G++、Boost system/program_options/thread、zlib、libaio、Python 3，
CPU 需支持 AVX2/SSE4.2。当前机器已经具备这些依赖，无需安装系统文件。

```bash
make -j4 bin/benchmark_driver   # 作者原生入口
make -j4 e2                    # bin/e2_append、bin/e2_driver
# bin/oneway_benchmark 是提交到源码库的 Python 入口，无需生成
SG_BENCH_BFS_ROOTS=4 SG_BENCH_PR_ITERS=1 SG_BENCH_CC_RUNS=1 \
  timeout 120s bin/oneway_benchmark INPUT.bin VERTICES EDGES 16 NEW_OUTPUT_DIR
```

`NEW_OUTPUT_DIR` 必须为空，入口拒绝覆盖已有文件。正式实验交给 ScatterGraph
E2 runner，示例 timeout 仅用于临时 LiveJournal 或更小输入。二进制哈希需同时记录
`bin/e2_append`、`bin/e2_driver` 和 `bin/oneway_benchmark`。构建 flags 沿用上游
`-O3 -mavx -mavx2 -std=c++11 -DCOMPACT_GRAPH -msse4.2`；E2 driver 额外
`-faligned-new`，append helper 使用 C++17。所有入口禁用 core dump。

入口参数依次为 INPUT、VERTICES、EDGES、THREADS、OUTPUT_DIR；公共实验 wrapper
可映射自己的五参数顺序。`SG_BENCH_BFS_ROOTS` 默认 1，`SG_BENCH_PR_ITERS`
默认 1，`SG_BENCH_CC_RUNS` 默认 1。原生 engine 要求计算线程数为 2 的幂。

## 机制与算法

| 论文机制 | 实际路径与适配 |
| --- | --- |
| 1/2 KiB 边块上的源顶点 BF | 原生 `core/sg_driver.hpp`，默认块 2048 B，每块 256 个 8 B 边，每个 BF 256 bits |
| MBF-construct | 原生 CRC32 四 hash 和 bitshuffle，256 个 BF 转置为一组，原生 create/update/verify 保留 |
| MBF-query | 原生 AVX2 对转置 BF 批量查询，MAX=600000，按 superpartition 分摊阈值；输出 `[mbf] query active=...` 证明该路径执行 |
| 外部 scatter/gather | `core/x-lib.hpp`、`core/split_stream.hpp`、libaio/O_DIRECT 原块读写与临时更新流保留 |
| 演化图追加 | `e2_append` 从空文件按 1,048,576 边（8 MiB）批次追加原始 bin32，保留边多重性、原始到达顺序，更新 degree sidecar；原生 BF update 重做最后一个 BF 组及新增组 |
| 可选 MBF-sort | 本次 E2 不执行排序，输入保持公共 shuffled 次序。论文 §6.1 默认使用 MBF-sort，随机次序可能显著降低过滤收益；这是本候选结果的适配差异 |

没有构建内存 CSR 或全图邻接表。统一 16 B 原生 vertex state，使 index/BFS/PR/CC
的自动分区结果一致；否则原生不同算法的状态宽度会改变分区数量，导致重复构建
BF 或错误复用。算法结束增加原生 state-iteration 结果访问器，收集校验值，测试时
可通过 `MBF_E2_DUMP=/absolute/path` 导出每顶点结果。

- BFS：沿原生分阶段 scatter/gather，源顶点是输入第一条边的 source，此后逐个加一
  对顶点数取模。只沿出边传播，校验包含 root；每个 root 的可达数相加。
- PR：每轮初始分数为 float `1/n`，先求 dangling mass（double 求和），使用公共
  实现的 float `0.85` 阻尼及 float base；scatter 生成每条边的 contribution，
  gather 在对应目标分区累加，完整保留重复边贡献。正式 E2 固定一轮，未沿用
  原版阈值 delta-PR。多轮通过已计时的原生进程和 float score 文件衔接，额外
  文件 I/O 同样计入 PR。
- weak CC：scatter 为每条单向边产生 `(src,dst)` 更新，gather 对端点执行并查集
  合并。并查集 parent 是额外 `4*n` B 原子数组，没有保存反向边；最小顶点作为
  分区标识。第一 scatter 的活跃集合是完整顶点域，BF 阈值可能选择顺序读。
  不能使用原生单向 label propagation 的结果代替 weak CC。

PR/CC 第一阶段都处理完整边集，E2 的一次 push PR 与完整 weak CC 会减少原论文
稀疏活跃集优化的机会。BFS 保留按 frontier 过滤。这是“相同 E2 语义”的候选实验，
不能将本结果说成完全重复论文中的 delta-PR 工作负载。

## 配置、计时与资源

默认环境覆盖项：`MBF_PHYSICAL_MEMORY=1073741824`、`MBF_PAGE_SIZE=2048`、
`MBF_BITS_PER_BF=256`、`MBF_MAX_CHECK_V=600000`。禁止 MAX=1。
正式配置 16 个计算线程（包括原生主线程）加 1 个原生 I/O 线程；另有一个等待
子进程的 Python 编排进程。append/index 构建主要是顺序单线程，后台 I/O 线程
仍由原生 engine 管理。原生 autotuner 的 partitions、superpartitions、buffer
大小与实际内存预算会完整输出。

`physical_memory` 只约束原生 buffer 预算，不能当作进程 RSS 上限。适配额外使用
4 B/vertex degree 文件（append 时在内存维护；PR 时 mmap）；CC 额外使用
4 B/vertex parent 数组。多轮 PR 的中间 scores 为 4 B/vertex。vertex state
可由原生 superpartitions 分批载入。Linux page cache 允许存在，不声称冷缓存。
边查询保留原生 O_DIRECT/libaio，append 与 BF 构建用 buffered I/O，再显式 fsync。

- `Load` 仅包括输入长度/元数据检查；没有免费预载整图。
- `Ingest` 从空目录开始，包括输入流读取、边追加、degree 统计/写入、metadata、
  原生分区与 BF 构建、子进程启动和结束，以及 graph/index 文件 fsync。
  `[mbf-timing]` 单列 append、index、sync。完成后可查询，未声称验证崩溃恢复。
- BFS/PR/CC 包括对应原生子进程启动、buffer/state 初始化、BF 加载/查询、
  原生算法、结果统计与退出。顺序为 BFS → PR → CC，无额外免费预热。
- `RSS_Ingest` 输出 **bytes**，定义为摄入期间原生子进程与编排进程的最大单进程
  RSS；旧 E2 某些系统是摄入结束时 RSS，不能将这两个定义视为完全相同。
  wrapper 的 GNU time peak RSS 是全流程最大单进程 RSS，另行保留。
- 数据目录含单份原始边、BF、必要分区副本（当 superpartitions>1）、degree、
  vertex/update 临时流；全部计入 runner 的 `du` footprint。没有隐藏预处理副本。
  运行日志也保存在该目录便于排错；正式 raw 由外层 runner 完整保存。

## 正确性与必要修复

原生入口先在 16 顶点/6 边小图跑通 BFS（create），日志保存在 ScatterGraph 的
`output/tmp/mbfgraph-native-20260930/native.log`。后续可重复运行独立参考验证：

```bash
python3 tests/verify_e2.py --output /absolute/path/to/output/tmp/mbfgraph-new-test
```

验证覆盖孤立顶点、dangling、非连续活跃顶点、自环、重复边、反向编号连接、
跨 2 KiB 块/256-BF 组的高阶顶点、非整除顶点域、两次插入之间分别查询、
一/多 superpartition 以及 16 计算线程。BFS 比较每个 root 的完整可达 bitmap，
PR 比较每顶点 float 值，CC 比较每顶点最小 component label；字节比较原始边流
确保多重性和次序。`tests/check_bf.cpp` 独立检查每条边的源顶点四个 hash bit，
要求无假阴性；原生 verify 检查 BF 文件逐字节等价。日志还核对 update 的
`source_start_edge`，确保已完成 BF 组不被全量重建。

导入后只修复必要问题：Makefile 自动建立 bin/object_files；原生分区 update 的
`fread` 返回元素数检查；空 BF update 的无符号下溢；BF 临时数组的 `delete[]`；
4 顶点 SIMD padding 的数组上界；vertex buffer 对非整除顶点域向上取整；空 edge superpartition 不注册无法被读取请求消费的 bitmap。E2
index-only 入口在 BF 构建后直接停止，无需执行 degree 算法额外扫描全部边。

2026-09-30 的完整适配验证保存在 ScatterGraph
`output/tmp/mbfgraph-correctness-20260930-3/`：四个场景全 PASS，48 次逐 root
可达 bitmap 检查，8 次逐顶点 PR/CC 检查；逐顶点 PR 最大绝对差
`1.339e-9`。`reference-summary.json` 与各 case 的 `reference-*` 保存独立参考
结果，`summary.json` 保存测试状态。此前空分区问题的失败日志在
`output/tmp/mbfgraph-correctness-20260930-2/`，未覆盖。
