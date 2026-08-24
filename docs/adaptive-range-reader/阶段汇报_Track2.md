# Track 2 阶段汇报：对象存储感知的 Writer Layout Advisor

> 汇报区间：2026-08-04 — 2026-08-21  
> 依据：`TRACK2_PROJECT.md`（执行与变更记录）、`TRACK2_M0_CONTRACT.md`（实验合同，冻结 r4）、`what-if_architecture_design_943271cc.plan.md`（阶段 E 架构计划）  
> 性质：阶段工作梳理，不是合同修订。数字一律按实测机器报告；合同 D-8 的同区 EC2 数字以预留实验 E12 为准。

---

## 1. 一句话结论

Track 2 已经从「研究设想」走到「可运行的 Advisor + 真实云复测」：在跨云 TPC-H SF100 上，按表排序把端到端中位时间从 **3205 s 降到 2019 s（−37%）**，稳定性与基线同级（CV 2.89%），但合同 E8 因 Q18/Q22 单查询回归未过门禁。What-if 的 L1 模型能在不物化候选的情况下复现基线 GET/字节（误差 2.6% / 2.9%），并正确指向「不要全局合并文件、只对无序的日期列排序」。随后在 ClickBench 上证伪了「排序一定更快」：基线已经有序时重排会破坏并行度。L0 因此补了聚簇度与剪枝后并行度两道硬闸门。

当前状态：**MVP 闭环已跑通，合同止损门槛（≥10% 且全部 guardrail）尚未满足；E12 同区复核未做。**

---

## 2. 这条线要解决什么

总项目是面向对象存储的 Intelligent SDK。Reader 侧做缓存、异步、预取、IO 合并和长尾；**Track 2 负责 Writer 侧**：改变 Parquet 的物理组织，让后续读取少打开对象、少发 GET、少读远端字节。

价值主张不是「AI 猜一个更好的布局」，而是：

> 三层 telemetry 决定改什么；Advisor 用可解释的 What-if 估价；AI 只负责定位 Writer 代码并翻译成最小 Diff。性能判断不交给模型直觉。

不可突破的边界（合同继承自 `TRACK2_PROJECT.md` §1.3）：

- **非侵入**：不改 Parquet 格式，不做逻辑地址重映射，不引入文件可读性依赖的外置映射表。
- **建议式**：Diff 必须人工审核，不自动合并，不因当前窗口没访问到就删除列或旧布局。
- **可验证**：每条建议要有 trace 证据、作用机制、Writer 参数和复测方法。
- **真实云验收**：模拟器只能粗筛；对外数字必须来自 AWS S3 / COS。
- **主指标是 wall-clock**：GET、remote bytes、skipped rows 只作代价/解释，禁止事后改成主结论。

止损门槛（计划 §1.2 / 合同 §4.1 / §7 E8）：TPC-H SF100 完整负载端到端 median 改善 **≥10%**，基线 CV **< 5%**，单查询 median 回归 **≤10%**，P95/P99 回归 **≤5%**。达不到则 Track 2 在论文中降为 discussion，不再扩张动作空间。

---

## 3. 怎么设计：从相关工作到可实现的 Advisor

8 月上旬先把「优化器、成本函数、搜索器」的角色拆开，再写合同。核心判断如下。

### 3.1 借鉴什么、不移植什么

| 来源 | 采纳 | 明确不搬 |
| --- | --- | --- |
| **PTO**（湖仓四参数联合优化） | partition / TFS / RGS / sort 作为首轮动作空间；按表决策；保留 `none` 保守候选；压缩负载只用于搜索 | skipped rows 作训练标签；按采样率同比缩小文件/RG；只优化平均延迟而允许单查询大回归；MinIO 数字当对外结论 |
| **DB2 Design Advisor** | `RECOMMEND / EVALUATE` 分离、虚拟结构不物化就估价、工作负载压缩、不建议删除未观测结构、估计与实测并列 | DB2 优化器当代价预言机；索引/MQT 依赖分类；knapsack 搜索（我们空间只有几百点，穷举即可） |
| **ATUN-HL** | 系统常量 × 数据统计 × 负载 × 布局变量；`存活 RG → 字节 → 请求计划 → 时间` 的可解释分解 | HDFS chunk/seek/本地性公式；闭式最优；单一 dictionary size |
| **SIGMOD'17 宽表列序** | 工作负载加权、不能只看相对百分比、避免在弱基线上出漂亮数字 | HDD seek 成本；列复制（要改 schema 和查询） |
| **Qd-tree** | 仅 related work：predicate-aligned cut、anytime 搜索 | 记录级路由树、BID、查询改写——与「只改 Writer 参数」冲突，8 月 5 日从实现范围拿掉 |

论文级观察（阶段 E 计划写明，实验后仍然成立）：

> 湖仓没有 DB2 那种「与运行时自洽的优化器代价预言机」。Spark CBO 不建模 GET 数、RTT、Range 合并；Parquet 的 row-group / page 裁剪发生在 **plan 之下**。因此 What-if 必须自己模拟冻结 Reader 的请求计划，而不是 `EXPLAIN`。

### 3.2 多保真 What-if（计划中的梯子）

```text
L0  静态可行性（Writer/Reader 能力、文件数、单调性、预算）
 → L1  解析模型（virtual footer + 请求计划 + t_io + t_exec_residual）
 → L2  少量真实改写 / 残差校准     ← 本阶段未单独做成 surrogate
 → L3  真实云 top-k canary
 →     SF100 完整负载验收（E8）
```

低保真层只负责淘汰明显较差或不可行的点，**不得把估计值写成最终性能结论**。

### 3.3 观测必须是三层，不是 SDK-only

| Collector | 看见什么 | 实现 |
| --- | --- | --- |
| Semantic | 查询、谓词、投影列、scan fragment | Spark eventlog / plan（`collect_semantic.py`） |
| Format | 文件 → row group → column chunk → min/max | PyArrow 离线解析 footer（`parse_footer.py`），热路径不解析 |
| Physical | Range offset/length、GET、字节、时延 | AWS SDK v2 `ExecutionInterceptor`，经 S3A 审计注入 |

三层用 `query/span id + object key + version + time window` 关联（`correlate.py`）。没有 SQL 计划的负载（Lance/ML）只做物理优化，不得声称恢复了完整谓词语义。

---

## 4. 实验合同怎么冻的（M0，8 月 7 日）

合同把后续所有实验的技术栈、负载、指标、档位和能力矩阵一次写死，禁止为了结果更好看而改主指标。修订过两次关键方向，一次记录环境偏离。

### 4.1 栈：从「Spark 3.5 + PyArrow 写」改到「Spark 4.1 + parquet-mr」

r1 用 Spark 3.5 会同时踩两个坑：自带 Hadoop 3.3.4（S3A 仍在 SDK v1，采集配置会被**静默忽略**），自带 parquet-mr 1.13.1（无 vectored IO）。r2 升到 **Spark 4.1.x**（自带 Hadoop 3.4.2 + parquet-mr 1.15.x），一次解决。

Writer 从 PyArrow 换成 **与 Reader 同库的 parquet-mr**，理由不是「默认值更好看」，而是 r1 里两项错配是自找的集成缺陷：

- PyArrow 默认不写 page index、读侧默认启用 → 实验会得出「page index 无收益」的假阴性；
- `sorting_columns` 只是声明，PyArrow 并不排序。

同库之后，基线就是 `df.write.parquet(...)`，不必再人工「对齐默认值」。`parquet.block.size` 以字节计，消灭行数↔字节换算。SF100 可分布式写。

能力矩阵里现在还剩下的真实错配：

- **M-2** bloom 写关读开（库内设计，须显式打开写侧，否则又是假阴性）；
- **M-3** vectored IO 默认值在 parquet 1.15/1.16 之间翻转，合同显式冻成 `true`；
- **M-5** parquet-mr 不能 per-column 压缩/编码。

另外一个可写进论文的发现：三个主流 writer 对 **ColumnIndex/OffsetIndex** 行为完全不同——parquet-mr 恒写、Trino 原生 writer **不写**、PyArrow 默认不写。同一条「靠页级裁剪」的建议在 Trino 数据上先验无效，而且是静默失效。这就是必须做 Writer × Reader capability matrix 的实证理由。

### 4.2 动作空间是格式级的，不是引擎级的（r3）

Flink/Trino 适配器不进 M1–M4。但候选动作现在用 Iceberg 属性词表的 **canonical 名**（`write.parquet.row-group-size-bytes`、`sort.columns` 等），分析/What-if/约束只看 canonical，只有最后生成 Diff 才 render 成 parquet-mr。MVP 仍写裸 Parquet 而不是 Iceberg，避免 manifest 额外 GET 抬高归因覆盖率难度。

### 4.3 负载、指标、档位

- 负载：TPC-H 标准 22 条，均匀权重；SF100 由 DuckDB `dbgen` 生成一份规范源，所有候选从这份源重写，禁止重新 dbgen。
- 候选排序目标：scan/IO critical-path wall-clock median。
- 最终验收：完整负载端到端 median；同时必须逐查询报告。
- 主 profile：cold-cache（每轮新 JVM）。
- 首轮网格：partition × file size × RG size × sort（含 `none`）。列顺序、page 粒度、bloom、dictionary 留 M5。
- Reader 全程冻结（vectored IO、S3A 合并阈值、filter 开关）；比较布局时 Adaptive SDK 五维全部关闭。

### 4.4 合同写的环境和实际测的环境不是同一台（r4）

D-8 冻的是 **同区** `m5d.4xlarge` 访问 `us-east-2`（Track 1 同区 RTT ≈ 25 ms）。已归档 E2/E8 跑在**腾讯云 VM 跨云访问**同一桶（RTT ≈ 228 ms）。合同目标环境不改；What-if 把 `(RTT, BW, K)` 做成显式输入；**预留 E12**：论文数字冻结前在同区机器重跑基线与验收。缺席则对外不得写成 D-8 / 同区 EC2。当前 3205 s 只是跨云工作基线。

---

## 5. 系统怎么搭起来的

代码落在 `tools/track2/`，采集钩子在 `s3-adaptive-range-reader`。拦截器只实现 `ExecutionInterceptor`，不依赖 Hadoop，Java 8 编译、JDK 17 运行。

### 5.1 流水线

```text
E0 能力探针（probe_capability.py）
  → DuckDB dbgen SF100 → Spark 按候选写出（write_layout.py）
  → E2 基线 22×5 cold-cache（run_benchmark.py，每条查询打 track2:q<N> 标记）
       ├─ SDK interceptor → io/*.ndjson
       ├─ Spark eventlog → 语义
       └─ footer → 几何
  → correlate.py 三层关联
  → sysconst.py 拟合 RTT / BW / K_busy
  → column_stats.py 一次扫描：NDV / CDF
  → dataset_snapshot.py   ：对象列表 + 抽样 footer + column_stats → 布局几何 / rg_span
  → workload_snapshot.py  ：eventlog → 每条查询的 scans[]（投影 + 下推谓词）
  → predicates_from_runtime.py：eventlog 谓词 → 排序键 / 分区键候选
  → adaptive_physical_options.py：实测几何 → 文件数 / 行组数候选
  → analyze_layout.py（RECOMMEND）：问题报告 + 压缩负载 + 候选网格
  → whatif.py（EVALUATE）：L0 → virtual footer → L1 → 穷举 / 迭代分解 / RTT sweep
  → write_layout 物化 top-1
  → E8 同口径复测 + e8_gate.py
```

`write_layout.py` 是合同指定的 **Code Diff 目标文件**：canonical 动作在这里变成 parquet-mr 配置和 DataFrame 变换（`repartitionByRange` + `sortWithinPartitions`），分析器本身不知道引擎细节。

**Advisor 的输入全部是实测快照（2026-08-24 改）**。此前 `workload.py` 一个文件里混着三类东西，都是手抄常量：动作网格（`FILE_SIZE_GRID` / `SORT_GRID` / `PER_TABLE_SORT`）、数据集快照（`BASELINE_GEOMETRY` / `COLUMN_ORDER` / `SYNTHETIC_STATS`）、查询目录（`QUERIES`）。现已拆开：

- `dataset_snapshot.py` —— 几何与列事实按需从 footer / 对象列表 / column_stats 读。它描述的是"某个目录此刻长什么样"，不是 workload；写死就意味着候选布局写出来之后没法对**它**重新估价。
- `workload_snapshot.py` —— 每条查询的 `scans[]` 从 eventlog 读。手抄目录抄的是 SQL 文本说了什么，L1 要估价的是 **Spark 实际读了什么**，两者不等（详见 6.8）。
- `adaptive_physical_options.py` —— 文件数与行组数候选由实测几何在**计数空间**生成，不再是字节网格。
- `advisor_policy.py` —— 闸门阈值与模型假设，与数据集无关，明确标注哪些是"决策"、哪些是"承认的近似"。
- `hand_catalog_tpch.py` / `hand_catalog_clickbench.py` —— 原 `workload.py` / `clickbench_workload.py` 的残余，只剩 `QUERIES`，**冻结**，唯一用途是给运行时目录做交叉验证。`analyze_layout` 已经没有手写网格 fallback：没有 eventlog 直接报错。

**候选键的来源**：排序键和分区键由 `predicates_from_runtime.py` 从 eventlog 的 `PushedFilters` 推导，按各 execution 自身墙钟加权；range 谓词进排序候选，低 NDV 列进 identity 分区候选。此前它们来自手写常量，而"证据"函数 `sort_from_predicates` 从另一份手抄目录算出同样结论、返回值还没人消费——是循环论证。改后 TPC-H 上运行时独立复现了 `l_shipdate` / `o_orderdate`；ClickBench 上 `CounterID` 因只有等值谓词而根本不被提名，那正是造成 +23.5% 回归的键。

### 5.2 L1 代价（阶段 E 计划落地后的公式）

先导发现把整个模型钉在「请求延迟主导」而不是「字节主导」上。E2 的 io NDJSON（约 36 万条）给出：

- 每 run ≈ **64,032 ranged GET + 8,859 HEAD**，约 **149.3 GiB**；
- 5.5 KB GET 中位 228.5 ms，15.7 MB GET 中位 294.5 ms → **RTT ≈ 0.228 s，BW ≈ 226–237 MiB/s**；
- **81% 的 GET < 1 MiB**；busy 区间平均并发 **K_busy ≈ 6.8**（不是 `local[16]`——vectored 上限 4，大量是 HEAD+footer）。

因此：

```text
requests = 每打开文件 (HEAD + 数个 footer/page-index 小 GET)
         + 每 (存活 RG × 投影列) 按冻结 vectored 规则合并后的数据 GET
t_io     = Σ_scan (n_req × RTT + bytes / BW) / K_eff
           K_eff = min(K_busy, n_files_opened, 16)
t_exec_residual = (实测 median − 基线 t_io) × (候选扫描行数 / 基线扫描行数)
           × 无谓词多表扫描时的 (n_files_base / n_files)^0.5
t_e2e    = t_io + t_exec_residual
```

`t_exec_residual`（旧名「CPU 残差」，已改名）不是 CPU 模型，是标定残差：解压解码、聚合、shuffle、调度、JVM/GC **以及 I/O 项自身的误差**全被它吸收，然后按扫描行数线性外推。线性外推对 shuffle/join 阶段和每查询固定开销都是错的——改名是为了让这份粗糙可见，而不是让它听起来像知道 CPU 在干什么。

virtual footer 不写数据：按候选的文件大小/RG/排序预测几何；谓词列若是 sort 前缀则用 CDF 算存活 RG，否则按无序 overlap（≈ 不裁剪）；`l_receiptdate ~ l_shipdate` 是可证伪的经验分支。

自洽门禁（计划里 DB2 没有、我们能做的）：L1 对**基线候选**必须复现 E2 的 GET 数和字节，误差 ≤10%。这是模型能不能信的第一道证据。

---

## 6. 按阶段实际做了什么

### 6.1 8 月 4–7 日：定范围、冻合同

- 把布局拆成表 / 文件 / row group / page 四层，建议必须落到少对象、少 GET、少字节或更好局部性。
- 确认 PTO 是直接 baseline；列复制、Qd-tree 出范围；列顺序不进首轮门禁。
- 写出合同 r2/r3、能力探针、canonical 动作词表。
- 这一周没有 SF100 数字，产出是「后面所有实验不许改的规则」。

### 6.2 阶段 D（8 月 14 日）：E2 基线过门禁

Spark 默认布局写入 `s3a://home-haoyue/track2/baseline_sf100`，22×5 cold-cache。

| 项 | 结果 |
| --- | --- |
| 五轮 wall-clock / s | 3288.7, 3188.2, **3205.0**，3228.3, 3112.9 |
| CV | **1.99% < 5%，PASS** |
| 每 run | ≈ 64032 GET，149.3 GiB |
| 查询 | 最慢 Q21 295.7 s，最快 Q16 29.4 s；Q6 151.1 s |

过程问题（都留下了，没有改合同口径）：第一次全量被 `/tmp` ENOSPC 打断，改 `spark.local.dir` 后重跑；`report.json` 的 io 字段曾误累加五轮 NDJSON，墙钟不受影响。lineitem 被 Spark 写成 200 个文件，RG 未压缩中位约 247 MiB。

### 6.3 阶段 E（8 月 17 日）：What-if 落地，E5 自洽过门禁

按计划实现 `sysconst.py`、`column_stats.py`、`analyze_layout.py`、`virtual_footer.py`、`whatif.py`。

- **自洽**：预测 62391 GET / 153.68 GiB vs 实测 64032 / 149.32，误差 **2.6% / 2.9%，PASS**。`t_io` 2418 s，实测 3211 s，差额作逐查询 `t_exec_residual`。
- **穷举**（可读 RG 约束后 257 点）：最优 `p-none_f-1GB_rg-128MB_s-l_shipdate`，预测 −25%。DB2 式迭代分解命中同一点，regret 0%。`partitionBy` 和过小 RG 在高 RTT 下增加请求，预测为负收益。
- **256 MiB RG 被 L0 拒绝**：第一只 canary 写出未压缩中位 ~473 MiB RG，vectored range 35–182 MiB 触发 parquet-hadoop 写死的 300 s 超时。这不是 Reader 冻结旋钮，故 L0 增加「requested RG ≤ 128 MiB」。
- **RTT 敏感性是负结果**：25 ms 与 228 ms 的 top-1 相同（该点同时减少请求和字节，仿射代价翻不了名）。Spearman 0.97。计划曾期望「推荐随 regime 改变」作为 advisor 存在价值的证据，实验没有给出翻转。
- **相关列经验分支**：关掉后同一 top-1，预测只差约 72 s，暂留。
- **M2 压缩负载（10 查询 × 5）**：L1 最优布局实测 −34.5% vs 压缩基线，top-3 命中，regret 0%，**M2 排序门禁 PASS**。墙钟 CV 5.26%，略超 E2 的 5%，`run_benchmark` 因此 exit 1，但不改排序结论。

### 6.4 阶段 F / E8 第一轮（8 月 18 日）：端到端变快，合同仍 FAIL

对 L1 推荐的全局 `1GB + l_shipdate` 跑 22×5。

| | E2 | E8 全局 1GB |
| --- | --- | --- |
| median / s | 3205 | **2128（−33.6%）** |
| CV | 1.99% | **39.5% FAIL**（第一轮 4304 s 离群） |
| Q11 / Q18 | 45.8 / 170.2 | **+18% / +21%**，单查询 guardrail FAIL |

≥10% 这一条过了；稳定性和单查询回归不过。原因很快定位：**全局 target-file-size=1GB 对 22 GB 的 lineitem 合适（→22 文件），却把 orders 100→7、partsupp 50→5，低于 `local[16]`。** 顾问过拟合了 fact 表。合同禁止丢掉最慢一轮来过门禁。

### 6.5 纠偏：并行度下界、按表动作、第二轮 E8（8 月 19 日）

三步，每一步都是对上一轮失败的直接回应，不是另起炉灶。

1. **L0 大表文件数 ≥ min(16, 基线)**；L1 用 `K_eff = min(K_busy, n_files_opened, 16)`，无谓词 join/agg 再乘文件数惩罚。全局网格在 L0+§4.1 下**合法集只剩 baseline**——不是搜坏了，是 128 MB 已经粗于 orders/partsupp 的基线文件。
2. **搜索空间改成按表**：lineitem 5×2、orders 3×2、partsupp 3×1 → 180 点。合法 4 点，全部不改 file size。最优 `li-baseline-shipdate_o-baseline-odate_ps-baseline`，预测 −25.7%，max regression 0%。
3. **按该布局重写并复测**（文件数钉死为 E2 基线，只改行序）：

| | E2 | E8 1GB（旧） | E8 按表 sort |
| --- | --- | --- | --- |
| median / s | 3205 | 2128（−33.6%） | **2019（−37.0%）** |
| CV | 1.99% | 39.5% FAIL | **2.89% PASS** |
| Q11 | 45.8 | +18% | **−2%** |
| Q18 | 170.2 | +21% | **+32% FAIL** |
| Q22 | 39.6 | −16% | **+16% FAIL** |
| 合同 E8 | — | FAIL | **FAIL（Q18/Q22）** |

端到端和稳定性都达到合同要求；Q18（两次全表 lineitem，日期排序裁不到）和 Q22（扫 orders 无日期谓词）仍回归。L1 预测 Q18 持平，没抓住无谓词扫描在排序后的代价。合同门槛未改。

### 6.6 ClickBench 泛化与两道新闸门（8 月 20–21 日）

同一套 Advisor 接到单表 `hits`、官方 43 条查询。14 GiB SF1 在本地和跨云 S3 上都测了基线 vs `CounterID` 排序。L1 预测会快，实测整体变慢（S3 约 **+23.5%**）。

原因与 TPC-H 对称、方向相反：

- TPC-H `l_shipdate` 的 `rg_span ≈ 0.999`（每个 row group 几乎覆盖全值域），排序是从零造出裁剪；日期范围仍然跨许多文件，并行度还在。
- ClickBench 来自 ClickHouse 主键 `(CounterID, EventDate, …)`，`CounterID` 的 `rg_span ≈ 0.080`，基线已经能裁。`CounterID=62` 只占 0.74% 的行，排序后从 3 个文件压到 **1 个文件 / 2 个 RG / 1 个 Spark task**。剪枝省下的字节和并行度损失是同一动作的两面。带 `CounterID` 谓词的查询只占基线总时间约 4%，收益上界本来就很小。

因此 L0 增加（阈值可关，留给消融，**不做「谓词时间占比」闸门 B**——与聚簇度是同一信号）：

| 闸门 | 参数 | 默认 | 作用 |
| --- | --- | --- | --- |
| A 基线聚簇度 | `--cluster-span-min` | **0.5** | 排序前缀 `rg_span` 低于阈值则拒。CounterID 0.080 拒；`l_shipdate` 0.999 / `o_orderdate` 1.000 过 |
| C 剪枝后并行度 | `--prune-parallelism-floor` | **4** | 存活 RG **且** 存活文件都低于门槛则拒。`K_eff` 仍用于合法点排名。默认不是 16：TPC-H 三个月窗口在 orders 上大约 5 个文件，那是测过的赢家 |

`rg_span` 从 footer min/max 收集。UserID 点查（Q20）过闸门 A、被闸门 C 拦住。TPC-H 获胜布局仍过。未用新闸门重跑 E8。

> **闸门 A 后来被实测证伪，已改写（见 6.8）**：它当时的标定依据是**手抄**的 ClickBench `EventDate` `rg_span = 0.5232`，恰好卡在 0.5 上方；实测是 **0.358**，即这个绝对阈值会否决 E8 实测 −51.3% 的那个布局。闸门 C 未变。

### 6.7 候选键改由运行时谓词生成，分区动作空间重定（8 月 24 日）

闸门 A/C 是在**候选已经被提名之后**才拦截。往上追一层，提名机制本身就是错的：`sort_from_predicates` 只把实测中位数当标量权重，谓词结构全部来自手抄的 `workload.QUERIES`，而且它的返回值没有任何消费者——真正的候选来自 `SORT_GRID` / `PER_TABLE_SORT` 两个手写常量。同时 `collect_semantic.py` 采集的 `PushedFilters`（真正的运行时谓词结构）只被 `correlate.py` 用于覆盖率记账。

新增 `predicates_from_runtime.py`：eventlog → `(表, 列, 算子, 字面量)`，按各 execution 自身 `end_ms − start_ms` 加权（不需要 query-id 映射）。range 谓词进排序候选，低 NDV 列进 identity 分区候选。`analyze_layout` 的 per-table 网格改为 `large_tables()`（实测几何）× 运行时键的笛卡尔积；手写常量降级为无 eventlog 时的 fallback。

离线验证（未重跑基准）：

| 数据集 | 网格 | 闸门 | top-1 | 预测 |
| --- | --- | --- | --- | --- |
| TPC-H SF100 | 手写 fallback（180 点） | A/C/D | `lineitem-shipdate` + `orders-orderdate` | 2385.0 s（逐位复现归档） |
| TPC-H SF100 | **运行时**（1620 点） | A/C/D | **同一布局** | 2385.0 s（= 已实测 −37% 的 E8 点） |
| ClickBench SF1 | 手写 | 全关 | **`CounterID`** | −14.5%，实测 **+23.5% 回归** |
| ClickBench SF1 | **运行时**（24 点） | A/C/D | `EventDate`，CounterID 未被提名 | −27%，**实测 −51.3%（n=2）** |

TPC-H 上运行时目录独立复现了手写的两个键（`l_shipdate` 5839.2 s / n=35，`o_orderdate` 3732.5 s / n=25，各自表第一名），且 1620 点网格选出的 top-1 与归档 E8 的 `cand_ptable_sort_sf100` 是同一个布局——那个点已测 2019.2 s vs 基线 3205.0 s（−37%），所以不需要重写 SF100。ClickBench 上 `CounterID` 只以等值谓词出现（ndv=7526），进不了排序候选；运行时给出的 −1.4% 本身就是正确答案——基线已聚簇，没有收益空间。

换成运行时枚举之后，**两个此前看不见的几何建模错误立刻暴露**——手写网格只含测过的点，模型错了也没人知道；一旦枚举没测过的点，错误就会浮出来：

1. **`partitionBy` 的文件数是 `F × P`。** `write_layout` 先按排序键 `repartitionByRange` 再 `partitionBy`，而 partitionBy 由每个写出 task 各自执行；排序键与分区键不相关，每个 task 都含全部分区值。lineitem 200 × 3 = 600 个 36 MiB 文件，228 ms RTT 下多出的 open 吃掉全部剪枝收益。修正后两个数据集上都没有分区候选通过 §4.1——它们是被代价否掉的，不是被闸门否掉的。
2. **排序键的 NDV 是文件数上界。** `repartitionByRange` 的非空区间不多于键的相异值个数。ClickBench `EventDate` 只有 17 个日期，请求 110 个文件、实际写出 17 个 882 MiB 文件。按 110 估价预测 −1.4%，按真实的 17 估价是 −27%（每次全表扫描少开 93 个文件，43 条查询累积 GET 从 44880 降到 28357）。

**ClickBench E8 实测**（`e8_sf1_eventdate_s3`，跨云 S3，n=2）：基线两轮 1826.0 / 2720.7 s（中位 2273.3），EventDate 两轮 1112.9 / 1103.3 s（中位 **1108.1**），**−51.3%**；43 条里只有 Q40 变慢（+9.0%），CV 0.62%。即使对比基线较快的那一轮仍为 −39.3%。之前在 `CounterID` 排序下整体回归的 Q37–43 现在普遍变快。**合同 n=5 未做，不能写成过门禁**；基线 n=2 方差大，这是方向性结论。

**遗留风险**：L0 可读性闸门只看**请求的** RG 大小，看不见「排序压出更少更大的文件、RG 随之变大」这条路径。EventDate 布局实测 RG 中位 335 MiB（基线 192 MiB），M2 canary 曾在约 473 MiB 触发 vectored 300 s 超时；这次 399 MiB 读下来了是运气不是设计，应把预测 RG 大小纳入闸门。

**分区动作空间重定**：`l_shipdate:year` 这类派生变换在 bare Parquet 上可证明惰性（Spark 按分区列名匹配谓词，查询从不提 `l_shipdate_year`，零目录被裁而文件数上升），移除并记为负结果；要生效需 Iceberg hidden partitioning，不是 writer 配置问题。替代品是运行时低 NDV 列的 identity 分区，`write_layout` 本就原生支持，**无需改 writer 代码**。L0 新增闸门 D（`--max-partitions` 64 / `--min-partition-bytes` 128 MiB）。

### 6.8 workload.py 拆解：几何 / 查询目录 / 动作空间全部改为实测（8 月 24 日，其二）

6.7 只换掉了**键**的来源。往上再追一层，`workload.py` 里还剩三类性质完全不同的手抄常量（动作网格、数据集快照、查询目录），拆解方案见 5.1。这里只记**拆解暴露出来的错误**——它们都不是重构引入的。

1. **`PushedFilters` 正则的括号 bug**，与 6.7 修的逗号 bug 同源。`\[(.*?)\]` 非贪婪匹配到第一个 `]`，而 `In(l_shipmode, [MAIL,SHIP])` 的第一个 `]` 在括号**内部**，整条谓词被截断后解析失败、静默丢弃。TPC-H Q12 和 Q19 的全部 IN 谓词就是这么消失的（谓词数 60 → 81）。`Location` / `ReadSchema` 同写法，一并改为括号配对扫描。
2. **`Not` 展平时没取反**：`Not(EqualTo(p_brand, Brand#45))` 记成了 `eq`，L1 以为 Q16 只读 1/ndv 的行，实际读 1−1/ndv，差 24 倍。对排序键**排名**无影响，对选择率模型有影响，而新的 workload snapshot 正是喂给后者的。
3. **手抄目录漏数扫描**：计划里 TPC-H 有 **95** 个 `Scan parquet`，手抄目录只有 76 个——Q2 的相关子查询重扫 partsupp/supplier/nation/region，Q18 三扫 lineitem，Q22 两扫 customer。L1 一直在低估这些查询。手抄目录的谓词集合是运行时集合的**真子集**（`hand-only` 全空）。
4. **`BASELINE_GEOMETRY` 的 lineitem 行组大小 247 MB 是抽样偏差**：实测分布三簇（1 / 161 / 236 MiB），平均 **1.5 个行组/文件**而非抄进去的 2。原 manifest 抽的是**前** 20 个文件，恰好都在 236 MiB 那一簇。快照改记均值——L1 只以 `n_rg × rg_bytes = 总未压缩字节` 的形式消费它，两个中位数都合法但都还原不出总量。
5. **闸门 A 被证伪**：手抄的 ClickBench `EventDate` `rg_span = 0.5232` 卡在阈值 0.5 上方，**实测 0.358**，旧形式会否决 E8 实测 −51.3% 的布局。绝对阈值的形式本身也错——完美排序后每个行组约覆盖 `max(1/n_rg, 1/ndv)` 的值域，所以 0.358 在 165 个行组的表上叫"几乎没聚簇"、在 3 个行组的表上叫"已经完美"。改为余量比：`rg_span < max(1/n_rg, 1/ndv) × --cluster-headroom-min`（默认 2.0）。判定：`l_shipdate` 300×过、`o_orderdate` 100×过、`l_orderkey` 1.29×拒、`EventDate` 6.1×过。**遗留近似**：`rg_span` 量值域而排序均衡行数，对偏斜列偏松。

离线验证（未重跑基准）：

| 数据集 | 自洽 GET 误差 | 自洽字节误差 | top-1 | 与归档对比 |
| --- | --- | --- | --- | --- |
| TPC-H SF100（重构前） | 2.6% | 2.9% | `lineitem-shipdate` + `orders-orderdate`，2385.0 s | E8 实测 −37% |
| TPC-H SF100（**重构后**，2160 点） | **6.2%** | **0.5%** | 上式 + `orders-32f` + `o_orderstatus` identity 分区，2374.8 s | 归档 top-1 落在**第 3 名**（2398.7 s，差 1.0%） |
| ClickBench SF1（**重构后**，16 点） | E2 未记 IO | — | **`hits-baseline-eventdate`** | **与归档/实测 top-1 完全一致** |

字节误差从 2.9% 降到 **0.5%**，是几何与投影列表同时改为实测的直接结果。GET 误差**变差**（2.6% → 6.2%）：扫描节点从 76 涨到 95，每个都计了一次 open，而 Spark 在同一查询内会复用部分 open——这是一个**具名的模型缺口**。重构前 2.6% 那个好看的数字，部分来自"漏数扫描"与"高估行组数（400 vs 实际 300）"两个方向相反的错误。TPC-H top-1 变化 1.0%，在模型分辨率以下，**不宣称是改进**；新 top-1 是旧手写网格表达不出来的点（512 MB 档在 orders 上只剩 13 个文件、低于并行度下界被拒，而分区候选从来只在基线文件数上评估过）。m2_gate 判据（L1 top-3 含实测最优）仍成立。

---

## 7. 合同实验清单对照

| ID | 内容 | 状态 |
| --- | --- | --- |
| E0 | 环境、能力探针、vectored IO、带宽 | 已做（探针 + 写出回读）；持续带宽/同区实例未按 D-8 标定 |
| E1 | 三层关联覆盖率 ≥95% | 阶段 C 报道 correlate 99.13%；E1 正式报告是否单独成包需核对归档 |
| E2 | SF100 默认布局基线，CV<5% | **PASS**（跨云，CV 1.99%，3205 s） |
| E3 | 单旋钮矩阵 | 未按合同「每档 5 次」做满；网格搜索代替了部分消融 |
| E4 | skipped rows / bytes / GET / wall-clock 相关性 | 未单独成实验；E8 已显示 skipped/预测与端到端会分叉（Q18） |
| E5 | L0/L1 校准，top-3 或 regret≤5% | **M2 压缩负载 PASS**（regret 0%）；完整 22 条上 L1 低估了无谓词扫描代价 |
| E6 | PTO 四参数联合 | 先全局网格、后按表网格；收益来自 sort 而非联合 TFS |
| E7 | 建议包 + Code Diff 闭环 | 链路可重复到「推荐 JSON → write_layout → 复测」；面向应用仓库的最小 PR 未作为交付重点 |
| **E8** | SF100 门禁 ≥10% 且全部 guardrail | **FAIL**：端到端 −37% 且 CV 过，Q18/Q22 单查询回归 |
| E9–E11 | 扩展动作 / 成本分支 / vectored 敏感性 | 未做 |
| **E12** | 同区 m5d.4xlarge 复核 | **预留，未做** |

L2 残差 surrogate 未训练：M2 压缩负载已经直接 canary，没有走到「解析模型不够再上学习残差」那一步。

---

## 8. 必须保留的负结果与限制

合同和项目文档都禁止只记正面结果。本阶段已经证伪或需要如实写出的有：

1. **全局放大文件在跨表负载上有害。** 合并 lineitem 会饿死维表并行度；按表动作不是优化，是正确性。
2. **端到端变快 ≠ 合同过门禁。** −33% / −37% 都过了 ≥10%，Q18/Q22 卡住 E8。
3. **派生变换分区（`col:year` / `col:month`）在 bare Parquet 上完全不裁剪。** Spark 按分区列名匹配谓词，无法从 `l_shipdate >= …` 推出 `l_shipdate_year = 1995`；只增加文件数，比不分区更差。此前只记录为「预测负收益」，原因没写下来。需 Iceberg hidden partitioning 才能生效，已从动作空间移除。过小 RG 在高 RTT 下增加请求，同样为负。
4. **RTT 从 25 ms 扫到 228 ms，top-1 不变。** 计划中的「推荐随 regime 翻转」没有发生。
5. **256 MiB RG 在本 Reader 上不可读**（vectored 300 s 写死超时），不能当搜索点。
6. **排序不是普适加速器。** 基线已按主键有序时（ClickBench），再排会把高选择率谓词收到 1 个 task。
7. **L1 看不见无谓词全表扫描在排序后的代价**（Q18），也看不见基线已经具备的聚簇（ClickBench）。闸门 A/C 是补丁，不是模型已经理解了这两件事。
7b. **`execution_residual` 是标定残差，不是代价模型。** 它等于 `实测中位数 − 预测 I/O`，把解压解码、聚合、shuffle、调度、JVM/GC 以及 I/O 项自身的误差一并吸收，再按扫描行数**线性**外推；线性外推对 shuffle/join 阶段和每查询固定开销都是错的。旧名「CPU 残差」有歧义，已改名以使这份粗糙可见。
7c. **运行时候选生成依赖 eventlog 里能看到 pushdown。** 引擎没下推的谓词（UDF、复杂表达式、Spark 计划渲染改版）就不会进候选；解析失败时表现为该表没有候选键，是可见的失败而不是错误答案。ClickBench 的 `SearchPhrase` 等值权重最高（1924 s）却因 `column_stats` 没采集其 NDV 而无法判定分区资格——统计覆盖是当前短板。
8. **所有门禁数字都是跨云 228 ms RTT。** 未完成 E12 前，不得对外写成同区 EC2。
9. **ClickBench S3 对比 n=2，基线两轮 1826 s / 2721 s，方差大**；CounterID 组 Q37–43 的变慢是干净信号，其余全表扫描约 +20% 尚未完全归因。后续 EventDate 布局测得 −51.3%，但同样是 n=2、且基线方差未收敛，只能作方向性结论。
10. **两个几何建模错误是在换成运行时枚举之后才暴露的**（`partitionBy` 的 `F × P`、排序键 NDV 对文件数的上界）。手写网格只含测过的点，模型错了也无从发现——这本身说明「用手写网格验证 advisor」是循环论证。
11. **L0 可读性闸门存在盲区**：只检查请求的 RG 大小，不检查排序间接压出的 RG 大小。
12. **闸门 A 的第一版被实测证伪。** 绝对阈值 `rg_span < 0.5` 的标定依据是手抄的 ClickBench `EventDate` = 0.5232，实测为 0.358——旧闸门会否决 E8 实测 −37% 之外唯一的正结果（−51.3%）。已改为相对可达跨度的余量比，但这说明**只要还有统计量是抄的，闸门就可能是在拟合抄错的数**。
13. **重构后 GET 自洽误差从 2.6% 升到 6.2%。** 旧的好数字部分来自两个方向相反的错误（漏数 19 个扫描 / 把行组数高估 400 vs 300）。现在扫描数是实测的，剩下的 6.2% 是一个具名缺口：Spark 在同一查询内复用部分文件 open，L1 按扫描节点逐个计费。字节误差同期从 2.9% 降到 0.5%。
14. **查询号在 eventlog 里不存在。** 新跑的日志靠 `run_benchmark.py` 打的 `track2:q<N>` 标记；归档日志靠**位置对齐**（查询串行，第 k 个带扫描的 execution 即第 k 条查询），已用手写目录逐条复核，但 AQE 或缓存改变"每条查询恰好一个带扫描 execution"就会错位，这不是通用方案。
15. **`rg_span` 与列字节占比来自抽样 footer**（默认每表 8 个文件）。lineitem 的三簇行组分布说明抽样噪声真实存在，加大 `--sample-files` 只是把成本从建模挪到 S3 往返。

---

## 9. 现在手里有什么、下一步是什么

**可演示的资产**

- 冻结合同 + 能力矩阵 + canonical 动作词表；
- 三层采集与关联、E2 可复现基线；
- 不物化的 L0/L1 Advisor（穷举 + 迭代分解 + RTT sweep + 按表网格）；
- TPC-H 按表日期排序的跨云复测（快、稳、单查询未过）；
- ClickBench 适配与「已有序则勿排」的硬闸门。

**若继续推进，优先级建议**

1. **Q18/Q22**：要么在 L1 补上无谓词扫描代价，使顾问不再选伤害它们的 sort；要么接受 E8 止损，把 Track 2 写成 discussion（架构、预测误差、负结果仍然有用）。
2. **闸门 A/C/D 的消融**：`cluster-headroom-min`、`prune-parallelism-floor`、`max-partitions` / `min-partition-bytes` 尚未用网格扫过。闸门 A 刚换过形式，2.0 是起始切分不是拟合值，优先级最高。
3. **E12**：同区 `m5d.4xlarge` 重跑 E2/E8，才能把对外数字对齐合同 D-8。
4. 合同清单里未做的 E3/E4/E9–E11，只在 E8 路线还开着时才值得排期。

**对论文主张已经站得住的句子**

- 湖仓布局 What-if 不能调用引擎优化器，必须模拟 Reader 请求计划；我们在跨云 regime 上把基线 GET/字节误差做到 10% 以内。
- 对象存储上代价经常是 `请求数 × RTT`，不是字节；文件数同时是并行度和 HEAD/footer 开销。
- 四参数必须按表决策；全局 TFS 会过拟合最大表。
- Writer × Reader 能力必须探针，不能靠文档；页索引在三个 writer 上行为不一致。
- 排序收益取决于基线聚簇度；已有序的生产数据上重排可以是负优化。
