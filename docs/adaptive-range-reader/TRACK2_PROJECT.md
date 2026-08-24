# Track 2 —— 非侵入式 Writer 布局顾问

> **定位**：本文件是 Track 2 路径的专用工作文档与变更日志。
> 自 2026-08-04 起，所有 Track 2 相关的代码、数据、实验、判断、失败结果和范围变更都记录在这里。
>
> **文档优先级**：
> 1. `PROJECT3.md` 是当前项目总路线的权威来源；
> 2. 本文件是 Track 2 的执行与交接权威来源；
> 3. `PROJECT2.md` 仅作为历史证据库与资产索引，不再更新。

---

## 1. 已确认的工作前提

### 1.1 项目阶段已经改变

- 不再把「在四条 S3A 风格策略之间做选择」作为主研究方向；四策略和旧决策树只保留为 baseline。
- 当前总项目是面向真实对象存储的 **Intelligent SDK**，Reader 侧围绕缓存、同步转异步、预取、IO 合并和长尾消除五个维度优化。
- Track 2 负责 Writer 侧：改变数据的物理组织，使后续读取天然更高效；它与 Reader 侧优化互补，但必须能单独归因和验证。

### 1.2 Track 2 的目标

从 SDK 的 offset 级访问 trace 中挖掘布局问题，把量化结论映射回应用代码中的 Writer 配置或 schema 定义位置，并产出：

1. 人可读、可审计的优化建议；
2. 机器可读的分析结果；
3. 可直接审阅或应用的最小 Code Diff，条件成熟时可形成建议性 PR；
4. 按建议重写数据后的真实云端端到端验证结果。

Track 2 的价值主张不是“AI 猜一个更好的布局”，而是：

> trace 量化分析决定改什么、改成什么；AI 只负责定位写入代码并把结论翻译成最小改动。

### 1.3 不可突破的边界

- **非侵入式**：不修改 Parquet、Lance 等文件格式，不做逻辑地址到物理地址的重映射，不引入文件可读性依赖的外置映射表。
- **建议式**：Diff / PR 必须经过人工审核，不自动合并。
- **可验证**：每条建议必须给出 trace 证据、预期作用机制、涉及的 Writer 参数和验证方法；无法验证的建议不输出。
- **不以 AI 代替性能判断**：AI 用于代码理解、写入点定位和 Diff 生成，不负责凭直觉判断性能。
- **不以本地模拟器形成对外结论**：模拟器只能粗筛；最终性能数字必须来自真实 AWS S3 或腾讯云 COS。
- **不以 cache hit rate 或 remote bytes 单独作为目标**：主指标是 wall-clock；GET、remote bytes、读放大等是必须同时报告的代价指标。
- **不重建已有资产**：优先复用现有 trace、采集链路、热力图工具、真实云接线和结果归档规范。

---

## 2. Track 2 工作流

1. **采集**：复用 SDK / pyarrow / syscall 链路获得 offset 级 trace。
2. **挖掘**：
   - 列、块、row group 的共访问矩阵与共现图；
   - 被 row-group / page 边界切碎的访问比例；
   - RTT 主导的小对象及潜在合并机会；
   - 从未访问或极少访问的冷字段；
   - 与 Writer 参数相关的其他可量化布局问题。
3. **定位**：在应用源码中找到 Writer 配置、schema、分区键和排序键的定义位置。
4. **建议与改动**：
   - row-group / page 大小；
   - 列顺序与共置；
   - 分区键；
   - 排序键；
   - 有充分证据时的小对象合并或冷热分层建议。
5. **验证**：按建议重写数据，使用相同逻辑负载在相同真实云环境中复测，对比 wall-clock、延迟分位数和代价指标。

### 2.1 因果归因要求

- 基线与优化版本只改变待验证的布局变量，Reader 配置、查询、云环境和客户端规格保持一致。
- “同一条 trace”原则上指同一逻辑访问意图；布局变化后物理 offset 会改变，不能机械重放旧物理 offset 并声称是端到端验证。
- 每个配置至少独立运行 5 次，报告 median 和分布，不取最快一轮。
- 必须保存原始结果、环境元数据、数据生成参数和 Writer 代码版本。

---

## 3. 8 月 MVP 与止损门槛

### 3.1 最小可交付

1. 一个 trace 分析器，输出：
   - 共访问矩阵；
   - 切碎率；
   - 冷字段列表；
   - 建议参数；
   - JSON 结果与人读报告。
2. 在一个负载上跑通“采集 → 分析 → 定位 → Diff → 重写 → 云上验证”的完整闭环。
3. 首选负载为 **TPC-H Parquet**：布局旋钮清晰，收益最容易归因。
4. 至少演示一次针对数据生成 / Writer 脚本的真实 Code Diff。

### 3.2 成功与止损判据

- 主要门槛：优化布局相对基线的真实云端端到端 **wall-clock 改善 ≥10%**。
- 同时报告：每次读 P50/P95/P99/P999、GET 数、remote bytes、读放大、峰值内存和 CPU。
- 若 TPC-H MVP 达不到可观改善，Track 2 在论文中降级为 discussion，不继续扩张为独立贡献点，把资源转回 Reader 五维实现。

---

## 4. 数据与实验前提

- 主实验环境：同 region 客户端访问 AWS S3；腾讯云 COS 作为可选双云对照。
- 对外结论的数据规模目标：至少 100GB；TPC-H 目标为 SF100。
- 小规模数据可用于开发、正确性测试和参数粗筛，但不能替代真实云大规模结果。
- 工作集 / 预算比 `ρ` 是关键工况变量，实验与报告中必须显式记录。
- 云端重复实验应归档客户端实例规格、region、网络条件、JDK / 依赖版本、对象版本和运行时间。

---

## 5. 可复用资产

优先复用而不是重建：

- `logging_fs.py`：pyarrow 路径的 offset 级读取采集；
- `strace_to_trace.py`：Rust / Lance 等原生 IO 的 syscall trace 转换；
- `lakehouse_access_heatmap.py`：统一 trace 分析与可视化入口；
- `traces/`：已有负载 trace 及其结构保持方法；
- `tools/build_mixed_trace.py`：保结构采样与源前缀命名空间方法；
- TPC-H、Lance、ML、multimodal 的现有数据生成与工作负载驱动链路；
- AWS S3 真实 backend、对象预置、版本隔离和实验结果归档规范。

历史模拟器、四策略执行器和旧决策树可作为对照或粗筛工具，但不作为 Track 2 的性能结论来源。

---

## 6. 当前实施顺序

1. 盘点当前工作区中 Track 2 可直接复用的脚本、trace schema、TPC-H Writer 与查询入口。
2. 为 TPC-H 明确“逻辑访问 → Parquet 列 / row group / page → Writer 参数”的映射方法。
3. 定义分析器的输入 schema、JSON 输出 schema、指标公式和建议证据格式。
4. 先实现可验证的最小分析闭环，再接入源码定位与 Diff 生成。
5. 用小规模数据做正确性测试，然后迁移到真实云 SF100 做最终验证。

---

## 7. 列式存储布局的研究框架

布局按四层分析，避免把不同机制混成一个“调大块”问题：

| 层级 | 主要布局变量 | 主要影响 |
| --- | --- | --- |
| 表 / 数据集 | 分区、bucket、聚簇、全局排序、文件数量与大小 | 文件裁剪、元数据量、对象 GET 数、并行度、小文件开销 |
| 文件 | row group / ORC stripe 大小、row group 顺序、列块顺序 | 顺序 IO 粒度、投影列 Range 的距离、并行度、跳过粒度 |
| 行组 / Stripe | 行排序、列 chunk/stream 的组织、统计量、Bloom filter | min/max 与谓词裁剪效果、共访问列的物理邻近性 |
| Page / 编码 | page 大小、page index、字典、编码和压缩 | 页级跳过、随机点查放大、remote bytes、解压 CPU |

### 7.1 优先研究的布局旋钮

1. **分区与聚簇 / 排序**：让常用过滤条件集中到少量文件、row group 和 page，提高统计量与索引的裁剪能力。
2. **文件大小与小文件合并**：平衡对象打开 / GET / footer 成本与任务并行度；避免大量 RTT 主导的小对象。
3. **row-group / stripe 大小**：大组利于大块顺序 IO、降低元数据和请求数；小组提供更细的裁剪粒度、降低选择性查询的浪费。
4. **行顺序**：即使 schema 和列顺序不变，按高频谓词列排序也会收紧 min/max 范围，并改善 page index、压缩和局部性。
5. **page 大小与页级索引**：小页利于点查和高选择性扫描，大页降低 header / 解码开销；只有 Writer 生成且 Reader 使用 page index 时，页级跳过收益才成立。
6. **列顺序 / 共访问列邻近放置**：不改变列裁剪本身，但会改变多个投影列的 Range 距离和合并机会；实际收益取决于 Reader 是否合并邻近 Range，必须实测，不能先验假定。
7. **统计量与 Bloom filter**：它们严格说是辅助结构，但直接决定能否在文件、row group、stripe 或 page 层跳过数据。
8. **编码、字典与压缩**：主要改变物理字节数和 CPU；数据排序还会改变编码与压缩效果，因此需要作为布局的联动变量记录。
9. **冷热列拆分 / 垂直分区**：可能减少热路径字节，但会改变表组织和读取方式，侵入性高于普通 Writer 参数，MVP 不优先。

### 7.2 对对象存储访存的四类直接结果

任何布局建议都必须最终落到以下至少一个可测结果：

- **少打开对象**：文件 / 分区裁剪更有效；
- **少发 GET**：减少小文件、使所需 Range 可合并；
- **少读远端字节**：row-group / stripe / page 跳过更精确，压缩更好；
- **更好的时序与局部性**：请求更连续、并行度更合理、缓存复用更高。

### 7.3 TPC-H MVP 的首轮优先级

首轮按“收益可归因性”排序：

1. 分区 / 排序键；
2. row-group 大小；
3. 文件大小与 compaction；
4. page 大小及 page index；
5. 列顺序与共访问列邻近性；
6. Bloom filter、字典和压缩联动。

其中“列顺序”不能只凭共访问矩阵给建议：必须先把 trace offset 映射到 Parquet column chunk，并确认所用 Reader 的 Range
合并行为。否则只能证明列被一起访问，不能证明把它们放近会减少 GET 或 wall-clock。

### 7.4 SIGMOD'17 宽表列布局论文的借鉴与边界

参考论文：*Wide Table Layout Optimization based on Column Ordering and Duplication*。

**可直接借鉴的方法骨架**：

- 将查询表示为“访问列集合 + 查询频率 / 权重”，将列大小纳入布局成本；
- 用全局优化而非简单地把热门列放在前面，协调不同查询之间冲突的列邻接需求；
- 复用 SCOA 的模拟退火搜索框架：以交换两列作为邻域动作，并只重算受影响查询的增量成本；
- 维护滑动工作负载窗口；当当前布局成本相对新负载明显恶化时，生成新的 Writer 建议；
- 新布局优先用于后续写入数据，不强制重写全部历史数据；
- 必须使用目标部署环境校准成本函数，不能用错误的代理指标替代 wall-clock。

**迁移到 S3 时必须替换的部分**：

- 原论文优化 HDD seek 距离 `f(distance)`；S3 没有可见磁头 seek，不能沿用该目标。
- S3 目标应变为 Reader 实际请求计划下的合并 Range 成本：
  `Σ(GET_RTT + fetched_bytes / BW)`，同时受最大单次抓取和浪费字节上限约束。
- 列物理距离只有在 Reader 会合并邻近 Range 时才有价值；若 Reader 始终逐 column chunk 单独 GET，单纯调整列顺序可能无收益。
- 成本估计必须使用每个 row group 的压缩后 column-chunk 大小分布，不能只用未压缩列宽或全表平均值。

**当前范围决定**：

- 暂不把“列复制”纳入 MVP。论文的列复制需要增加 replica 列，并通过查询重写 / Column Redirector 选择副本；
  这超出当前“只改 Writer、应用人工审核、Reader 无额外语义依赖”的低侵入边界。
- TPC-H 继续用于 row-group、排序、文件大小和索引建议的 MVP，但不作为列顺序优化的主要验证负载。
  论文场景是 1187 列生产表，而 TPC-H 最大事实表只有 16 列；列顺序的收益空间不可类比。
- 列顺序模块应另选真实宽表，或构造保留真实访问偏斜与列大小分布的宽表工作负载。

**评测教训**：

- 同时报告 Reader 层和端到端 wall-clock；I/O 层的大幅改善可能被调度、计算、shuffle 和 GC 稀释。
- 相对提升不能替代绝对结果。论文中更大的 row group 会降低列重排的相对收益，但绝对读取时间反而更低；
  因此必须比较联合配置的绝对 wall-clock，避免在较差 baseline 上得到漂亮百分比。
- baseline 至少包括：原 schema 顺序、热门列优先、基于共访问图的邻接布局、SCOA / 模拟退火布局。

### 7.5 优化器、约束与成本函数的定位

列布局搜索算法是可替换实现，不应成为 Track 2 的核心假设。模拟退火、图排序、贝叶斯优化、学习型 surrogate
或其他组合优化器都应在同一份候选空间、硬约束和成本函数下比较。

**不可由优化器绕过的硬约束**：

- 文件格式、schema 兼容性与现有 Reader 的字段解析语义；
- Writer 实际可控制的参数与不允许改变的应用语义；
- 禁止默认列复制、查询重写和外置物理地址映射；
- 单文件 / 单 Range 上限、内存与额外远端字节预算；
- 数据重写预算、存储膨胀上限、历史数据是否允许重写；
- workload 覆盖率、layout drift 阈值和建议的最小收益门槛。

**S3 版成本函数的原则**：

- 离线阶段以实际 Reader 的 Range 合并规则，将 query × row-group 的 column chunk 区间变成请求计划；
- 代理成本以 RTT、传输时间、并发调度、GET 数、浪费字节、写入 / 重写成本和稳定性惩罚组成；
- 最终优化目标与验收指标仍是真实云上的端到端 wall-clock，代理成本只用于候选筛选和排序；
- 对尾延迟敏感的负载需同时约束 P95/P99，不能只优化加权平均值。

**大模型的适用边界**：

- 适合：解析 Writer 源码和 schema、提取可调参数、把量化结论转为最小 Code Diff、解释建议与生成验证计划；
- 可辅助：根据历史实验提出候选布局或缩小搜索空间，但候选必须经过约束校验和成本评估；
- 不适合：直接给数百 / 数千列生成最终排列，或充当性能成本函数和最终验收者。其输出不可校准、不可保证约束满足，且工作负载 / schema 可能含敏感信息，不应默认发送到外部 API。

建议采用三层闭环：`约束校验 → 代理成本筛选 / 搜索 → 真实云复测`。当真实结果持续积累后，再训练轻量 surrogate
预测候选布局的 wall-clock，并用主动测量校正；是否使用模拟退火或其他搜索器不影响这条主线。

### 7.6 PTO（SIGMOD 2026）的借鉴与路线修正

参考论文：*PTO: A Workload-driven Predictive Table Optimizer for Lakehouse Systems*。

PTO 已经联合优化 Iceberg / Parquet 的四个参数：`<partitioning column, target file size, row-group size,
multidimensional sort scheme>`。它从 Presto 查询日志提取 scan fragment，按过滤列频率、scan selectivity 和
sort-cluster size 缩小候选空间；对少量 sample-table 布局执行真实改写，用跳过行数标注约 3%–4% 的候选，再训练
per-table GBT 预测其余布局。

**直接借鉴**：

- 参数必须联合发现：partition、TFS、RGS、sort 之间存在真实耦合，单旋钮建议只能作为消融；
- 采用“先缩候选空间、再用少量真实布局标注预测器”的两阶段结构；
- 从执行计划中提取 scan fragment，而不是只依赖 SQL 文本；
- 保留“无分区 / bin-packing”等保守候选，避免强迫每张表采用高级布局；
- 依据 NDV、预期分区数、过滤频率和选择性设置候选 guardrail；
- 训练标签成本远高于模型训练成本，因此优化重点应是标签预算、代表性查询选择和并行化，而不是复杂模型；
- 候选空间质量（top-k、TFS/RGS 档位）比模型超参数更敏感；
- 用 distinct predicate-column combinations 选择代表性 scan fragments，可在论文实验中将标注时间约减半；
- 布局需按 table / partition 独立决策，并在数据分布或 workload 漂移后重新采样和校准。

**必须修正**：

- PTO 以 skipped rows 为训练标签。Track 2 不得照搬：跳过行数不能反映投影列宽、压缩、footer / metadata GET、
  Range 合并、并发和真实云长尾；它应作为解释特征，而非最终目标。
- PTO 的 sample-table 会按采样率同比缩小 TFS/RGS；论文自身观察到小样本可能无法形成足够文件 / row group，
  从而把候选推向错误参数。我们的样本必须满足结构保真门槛，而非仅按比例缩放。
- PTO 未把最终 compaction / rewrite 时间纳入布局评分。Track 2 必须报告预期回收周期：
  `rewrite_cost / saved_query_cost_per_period`，并设置最小收益门槛。
- 平均 workload latency 允许少数查询明显回归。Track 2 除加权总 wall-clock 外，还需设置单查询退化和
  P95/P99 guardrail，输出 Pareto 结果而非只有一个全局最优。
- PTO 在 MinIO S3 上评测；我们的对外结论继续要求真实 AWS S3 / COS。

**对 Track 2 的路线影响**：

- PTO 应成为四参数联合布局推荐的直接 baseline；仅复现其四个旋钮不足以构成新贡献。
- 观测面改为双层：
  1. 查询语义层：table、predicate、filter/project columns、rows in/out、files/row groups pruned；
  2. SDK 物理层：真实 offset/length、GET、remote bytes、Range 合并、延迟分位数。
- 新的差异化目标是：同时预测“能跳过多少数据”和“剩余数据在对象存储上实际花多少钱”，并扩展 PTO 未覆盖的
  列物理顺序、page/index 与 Reader Range 合并联动。
- 首个分析器输入需要从纯 offset trace 升级为 `scan fragments + Parquet footer map + SDK trace`；缺少查询谓词语义时，
  不能可靠地产生 partition / sort 建议。
- 建议学习多指标成本向量
  `<wall-clock, GETs, remote bytes, files touched, row groups touched, rewrite cost>`，以真实云 wall-clock 为主目标，
  skipped rows 作为特征和解释指标。

### 7.7 双观测面的可采集性与优化目标

#### SDK-centered，而非 SDK-only

三类信息的可见性不同：

| 信息 | 通用对象存储 SDK 能否直接获得 | 建议采集位置 |
| --- | --- | --- |
| Range offset/length、对象 key/version/size、GET、bytes、时延、重试、并发 | 能 | SDK / Range Reader 热路径 |
| SQL predicate、filter/project columns、scan rowsIn/out、query plan | 不能可靠获得 | Spark/Trino/Presto/Velox 的 plan/event adapter |
| Parquet row group、column chunk、page/index、min/max、Bloom 配置 | SDK 只能看到原始字节，不能可靠理解 | 格式插件或离线 Advisor 读取 footer；必要时由 Parquet Reader 主动上报 |

因此系统应由三个 collector 组成：

1. `SemanticCollector`：输出 query/scan-fragment 语义；
2. `FormatMetadataCollector`：输出 object-version → row-group/column-chunk/page 映射；
3. `SdkIoCollector`：输出真实物理请求与延迟。

三者通过 `query/span id + object key + version/snapshot + time window` 关联。严格的应用代码可以保持不变，但需要安装
engine/format adapter；若某类负载没有 SQL 查询计划（如 Lance/ML），则只提供物理优化和应用显式传入的逻辑 span，
不得声称已恢复完整谓词语义。

不建议让通用 SDK 在热路径主动重复读取并解析 Parquet footer：这会引入格式依赖、额外 GET、加密/分片 footer 处理和
Reader 缓存不一致问题。优先由 Advisor 离线读取一次并缓存，或复用 Reader 已解析的 metadata。

#### 创新定位

系统由两部分组成：

- `Adaptive SDK Runtime`：采集物理 IO 并执行 Reader 侧自适应；
- `Writer Advisor Control Plane`：联合语义、格式和物理 telemetry，生成 Writer 参数与可审核 Code Diff。

相对 PTO，扩展动作空间包括列物理顺序、page size、page index、Bloom filter、编码/压缩，以及它们与 Reader Range
合并的联动。但“动作更多”本身不是贡献，必须通过 `PTO action space` 与 `extended action space` 的增量消融证明额外自由度
带来稳定 wall-clock 收益。

#### 两级 wall-clock 目标

不能在看到实验结果后任意更换主目标。预先冻结：

- **候选排序目标**：scan-stage wall-clock / IO critical-path wall-clock 的重复运行 median；它与 Writer 布局的因果关系更直接；
- **最终验收目标**：完整 workload 的端到端 wall-clock median；
- **硬约束**：单查询回归、P95/P99、GET/bytes、峰值内存和 rewrite payback；
- **解释指标**：skipped rows、files/row groups touched、page/index pruning。

若实验表明完整 query wall-clock 噪声过大，只允许调整训练代理与采样方法，不可事后把更有利的 bytes、GET 或 skipped rows
改成主结论。不同部署目标（延迟、吞吐、云成本、尾延迟）应在实验前声明为独立 profile，并报告 Pareto frontier。

#### 前置可行性实验

1. **观测覆盖率**：在 TPC-H 上测量 semantic、metadata、physical 三层的关联率，目标是绝大多数 GET 可映射到
   query → file → row group → column chunk/page；
2. **目标相关性**：对一组可控布局比较 skipped rows、remote bytes、GET、scan wall-clock 和 end-to-end wall-clock 的
   rank correlation，决定哪些指标可作为代理；
3. **动作空间消融**：Default → PTO 四参数 → +page/index/Bloom → +column order → full joint；
4. **隔离布局收益**：Writer 候选比较时冻结 Reader 策略，并分别报告 cold-cache 与 steady-state，避免 Adaptive SDK
   缓存变化掩盖布局本身的因果效果。

### 7.8 DB2 Design Advisor 的架构借鉴

参考论文：*DB2 Design Advisor: Integrated Automatic Physical Database Design*（VLDB 2004）。

这篇论文的目标、优化对象和部署环境都与 Track 2 不同：它面向 DB2 内部的索引、MQT、分区与 MDC 设计，而 Track 2
面向开放湖仓 Writer 布局和对象存储 IO。因此只借鉴其 Advisor 的工程实现方式，不把论文的目标函数、特征依赖分类或
搜索策略迁移为 Track 2 的标准架构。

最值得借鉴的是清晰的控制面边界：数据库优化器提供 `RECOMMEND` 与 `EVALUATE` 两类 What-if 能力，外部 Advisor
负责组织工作负载、调用候选模块、管理评估预算并输出建议。论文中的混合依赖搜索可作为未来减少搜索成本的优化，但不是
MVP 的必要组成。

#### 直接借鉴

- **候选生成与候选评估分离**：每类布局动作独立生成少量有希望的候选，所有候选通过统一评估接口比较；搜索器和候选生成器
  均可替换。
- **组件接口先于组件依赖**：第一阶段要求各候选模块使用统一输入、输出和评估协议，不要求先建立完整依赖图。待独立组件闭环
  跑通并积累实验数据后，再用依赖图、联合组件或顺序迭代减少搜索成本。
- **支持任意动作子集并固定其他设计**：实验可启用 PTO 四参数、page/index/Bloom、列顺序或 Reader Range merge 的任意
  组合，同时把其余动作冻结。这既是产品能力，也是消融实验所需的基础设施。
- **共享预算并允许回收**：DB2 在组件间分配磁盘预算，未使用预算传给后续组件。Track 2 应把它推广为多维预算：
  Advisor 时间、真实改写次数、rewrite bytes、额外存储、训练样本数和云实验费用。
- **工作负载来自真实运行轨迹并带频率**：对应我们的 SemanticCollector、FormatMetadataCollector 与 SdkIoCollector；
  workload repository 必须保存 query/scan-fragment、频率、时间窗口、对象版本和物理 IO 证据。
- **先压缩、后全量复核**：可以按总 wall-clock 贡献、谓词形态和 IO 模式选择代表性片段来搜索，但最终候选必须在原始完整
  workload 上重新评估，并检查被压缩掉的低频查询是否回归。
- **时间预算与边际收益停止**：除固定候选数外，允许在用户时间预算耗尽或连续若干轮没有显著 wall-clock 改善时停止，
  同时保留当前 best-so-far。
- **建议式交付而非自动破坏**：DB2 因 workload 可能遗漏季度级低频查询，不直接建议删除未使用结构。Track 2 同样不得因
  当前窗口未观察到访问就自动删除列、旧布局或历史文件；只能报告“当前窗口未使用”，并要求更长观测或人工确认。

#### Track 2 的对应控制面

建议把 Writer Advisor 拆成以下稳定接口：

1. `WorkloadRepository`：保存三层 telemetry、权重、观测窗口与覆盖率；
2. `CandidateGenerator`：按动作族生成候选及其前置条件、冲突和作用范围；
3. `ConstraintChecker`：执行兼容性、资源、回归和 rewrite payback 硬约束；
4. `WhatIfEvaluator`：在候选未全面部署前估计真实 Reader 与对象存储成本；
5. `CandidateSelector`：在多维预算下选择 Pareto 候选和 best-so-far；
6. `RecommendationEmitter`：输出证据、预测、最小 Writer Code Diff、迁移与回滚方案；
7. `CanaryValidator`：在样本分区和真实云上复测，并将预测误差反馈给成本模型。

`DependencyPlanner` 暂列为可选扩展，不阻塞第一版 Advisor。只有实验表明跨组件组合数量或相互影响已经成为主要瓶颈时，
才引入显式依赖关系和联合搜索。

候选应采用统一中间表示，而不是由每个动作模块输出互不兼容的参数：

```text
LayoutCandidate = {
  scope, actions[], prerequisites[], conflicts[],
  evidence[], predicted_metrics, uncertainty,
  rewrite_cost, storage_delta, patch, rollout, rollback
}
```

PTO 可实现为 partition/TFS/RGS/sort 的一个候选组件；page/index/Bloom、column order、encoding/compression
和 Reader Range merge 则作为新增组件。第一阶段允许这些模块独立提出并评估候选；是否需要跨组件联合搜索，由后续实验
决定。任何依赖关系都必须由 Track 2 自身实验校准，不能把论文中 DB2 特征的强弱分类直接照搬。

#### 必须自行补齐的 What-if 层

DB2 能让成熟优化器在不创建真实索引 / MQT 的情况下编译查询并估价；开放湖仓没有等价的统一“虚拟 Parquet 布局目录”。
因此 Track 2 需要多保真评估：

1. 静态可行性与依赖检查；
2. 基于 footer、scan fragment 和实际 Range 合并规则的解析型 IO 模拟；
3. 少量 sample/partition rewrite，用真实 Reader 标注并训练或校准 surrogate；
4. 对 top-k 做真实 AWS S3 / COS canary，最终在完整 workload 上验收。

低保真层只负责淘汰明显较差或不可行的候选，不得把估计值当成最终性能结论。论文报告其优化器估计改善与实测较接近，
但这是 DB2 自有成本模型在特定 1GB TPC-H 环境中的结果，不能外推为我们的成本模型准确性；Track 2 必须单独报告预测误差、
候选排序相关性和 top-k recall。

#### 推荐包的工程边界

最终输出不应只是一个“最佳参数 JSON”，而应是可审核的 recommendation package：

- 适用的 workload 窗口、数据快照和置信度；
- 每项动作的证据、预期 scan/end-to-end wall-clock、GET/bytes 与 rewrite payback；
- 受益查询与可能回归查询；
- 最小 Writer Code Diff，以及生成新布局的命令 / 配置；
- canary 范围、旧新布局共存方式、回滚步骤和建议有效期。

这一架构进一步限定大模型的位置：大模型可以协助生成候选解释、识别源码改动点并形成 patch，但候选是否进入推荐包，必须
由硬约束、多保真 What-if 成本与真实云验证共同决定。

### 7.9 ATUN-HL 的解析成本模型与字典编码

参考论文：*ATUN-HL: Auto Tuning of Hybrid Layouts using Workload and Data Characteristics*（2018）。

#### 术语与范围

论文中的 hybrid layout 指“数据先被横向切成 row group / stripe / blocklet，每个横向分片内部再按列存储”的混合行列
组织，不是为同一张表生成多种异构 Parquet 布局并让查询动态选择。ATUN-HL 最终为一张表选择一组 row-group size 与
dictionary size，目标和动作空间都比 PTO 与 Track 2 窄。

#### 可借鉴的成本分解

ATUN-HL 把模型输入显式分为四类，这个接口设计适合成为 Track 2 解析型 What-if 层的基础：

| 类别 | ATUN-HL 输入 | Track 2 对应输入 |
| --- | --- | --- |
| 系统常量 | HDFS chunk、磁盘/网络带宽、seek、本地读取概率 | GET RTT/价格、有效带宽、并发、Range merge、重试与长尾 |
| 数据统计 | 行数、平均值宽、NDV、是否排序 | footer 实际压缩大小、NDV/直方图、聚簇质量、编码与 page 统计 |
| 工作负载统计 | predicate、selectivity、常见 clause | scan fragment、频率、投影列、谓词类型、rows in/out、wall-clock |
| 布局变量 | RG size、metadata size、dictionary size | TFS/RGS/page、sort、column order、index/Bloom、encoding/compression |

其模型依次估计：

1. row group 总数、每组行数和总 metadata；
2. 根据布局模式估计读取的 row group 数：
   - 排序数据 + min/max：约为 `selectivity × RG_count + 边界项`；
   - 均匀无序数据 + min/max：近似读取全部 row group；
   - 无序数据 + dictionary：根据一个 RG 中出现至少一个命中值的概率估计；
3. local dictionary 的期望 NDV、dictionary bytes、编码 ID 所需 bit width 和编码后列大小；
4. 将读取 RG、metadata、chunk、seek、磁盘与网络代价组合为 workload cost；
5. 从查询日志提取、合并和归并 predicate，使用高频 clause 与数据 sample 选择 RG 和 dictionary size。

最值得借鉴的不是论文的闭式求导，而是把“为什么这个候选更好”拆成可单独验证的中间量：
`pruned RGs → fetched bytes → request pattern → estimated time`。Track 2 应对每一层分别报告预测误差，避免只有一个无法解释的
wall-clock 预测值。

#### 放入多保真 What-if 的位置

ATUN-HL 式模型适合作为第二级的低成本解析估价器：

```text
静态可行性检查
  → 解析型布局/IO模型（ATUN-HL 思路）
  → sample rewrite + learned residual / surrogate（PTO 思路）
  → 真实云 top-k canary
  → 完整 workload 验收
```

解析模型负责快速剪枝、解释作用机制并提供模型特征；样本实验学习“解析模型没有覆盖的残差”，例如解码 CPU、任务调度、
并发、缓存和云端长尾。这样不需要在解析模型与机器学习之间二选一。

#### 必须重写的假设

- **HDFS 成本不可复用**：`ChunkSize`、磁盘 seek 和数据本地性必须替换为 S3/COS 的对象、Range GET、RTT、有效带宽、
  合并阈值、并发 wave、重试与尾延迟。对象存储成本不是论文 Equation 10 的参数替换版，而是新的请求计划模型。
- **排序模式是否需要细化是待验证假设**：ATUN-HL 用 `SortedCol=true/false` 选择估价公式，这是便宜且可能已经足够有效的
  粗粒度观察。Track 2 将比较它与基于 footer/sample 真实统计选择成本分支的方法；只有后者能稳定改善候选排序和最终布局
  时才保留，否则沿用简单有序/无序分支。
- **均匀独立分布过强**：期望 NDV 和 dictionary skip 概率可作为先验，但必须用 sample 或 footer 校准偏斜、相关性和
  热值；否则会系统性误判 Zipf 或时间序列数据。
- **dictionary filtering 不是格式层必然能力**：它取决于 Writer 是否全程使用 dictionary、是否中途 fallback，以及目标
  Reader 是否真正执行 dictionary filtering。必须建立 Writer × Reader capability matrix，并用 trace 证明发生了跳过。
- **单一 dictionary size 不够**：Track 2 应把 dictionary enable/disable、阈值和 fallback 视为 per-column 决策；高 NDV
  长字符串列与低 NDV 维度列不应共享同一编码结论。
- **闭式最优不适合直接采用**：现代 Reader 的 page boundary、Range merge、对象并发和 dictionary fallback 会造成离散
  跳变。解析模型用于给出候选区间和排序，最终仍采用离散候选与真实测量。
- **成本必须加入 CPU 与内存**：dictionary 既可能降低远端字节，也会增加 dictionary page 读取、查找和解码开销；
  全扫描场景还可能更偏好较大 RG。优化目标继续使用 scan/end-to-end wall-clock 和既定 guardrail。

#### 可用于候选生成的布局模式

首轮不需要穷举所有组合，可以按谓词和数据组织生成少量可解释模式：

1. `sorted/clustered + min/max`：适合范围谓词和可聚簇列；
2. `unsorted + dictionary + equality/IN`：只有 Reader 能进行 dictionary filtering 时才成立；
3. `unsorted + min/max`：通常作为低裁剪能力 baseline，而非期望产生跳过；
4. `full scan / high selectivity`：优先减少 metadata、GET、解码和任务开销，通常倾向较大 RG；
5. `mixed workload`：根据频率与 wall-clock 权重综合以上模式，不能只以出现次数选 top-k predicate。

ATUN-HL 的 clause 抽取、相似/包含关系合并和 frequent-itemset ranking 可用于缩小候选，但 Track 2 仍须在完整 workload 上
复核，并保护低频关键查询与尾延迟。

#### 成本分支选择实验

为判断是否值得超越 ATUN-HL 的简单观察，固定候选空间、其他成本项和云环境，仅比较：

- **A：ATUN-HL binary**：按有序 / 无序选择对应 min/max 或 dictionary 公式；
- **B：empirical branch**：使用每个候选布局的 RG min/max overlap、实际 RG hit ratio、值分布偏斜与 clustering quality
  选择或插值成本分支。

评估分三层：

1. `ReadRowGroups`、remote bytes 与 GET 的预测误差；
2. 候选 wall-clock 排序的 Spearman/Kendall 相关性与 top-k recall；
3. 最终选择 regret：
   `(T_selected - T_best_measured) / T_best_measured`。

只有 B 在多个 workload / 数据分布上稳定降低 selection regret，且收益超过额外 profiling 与模型复杂度时，才作为 Track 2
的小贡献保留。若 B 只让中间量拟合更好、却不改变或改善最终布局选择，则采用 A，并把 B 记录为无显著收益结果。

#### 实验结论的使用边界

论文基于 Parquet 1.8.2、Spark/HDFS、SATA 磁盘和最多 64GB 的反规范化 TPC-H 宽表；默认 RG/dictionary 为
128MB/1MB，ATUN-HL 选择约 32MB/1MB，报告平均 1.2× 加速和约 85% 的潜在收益。其成本模型只覆盖 IO，并通过归一化
曲线形状验证最小值位置，不能作为现代 Parquet + S3 的绝对成本或默认参数依据。

对 Track 2 的直接影响是：仅增加 row-group 与 dictionary 联合调优不是新的贡献。可形成差异化的是
**对象存储请求计划感知 + 现代 page/index/Bloom/dictionary 能力检测 + 解析模型与实测残差学习结合 + 真实云
wall-clock 校准**。

### 7.10 Qd-tree 的层次化记录分块与 RL 搜索

参考论文：*Qd-tree: Learning Data Layouts for Big Data Analytics*（SIGMOD 2020）。

**当前范围决定**：Qd-tree 与 Track 2 的优化对象和接入方式差距较大，不纳入 Advisor 实现、MVP、baseline 或消融实验。
它只作为 related work，用于说明另一类“workload-guided record-to-block assignment + learned routing tree”方案，以及
该方案与低侵入 Parquet Writer 参数优化之间的边界。

#### 它实际优化什么

给定记录集合 `V`、查询 workload `W`、块跳过函数和最小块大小 `b`，Qd-tree 要把记录分成互不相交的数据块，使整个
workload 需要扫描的记录数最少（等价于最大化 skipped records）。它优化的是**记录到块的分配**，块内部采用 Parquet
列存、行存或其他布局与 Qd-tree 正交。

Qd-tree 是一棵二叉数据路由树：

- 根节点代表整张表的多维数据空间；
- 每个内部节点使用一个 predicate cut `p`，左子树接收满足 `p` 的记录，右子树接收满足 `¬p` 的记录；
- 每条记录从根开始确定性路由到一个叶子；
- 每个叶子对应一个逻辑数据块，其语义描述是路径上所有 cut 的合取；
- range predicate 用超矩形范围描述，categorical `=` / `IN` 用取值 bit mask 描述；
- 数据落盘时增加 block ID（BID）并按 BID 分区；大叶子可以对应多个物理段。

候选 cut 不是任意数值切点，而是从目标 workload 的 pushed-down predicates 中提取，例如 `<`、`>=`、`=`、`IN`。
论文还讨论了列间比较、LIKE/UDF、局部数据复制和第二棵完整副本树，但这些都不是基础方案的必要部分。

查询时有两种路径：

1. 依赖普通 min/max 等块级 metadata 做 best-effort pruning；
2. 用持久化 Qd-tree 判断查询与哪些叶子相交，再把查询改写为 `BID IN (...)`，显式裁剪块。

第二种路径能利用叶子的语义描述与 completeness，但需要额外路由元数据、BID 和查询改写。

#### Greedy 与 Woodblock RL

Greedy 方法从根开始，在每个可切叶子上选择当前 skipped-records 增益最大的合法 cut；两个子块都必须不小于 `b`。
它速度快，对满足 tree-submodularity 的简单合取范围负载还有近似保证，但可能错过“当前没有收益、后续组合才有收益”的
切分序列，特别是含 OR 的查询。

Woodblock 将建树表示为 MDP：

| RL 元素 | Qd-tree 定义 |
| --- | --- |
| state | 当前节点代表的数据子空间：range 与 categorical mask |
| action | 从 workload 提取的一个候选 predicate cut |
| transition | cut 产生左右两个子空间，并进入待处理队列 |
| episode | 从根开始构造一棵完整 Qd-tree |
| legality/stopping | 固定 sample 上两个子节点均达到约 `s × b`；无合法 cut 时成为叶子 |
| reward | 完整子树在 workload 上的 normalized skipped-record ratio |
| learner | 共享两层 512-unit ReLU 的 policy/value 网络，使用 PPO |

论文使用固定的 0.1%–1% 数据 sample 判断 cut 合法性并计算奖励。每完成一棵树才获得真实的全局跳过质量，再把对应的
normalized subtree reward 分配给中间 action。训练不断生成完整树，在时间预算耗尽时部署 best-so-far，因此具有 anytime
特征。RL 的神经网络不是主要开销，sample 路由和 reward 计算才是。

RL 的主要价值是长期 credit assignment。论文示例中，两个 `cpu` cut 单独都不能立即跳过查询，Greedy 因而先选择
`disk`；RL 能先接受局部零收益 cut，再组合出同时隔离两个查询区域的四块布局。需要注意，论文随机初始化的树也已显著优于
随机分块，因为 action space 本身已经由 workload predicate 强力约束；不能把全部收益归因于 PPO。

#### 可作为概念对照的方法

- **predicate-aligned action space**：从 scan fragment 的可下推谓词生成切分候选，比让 RL/LLM 任意发明阈值更可靠；
- **层次化、分支相关的布局表达**：不同数据子空间可以使用不同列继续切分，表达能力高于单一 partition key、全局
  lexicographic sort 或固定 Z-order；
- **轻量 sample 复用**：同一结构保真 sample 可用于快速判断块大小、估算剪枝和比较大量候选树；
- **硬约束内置于 action legality**：非法 cut 不进入搜索；Track 2 可扩展为最小/最大压缩字节、文件数、大小偏斜、
  rewrite budget 和小文件约束；
- **anytime best-so-far**：允许 Advisor 按时间/云预算逐渐改善候选，而不是必须等待 RL 完全收敛；
- **RL 必须与 Greedy 对照**：这是评价此类方法的一般实验原则；论文结果本身也表明 RL 并非在所有 workload 上都显著
  优于 Greedy。Track 2 当前不实现二者。

#### 与当前 Track 2 边界的冲突

完整 Qd-tree 方案不是纯 Writer 参数建议：

- 增加 BID 并按 BID 组织数据，会改变 schema / partition organization；
- 最大裁剪收益依赖持久化 routing tree 和 `BID IN (...)` 查询改写；
- completeness 是 Qd-tree 外部语义，标准 Parquet Reader 不会自动理解；
- 每个叶子直接写成文件可能引入小文件、文件大小偏斜和大量对象 GET；
- data overlap / replication 需要副本选择与去重，超出当前禁止默认列/记录复制和查询重写的 MVP 边界。

因此不把完整 routed Qd-tree 或 Writer-only/no-route 变体纳入 Track 2。即使移除 query router，仍需实现记录级树路由、
非均匀分块、叶子到文件的物理映射和块大小治理；这不是对现有 Writer 参数接口的自然扩展，适配成本与当前研究收益不匹配。

#### 与 Advisor 的关系

Qd-tree/Woodblock 不进入当前 Advisor 组件图。若未来研究范围扩展到 record-to-block assignment，它在概念上应属于
`CandidateGenerator` 而不是 `WhatIfEvaluator`：

```text
scan predicates + data sample
  → Greedy / Woodblock 生成层次化分块候选
  → Writer/format 约束检查
  → ATUN-HL 式解析 IO 模型
  → sample Parquet rewrite / surrogate
  → 真实云 top-k canary
```

论文 reward 仅优化 skipped records。若未来重新开启这条路线，reward 需要改成低保真对象存储成本的负值，并加入文件数、
大小偏斜、metadata、rewrite 和压缩惩罚；真实云 wall-clock 仍不能在每个 RL episode 中测量，只用于 top-k 校准和验收。

Qd-tree 的最小块约束按记录数定义。若未来迁移到对象存储，则必须改成物理量约束；不同列宽、压缩率和 dictionary
fallback 下，相同行数可能产生完全不同的对象大小，因此至少需要约束：

`compressed bytes per leaf/file、min/max file size、file-count、size CV、projected GETs`。

以上仅作为未来扩展需要解决的问题，不形成当前实现任务。

#### Related-work 对照点

论文中可用以下维度与 Track 2 对照：

| 维度 | Qd-tree | Track 2 |
| --- | --- | --- |
| 优化对象 | 记录到块的分配与查询路由树 | 标准 Parquet Writer/format 布局参数 |
| 主要动作 | predicate cut、BID、可选复制 | TFS/RGS/page、sort、column order、index/Bloom、encoding |
| Reader 依赖 | 完整收益需要 routing tree 与 `BID IN (...)` | 优先复用现有 Reader/format 能力 |
| 原始目标 | skipped records / scan ratio | scan 与端到端 wall-clock，GET/bytes 为代价指标 |
| 接入成本 | record router、额外 metadata、查询改写 | Writer Code Diff 与 SDK/format telemetry |

论文撰写时可将 Qd-tree 归入 workload-aware physical partitioning / learned data routing，指出其布局表达力很强，但依赖
record-level routing 与查询侧配合；Track 2 选择较低自由度、较低接入成本的标准格式布局调优，并进一步建模真实对象存储 IO。

---

## 8. 变更记录规范

后续每次 Track 2 变更至少记录：

- 日期与变更类型；
- 修改的文件 / 数据 / 实验配置；
- 做了什么以及为什么；
- 验证方法和结果；
- 失败、限制与尚未解决的问题；
- 对下一步和论文主张的影响。

禁止只记录正面结果。被证伪的假设、无收益的建议、口径错误和不可复现结果同样必须保留。

---

## 9. 更新日志

### 2026-08-24（其二）—— workload.py 拆解：几何、查询目录、动作空间全部改为实测；闸门 A 被实测证伪并改写

**问题**：上一条把排序/分区键改成了运行时推导，但 `workload.py` 里还剩三类**性质完全不同**的东西混在一个文件里，全是手抄常量：

| 内容 | 真实性质 | 手抄的后果 |
|---|---|---|
| `FILE_SIZE_GRID` / `RG_SIZE_GRID` | 动作空间 | 单位就写错了。成本模型在意的是**文件数**（并行度 + 每次 open 一个 RTT），"512 MB" 在 21 GiB 的 lineitem 上是 43 个文件、在 100 MB 的表上是 1 个文件，同一个网格在不同表上含义不同、在小表上无意义。L0 的下界 `n_files ≥ min(16, baseline)` 本来就是关于**计数**的约束，字节网格只能碰巧满足它然后被事后过滤掉 |
| `BASELINE_GEOMETRY` / `COLUMN_ORDER` / `COLUMN_SHARE` / `SYNTHETIC_STATS` | 数据集快照 | 描述的是"某个目录此刻长什么样"，不是 workload。写死就意味着候选布局写出来之后无法对**它**重新估价 |
| `QUERIES` | 查询目录 | 抄的是 SQL 文本说了什么，但 L1 要估价的是 **Spark 实际读了什么**，两者不等 |

**改法**：拆成四个模块，`workload.py` / `clickbench_workload.py` 降级为 `hand_catalog_tpch.py` / `hand_catalog_clickbench.py`——只留 `QUERIES`，**冻结**，唯一用途是给 `workload_snapshot.py --compare` 做交叉验证。

| 新模块 | 职责 | 数据来自 |
|---|---|---|
| `dataset_snapshot.py` | 布局几何 + 列事实 | 对象列表（文件数、压缩字节，精确）+ 抽样 footer（RG 数、RG 字节、列顺序、列字节占比、`rg_span`）+ `column_stats.json`（ndv/cdf） |
| `workload_snapshot.py` | 每查询 `scans[]` | eventlog 的 `physicalPlanDescription` → `Scan parquet` 的 ReadSchema / PushedFilters / Location |
| `adaptive_physical_options.py` | 物理动作空间 | 由实测几何在**计数空间**生成：`n_files ∈ {floor, 2·floor, 4·floor, …}` 上界取基线文件数，`floor = min(parallelism, baseline_files)`，最后才折成字节 |
| `advisor_policy.py` | 闸门阈值 + 模型假设 | 手写，但明确标注为"决策"或"承认的近似"，与数据集无关 |
| `advisor_catalog.py` | 把上面四个装配成模型读的那个对象 | — |

查询号在 eventlog 里不存在，`workload_snapshot.py` 两条路解析：优先读 execution description 里的 `track2:q<N>`（`run_benchmark.py` 现在每条查询前 `setLocalProperty("callSite.short", ...)` 打这个标记，新跑的日志自描述）；归档日志退回**位置对齐**——查询串行执行，同一份日志里第 k 个带扫描的 execution 就是第 k 条查询，日志短的是被中断的那轮、只覆盖前缀。TPC-H 五份日志各 22 条、ClickBench 三份 43/43/25，对齐无歧义，且用手写目录逐条复核过。

**发现的四个真实错误**（都不是重构引入的，是重构**暴露**的）：

1. **`PushedFilters` 正则的括号 bug，和上一条修的逗号 bug 同源。** `PUSHED_RE = r"PushedFilters:\s*\[(.*?)\]"` 非贪婪匹配到第一个 `]`，而 `In(l_shipmode, [MAIL,SHIP])` 的第一个 `]` 在括号**内部**，于是整条谓词被截断、解析失败、静默丢弃。TPC-H Q12 和 Q19 的全部 IN 谓词就是这么消失的（谓词数 60 → 81）。`Location` 和 `ReadSchema` 是同一种写法，一并改成括号配对扫描。
2. **`Not` 被展平时没有取反。** `Not(EqualTo(p_brand, Brand#45))` 被记成 `eq`，L1 于是认为 Q16 只读 1/ndv 的行，实际读 1−1/ndv——差 24 倍。排序键**排名**不受影响（这一列无论如何都值得关注），但选择率模型受影响，而新的 workload snapshot 正是喂给后者的。现按 De Morgan 下推到叶子。
3. **手抄目录漏数了扫描。** 计划里 TPC-H 有 **95** 个 `Scan parquet`，手抄目录只有 76 个：Q2 的相关子查询会重扫 partsupp/supplier/nation/region，Q18 三扫 lineitem，Q22 两扫 customer。L1 一直在**低估**这些查询。手抄目录的谓词集合是运行时集合的**真子集**（`hand-only` 全空），即运行时目录不仅正确、而且更全。
4. **`BASELINE_GEOMETRY` 的 lineitem 行组大小 247 MB 是抽样偏差。** 实测分布是三簇：1 / 161 / 236 MiB，平均 **1.5 个行组/文件**，不是抄进去的 2。原来的 `_layout_manifest.json` 抽了**前** 20 个文件，恰好都落在 236 MiB 那一簇。快照现在记录**均值**而非中位数，因为 L1 只以 `n_rg × rg_bytes = 总未压缩字节` 的形式消费它——两个中位数（按前 N 个文件抽 236 MiB、按均匀抽 161 MiB）都是合法的中位数，但都还原不出总量。

**闸门 A 被实测证伪，已改写。** ClickBench `EventDate` 抄进 `SYNTHETIC_STATS` 的 `rg_span` 是 **0.5232**，恰好卡在阈值 0.5 上方；**实测是 0.358**。也就是说旧形式的闸门 A 会否决 `hits-baseline-eventdate`——**E8 实测 −51.3%** 的那个布局。这个数值只有在统计量从"抄"变成"测"之后才可能被发现。

绝对阈值的形式本身也是错的：完美排序后每个行组约覆盖 `1/n_rg` 的值域（相异值不足时被 `1/ndv` 顶住），所以 0.358 在 165 个行组的表上叫"几乎没聚簇"，在 3 个行组的表上叫"已经完美"，绝对阈值把两者读成同一件事。现改为相对**可达跨度**的余量比：

```
achievable = max(1/n_rg, 1/ndv)
拒绝条件：rg_span < achievable × --cluster-headroom-min   （默认 2.0，消融旋钮）
```

判定：TPC-H `l_shipdate` 300×（过）、`o_orderdate` 100×（过）、`l_orderkey` 1.29×（拒，dbgen 本来就按 orderkey 输出）、ClickBench `EventDate` 6.1×（过，与 −51.3% 一致）、`CounterID` 5.0×（过，但它只有等值谓词、根本进不了排序候选，且闸门 C 会拦）。**遗留近似**：`rg_span` 量的是值域，而排序均衡的是**行数**，所以 `achievable` 对偏斜列偏小、闸门对偏斜列偏松。

**验证（离线，未重跑基准）**：

| 数据集 | 自洽 GET 误差 | 自洽字节误差 | top-1 | 与归档对比 |
|---|---|---|---|---|
| TPC-H SF100（重构前） | 2.6% | 2.9% | `lineitem-shipdate` + `orders-orderdate`，2385.0 s | E8 实测 −37% |
| TPC-H SF100（**重构后**，2160 点） | **6.2%** | **0.5%** | 上式 + `orders-32f` + `o_orderstatus` identity 分区，2374.8 s | 归档 top-1 落在**第 3 名**（2398.7 s，差 1.0%） |
| ClickBench SF1（**重构后**，16 点） | —(E2 未记 IO) | — | **`hits-baseline-eventdate`** | **与归档/实测 top-1 完全一致** |

字节误差从 2.9% 降到 **0.5%**（好 6 倍），这是几何与投影列表同时改为实测的直接结果。GET 误差**变差**（2.6% → 6.2%）：扫描节点从 76 涨到 95，每个都按 `META_GETS_PER_OPEN` 计了一次 open，而 Spark 在同一条查询内会复用部分 open。这是一个**具名的模型缺口**，不是抵消掉的误差——重构前 2.6% 的好看数字，部分来自"漏数扫描"与"高估行组数（400 vs 实际 300）"两个错误方向相反。

TPC-H 的 top-1 变了，差距 1.0%，在模型分辨率以下，**不宣称是改进**。新 top-1（orders 32 文件 + `o_orderstatus` 两目录分区）是旧手写网格**表达不出来**的点：旧网格里 orders 的文件档只有 128/256/512 MB，512 MB 只剩 13 个文件、低于并行度下界被拒，而分区候选从来只在基线文件数上评估过。m2_gate 的判据（L1 top-3 含实测最优）仍然成立。

**限制**：（1）位置对齐只对"查询串行、每条恰好一个带扫描的 execution"成立，AQE 或缓存改变这一点就会错位；新日志靠 `track2:q<N>` 标记，旧日志靠与手写目录交叉验证，两者都有，但这不是通用方案。（2）`rg_span` / 列字节占比来自抽样 footer（默认每表 8 个文件），lineitem 那种三簇分布下抽样噪声是真实存在的，不是随便加大 `--sample-files` 就能免费解决。（3）`hand_catalog_*.py` 保留是为了留证据，不是留退路：`analyze_layout` 已经**没有**手写网格 fallback，没有 eventlog 就直接报错——上一版那个 fallback 会让"advisor 选中了 `l_shipdate`"退化成"读回某人写进常量里的 `l_shipdate`"。

### 2026-08-24 —— 排序/分区候选改由运行时谓词生成；派生变换分区记为负结果；CPU 残差改名

**问题**：`sort_from_predicates` 只把实测中位数当标量权重用，谓词结构全部来自手抄的 `workload.QUERIES`；而且它的返回值没有任何消费者，真正的候选来自 `SORT_GRID` / `PER_TABLE_SORT` 两个手写常量。等于结论先写死、再用另一份手抄目录去"证实"。同时 `collect_semantic.py` 采集的 `pushed_filters` 只被 `correlate.py` 用于 E1 覆盖率记账，advisor 从未读过。

**改法**：新增 `predicates_from_runtime.py`，把 eventlog 里的 `Scan parquet` 变成 `(表, 列, 算子, 字面量)` 目录，按该 execution 自身的 `end_ms − start_ms` 加权——不需要 query-id 映射，排名是运行时记录的纯函数。range 谓词给排序键，低 NDV 列给 identity 分区键（两者分开：2 值列拿不到它用不上的排序键）。`analyze_layout.py` 的 per-table 网格改为由 `large_tables()`（实测几何）× 运行时键笛卡尔积生成，`PER_TABLE_SORT` 降级为无 eventlog 时的 fallback。`clickbench_workload.generate_per_table_grid` 是第二份手写网格副本，已删除。

顺带修了 `collect_semantic.py` 的括号 bug：`pushed_filters` 原先用 `split(",")`，把 `GreaterThanOrEqual(l_shipdate,1994-01-01)` 劈成两段，字面量全丢、还造出 `EqualTo(n_name` 这种假列名。现改为括号感知切分并递归展开 `And/Or/Not`、`In(col,[...])`。

**验证（离线，未重跑基准）**：

| 数据集 | 网格来源 | 闸门 | top-1 | 预测 | 实测 |
|---|---|---|---|---|---|
| TPC-H SF100 | 手写（fallback） | A/C/D | `lineitem-shipdate` + `orders-orderdate` | 2385.0 s | −37%（E8 n=5） |
| TPC-H SF100 | **运行时**（1620 点） | A/C/D | **同一布局** | 2385.0 s | 同上，无需重测 |
| ClickBench SF1 | 手写 | **全关** | **`CounterID`** | −402.5 s (−14.5%) | **+23.5% 回归** |
| ClickBench SF1 | **运行时**（24 点） | A/C/D | `EventDate`（CounterID 未被提名） | −751.5 s (−27%) | **−51.3%（n=2）** |

TPC-H 上运行时目录**独立复现**了手写的两个键：`l_shipdate` 5839.2 s（n=35，ge×30/gt×5/le×10/lt×25）、`o_orderdate` 3732.5 s（n=25），都是各自表的第一名。1620 点的运行时网格（含 identity 分区轴）选出的 top-1 与归档 E8 的 `cand_ptable_sort_sf100` **是同一个布局**，因此不必重写 SF100：那个点已实测 2019.2 s vs 基线 3205.0 s（−37%，CV 2.89%）。fallback 路径也逐位复现归档（180 候选、4 个合法点、regret 0%）。

ClickBench 上 `CounterID` **只以等值谓词出现**（ndv=7526），根本进不了排序候选——加上闸门 A（`rg_span=0.080`），两条独立机制都会否掉那次回归。运行时网格给出的 −1.4% 同样是正确答案：基线已聚簇，本来就没得赚。

**过程中修掉两个真实的几何建模错误**，都是在跑实验之前发现的——这正是把候选生成从手写网格换成运行时推导的副产品：手写网格只含被测过的点，模型错了也看不出来；一旦开始枚举没测过的点，错误立刻暴露。

1. **`partitionBy` 的文件数是 `F × P`，不是 `F`。** 第一版记成 `n_parts × ceil(F / n_parts)`（≈ 不变），于是 `l_returnflag` / `o_orderstatus` 分区看起来能再省 85 s。但 `write_layout` 先按排序键 `repartitionByRange`、再 `partitionBy`，而 partitionBy 由**每个写出 task 各自执行**：排序键与分区键不相关，每个 task 都含全部分区值。lineitem 200 文件 × 3 个 returnflag = 600 个 36 MiB 文件，228 ms RTT 下多出的 open 远超剪枝收益。改正后两个数据集上都没有分区候选通过 §4.1——它们是**被代价否掉的，不是被闸门否掉的**。模型自洽性检查：分区列上有等值谓词时开 201 个文件（≈ 未分区的 200），无谓词时开 600 个。
2. **排序键的 NDV 是文件数上界。** `repartitionByRange(F, key)` 产生的非空区间不可能多于 key 的相异值个数。实测：ClickBench `EventDate` 只有 17 个不同日期，请求 110 个文件、实际写出 **17 个 882 MiB 文件**。第一版模型按 110 个文件估价，预测 −1.4%；按真实的 17 个文件估价是 **−27%**（每次全表扫描少开 93 个文件，43 条查询累积 GET 从 44880 降到 28357）。候选现在携带 `sort_ndv`，由运行时目录从 column stats 填入。

**ClickBench E8 实测（`e8_sf1_eventdate_s3`，S3 跨云，n=2）**：

| | 基线 E2 | EventDate | |
|---|---|---|---|
| 两轮 | 1826.0 / 2720.7 s | 1112.9 / 1103.3 s | |
| 中位 | 2273.3 s | **1108.1 s** | **−51.3%** |
| CV | —（两轮相差 49%） | **0.62%** | |
| 变慢的查询 | — | **1 / 43**（Q40 +9.0%） | |

即使拿基线**较快**的那一轮（1826 s）比，仍是 −39.3%，结论不依赖取哪一轮。先前在 `CounterID` 排序下整体回归的 Q37–43 组现在普遍变快（Q38 −18.0%、Q41 −19.3%、Q43 −17.0%）。canary（Q1/7/24/43）先行验证可读性通过，Q24 那条 105 列 `SELECT *` 从 336.8 s → 171.5 s（−49.1%），确认收益主要来自全表扫描少开文件。合同 n=5 未做，**不能报成过门禁**；基线 n=2 且方差大（CV 无意义），这个数字是方向性的，不是门禁结论。

**遗留风险**：L0 的可读性闸门只检查**请求的** `parquet.block.size`，而 EventDate 布局没有请求 RG 动作、RG 是全局排序压出来的副产品——实测 footer 中位 335 MiB / 最大 399 MiB（基线 192 / 391 MiB）。M2 canary 曾在中位约 473 MiB 时触发 parquet vectored 300 s 写死超时。这次 399 MiB 读下来了，但闸门确实看不见「排序导致文件变少变大、RG 随之变大」这条路径，属于运气而非设计。应把预测 RG 大小（由 `n_rg` 与 `sort_ndv` 推出）纳入闸门。

**分区（负结果）**：`PARTITION_GRID` 里的 `l_shipdate:year` / `:month` / `o_orderdate:year` 在 bare Parquet 上**可证明惰性**。`write_layout` 把它们渲染成 `l_shipdate_year` 派生列加 `partitionBy`，而 Spark 按**分区列名**匹配谓词；TPC-H 查询写的是 `l_shipdate >= DATE '1995-01-01'`，从不提 `l_shipdate_year`，于是零目录被裁、文件数反而上升——比不分区更差。E5 早就预测它们为负收益，但原因一直没写下来。要让变换生效必须由引擎改写谓词（hidden partitioning，即 Iceberg），不是改 writer 配置能解决的，故移除并记为负结果。替代品是运行时低 NDV 列的 **identity 分区**（`write_layout` 本就原生支持 `transform == "identity"`，无需改 writer）。

**L0 闸门 D**（`--max-partitions` 默认 64，`--min-partition-bytes` 默认 128 MiB）：派生变换直接拒；identity 分区超过目录数上限或每目录字节低于下限则拒。实测判定：`l_shipdate:year` 拒（派生列）、`l_shipdate` identity 拒（2505 目录 / 每目录 9 MiB）、`l_quantity`(57) 过、`l_returnflag`(3) 过。`virtual_footer` 相应区分两者：identity 分区按目录裁剪（range 谓词同样能裁，这正是派生变换做不到的），与排序前缀同列时用 `min()` 而非相乘。

**改名**：`CPU 残差` → `execution_residual`（`calibrate_execution_residual` / `residual_base` / `t_exec_residual_s` / `join_agg_residual_scale`）。它不是 CPU 模型，是 `实测中位数 − 预测 I/O` 的标定残差，解压解码、聚合、shuffle、调度、JVM/GC **以及 I/O 项自身的误差**全被它吸收，再按扫描行数线性外推；线性外推对 shuffle/join 阶段和每查询固定开销都是错的。改名后 E5 逐位不变（自洽 2.6%/2.9% PASS，top-1 2385.0 s，regret 0%）。归档结果 JSON 保留旧键 `t_cpu_s` 不动。

### 2026-08-21 —— L0 增加排序闸门 A（基线聚簇度）与闸门 C（剪枝后并行度）

ClickBench SF1 按 `CounterID` 重排后整体变慢（S3 +23.5%）：基线 `hits.parquet` 已是 ClickHouse 主键序，`CounterID` 的 `rg_span=0.080`，剪枝空间几乎为零；等值谓词 `CounterID=62` 把存活文件从 3 压到 1，16 槽只剩 1 个 task。TPC-H 相反：`l_shipdate` `rg_span=0.999`，日期范围排序是从零造出剪枝。

L0 增加两条可关的硬闸门（不做闸门 B：带排序键谓词的时间占比与聚簇度同一信号）：

- **Gate A** `--cluster-span-min`（默认 **0.5**）：排序前缀的基线 `rg_span = avg(rg.max−rg.min)/(global.max−global.min)` 低于阈值则拒。CounterID 0.080 拒；`l_shipdate` 0.999 / `o_orderdate` 1.000 过。阈值留给后续消融。
  > **后续已证伪（2026-08-24 其二）**：这个绝对阈值形式是错的，且它当时的标定依据 —— ClickBench `EventDate` 的 `rg_span` —— 是手抄的 0.5232，实测为 0.358。旧形式会否决 E8 实测 −51.3% 的那个布局。现改为相对可达跨度的余量比 `--cluster-headroom-min`，详见更新日志。
- **Gate C** `--prune-parallelism-floor`（默认 **4**）：剪枝后存活 RG **且** 存活文件都低于门槛则拒。`K_eff` 仍参与 L1 代价（软排名），但 1–2 个 task 的串行化不再只靠代价折扣。默认不是 16：TPC-H Q4/Q10 三个月窗口在 orders 上大约 5 个文件，E8 `o_orderdate` 是实测赢家。TFS 的「大表文件数 ≥ min(16, 基线)」门槛未改。

`rg_span` 从 Parquet footer min/max 收集（`parse_footer.py --clustering`；`column_stats.py` 顺带写入）。SYNTHETIC_STATS 带了 SF100 / ClickBench SF1 的实测值。未重跑 E8。

### 2026-08-19 —— E8 复测：按表 sort、文件数保持基线

对推荐布局 `li-baseline-shipdate_o-baseline-odate_ps-baseline` 按 E2 口径跑 22×5 cold-cache。写出 `s3a://home-haoyue/track2/cand_ptable_sort_sf100`：lineitem 200 文件 + `l_shipdate` 全局 sort，orders 100 文件 + `o_orderdate`，其余表 Spark 默认（文件数与 E2 相同）。`write_layout` 在无 TFS 的 sort 上钉死基线文件数，避免 shuffle.partitions 把 orders 写成 200 文件。归档：`results/track2/e8_ptable_sort/`。

**对照上一版 E8（全局 1GB + shipdate）**：稳定性明显更好，端到端略快，单查询 guardrail 仍未过。

| | E2 基线 | E8 1GB（旧） | E8 按表 sort（本轮） |
|---|---|---|---|
| 五轮 wall-clock / s | — | 4304, 2374, 2128, 1958, 1932 | **1985, 2011, 2019, 2137, 2056** |
| median / s | 3205 | 2128（−33.6%） | **2019（−37.0%）** |
| CV | 1.99% | **39.5% FAIL** | **2.89% PASS** |
| Q11 | 45.8 | 54.2（+18%） | **45.0（−2%）** |
| Q18 | 170.2 | 205.4（+21%） | 225.5（**+32% FAIL**） |
| Q22 | 39.6 | 33.3 | 45.7（**+16% FAIL**） |
| 合同门禁 | — | FAIL | FAIL（Q18/Q22） |

五轮全部 `error: null`。墙钟 CV 从 39.5% 降到 2.89%，达到 E2 同级稳定性；run-1 不再出现 Q9/Q18/Q21 的 500–600 s 离群。Q11 回归消失（维表文件数未合并）。Q18 仍慢：两次全表 lineitem 扫描，`l_shipdate` sort 不裁剪，且比 1GB 布局的 Q18 median 更差（225 vs 205），只是不再有 552 s 尖峰。Q22 扫 orders 无日期谓词，`o_orderdate` 排序没有帮助。P95/P99 未回归。合同禁止为过门禁丢掉回归查询。

L1 曾预测 −25.7% 且 Q18 持平；实测 −37%（sort 裁剪比模型更强），但 Q18/Q22 是模型没抓住的无谓词扫描代价。未改合同门槛。E12 仍预留。

### 2026-08-19 —— 按表 file size / sort（默认搜索空间）

全局 TFS 网格在 L0+§4.1 下只剩 baseline。本轮把动作做成**按表**：lineitem / orders / partsupp 各自选 file size 与 sort，小表保持 Spark 默认。不是 8 表全笛卡尔积：lineitem 5×2、orders 3×2、partsupp 3×1 → **180 点**。非法 file size（orders/partsupp 的 512MB/1GB）不进网格。

`actions[]` 增加可选 `table` 字段（合同 3.3 `scope.table`）。`write_layout.render(actions, table=)` 写出时按表覆盖；`virtual_footer.layout_for` 估价时按表解析。旧的全局网格仍可用 `--grid global`。

**搜索（L0 ∧ §4.1 vs 基线 L1）**：180 点 L0 全过，§4.1 拒 176，合法 4。最优 `li-baseline-shipdate_o-baseline-odate_ps-baseline`，预测 2385 s（−25.7%），max regression 0%。迭代分解命中同一点。合法集里**没有任何改 file size 的点**——改文件数仍会触发 Q11/Q13 一类回归。收益来自按表排序：lineitem `l_shipdate`、orders `o_orderdate`；partsupp 不动。Q11/Q18 预测 0%。Q06 −85%、Q14 −84%、Q12 −63% 仍是 sort 裁剪。

写出候选：`e5_whatif/recommended_per_table.json`。未重写 S3，未重跑 E8。若复测，应只对 lineitem / orders 做全局 sort，文件数保持 E2 基线。

### 2026-08-19 —— L0 并行度下界 + L1 无谓词 join/agg + §4.1 剪枝

E8 的 Q11/Q18 回归来自**全局** `target-file-size=1GB`：对 lineitem（22 GB → 22 文件）合适，把 orders 100→7、partsupp 50→5，低于 `local[16]`。顾问过拟合了 fact 表。每张表是独立的 Parquet 文件，**可以**有不同的 file size 与排序；当前网格仍是全局旋钮，本轮不爆炸搜索空间。

**L0**：压缩量 ≥ 2 GiB 的表（lineitem / orders / partsupp）要求 `n_files ≥ min(16, 基线文件数)`。全局 512 MB / 1 GB 非法。GET/GiB 自洽门禁未变：2.6% / 2.9% PASS。

**L1**：`K_eff = min(K_busy, n_files_opened, 16)`；无谓词多表扫描把 `t_exec_residual` 再乘 `(n_files_base / n_files)^0.5`（不是 join 基数模型）。基线大表文件数 > K，validate 公式与旧的 `/K_busy` 一致。

**搜索**：目标与合同 §4.1 对齐——可行性剪枝，不是加速。候选相对**同一套 L1 的基线预测**逐查询回归 ≤ 10%（标定后等于 E2 median）。321 点中 L0 拒 160，§4.1 再拒 160，**合法集只剩 baseline**。L0 合法但不看 guardrail 的最优是 `p-none_f-256MB_rg-128MB_s-l_shipdate`（2965 s，−7.7%），max regression 41%，11 条查询越线。RTT 扫到的两边都是 baseline。

这不是搜坏了：网格最细的全局 file size 已是 128 MB，而 orders 基线约 65 MB（100 文件）、partsupp 约 88 MB（50 文件）。全局 TFS 无法在合并 lineitem 的同时保住维表并行度。下一步应是**按表**的 file size / sort，而不是把全局网格加密。未重写 S3，未重跑 E8。

### 2026-08-18 —— 阶段 F / E8 开始：按 E2 口径复测 L1 推荐布局

在同一腾讯云客户端上对 `p-none_f-1GB_rg-128MB_s-l_shipdate`（`s3a://home-haoyue/track2/cand_rg128_sf100`）跑 22 查询 × 5 次 cold-cache，对照 E2 median 3205 s。`spark.task.maxFailures=4` 仅为跨云断流重试，不是布局旋钮。E12 仍预留。归档：`results/track2/e8_acceptance/`。

**五轮已完成，E8 门禁 FAIL。** 22×5 全部 `error: null`。end-to-end **median 2128 s vs E2 3205 s（−33.6%）**，≥10% 这一条过了。但 CV **39.5%**（run 1 = 4304 s 离群，Q9/Q18/Q21 单次 500–600 s；后四轮 2374/2128/1958/1932 s），且 Q11（+18%）、Q18（+21%）单查询 median 回归超过 10%。P95/P99 未回归。见 `e8_gate.json`。合同禁止丢掉最慢一轮来过门禁。

### 2026-08-17 —— 阶段 E：What-if L0/L1 落地（E5 自洽门禁 PASS）

实现 DB2 Design Advisor 的 recommend/evaluate 纪律：`analyze_layout.py` 生成 321 点 PTO 网格；`whatif.py` 用 virtual footer 估价，不物化。

**自洽门禁**：L1 对基线预测 62391 GET / 153.68 GiB，对照 E2 64032 / 149.32，误差 2.6% / 2.9%（≤10% PASS）。`t_io` 2418 s，实测 median 3211 s，差额作逐查询 `t_exec_residual`（旧名「CPU 残差」）。

**系统常数**（`sysconst.json`）：RTT 228.5 ms，BW 226 MiB/s，`K_busy` 6.77。并发不是 local[16]：vectored 上限 4，busy 区间里大量是 HEAD+footer；见 `concurrency.json`。

**穷举（可读 RG 约束之后）**：257 个 L0 合法点（64 个因 requested RG=256 MiB 被拒）。最优 `p-none_f-1GB_rg-128MB_s-l_shipdate`，预测 2403 s（相对基线 −25%）。DB2 式迭代分解命中同一点，regret 0%。`partitionBy` 与过小 RG 在本 regime 下增加请求数，预测为负收益。

**256 MiB RG 为何被拒**：M2 第一只 canary（`p-none_f-1GB_rg-256MB_s-l_shipdate`，`s3a://home-haoyue/track2/cand_best_sf100`）压缩 10 查询 5/10 失败。parquet-hadoop 1.16 把 vectored wait 写死为 300 s（`HADOOP_VECTORED_READ_TIMEOUT_SECONDS`，无配置项）。写出的 RG 未压缩中位数约 473 MiB，vectored range 35–182 MiB 超时或 HTTP body 提前关闭。这不是 Reader 冻结旋钮，故 L0 增加「requested RG ≤ 128 MiB」（E2 已证明可读）。证据：`e5_whatif/m2_canary_unreadable.json`。

**RTT 敏感性**：25 ms 与 228 ms 的 top-1 相同（该点同时减少请求和字节，仿射代价无法翻转）。Spearman 0.97，最大名次移动 40。这是负结果，照实记录。

**经验相关列**（`l_receiptdate`~`l_shipdate`）：关掉后同一 top-1，预测只差 ~72 s。暂留，等 M2 实测再决定是否删除。

**E12**：已预留，不阻塞 M2。见 `results/track2/e12_reserved.json`。缺席则论文不得声称 D-8 / 同区 EC2。

**M2 门禁 PASS**（压缩负载 X=60%，10 查询）：L1 最优 `p-none_f-1GB_rg-128MB_s-l_shipdate`。n=1 曾报 1978.7 s（−1.1%），Q18/Q21 被跨云抖动抬高。n=5 cold-cache 后，逐查询 median 之和 **1310.4 s vs 基线 2000.5 s（−34.5%）**，run-sum median 1395.5 s，CV 5.26%（略超 E2 的 5% 墙钟 CV，`run_benchmark` 因此 exit 1；排序门禁仍 PASS，top-3 命中，regret 0%）。见 `e5_whatif/m2_cand_rg128_n5/` 与 `m2_gate.json`。

逐查询 median（对照 E2）：Q15 30 vs 231、Q12 43 vs 160、Q7 66 vs 165 仍是 sort 裁剪；Q18 205 vs 170、Q21 240 vs 296，n=1 里那两次 400+s 不是稳态。E8 仍要 22×5。

lineitem 写出：22 文件，RG 未压缩中位数 236 MiB（与 E2 的 247 MiB 同量级），offset index 在。`write_layout.py` 默认 `local[16]` / 32g，否则本机 32 核 `local[*]` 会在全局 sort 时 OOM。

### 2026-08-17 —— 合同 r4：E2 客户端 regime 偏离，预留同区门禁实验 E12

已归档 E2 跑在腾讯云 VM 跨云访问 `us-east-2`（RTT ≈ 228 ms），不是合同 D-8 的同区 `m5d.4xlarge`。
What-if 把 `(RTT, BW, K)` 做成模型输入，**不替代**「测过的机器 ≠ 合同写的机器」。

**改了什么**：`TRACK2_M0_CONTRACT.md` r4。D-8 目标环境不改。§1.2 增加偏离说明；§7 增加预留 **E12（同区门禁复核）**；§9 增加 O-7。

**为什么**：参数化只解决模型；对外数字若声称同区 EC2，必须有同区实测。当前 3205 s 仍是 M2 工作基线。

**尚未做**：E12 本身。安排在 M4 之后、论文数字冻结前：同区重跑 E2 基线与 E8 验收。缺席则论文不得写成 D-8。

阶段 E 的 What-if 架构按计划推进；E12 不阻塞 M2。

### 2026-08-14 —— 阶段 D 完成：E2 基线门禁 PASS（CV 1.99%）

合同 E2（`TRACK2_M0_CONTRACT.md` §7 / §4.2）：Spark/parquet-mr 默认布局、SF100、22 查询 × 5 次 cold-cache。
**CV = 1.99% < 5%，22×5 全部 `error: null`，门禁通过。**

归档根目录：`docs/adaptive-range-reader/results/track2/e2_baseline/`
（`report.json`、`per_query.csv`、`_layout_manifest.json`、`env_metadata.json`、`io/` 342 MiB、`eventlogs/` 5 个 Spark 4 `eventlog_v2`）。

| 项 | 值 |
| --- | --- |
| 布局 | `s3a://home-haoyue/track2/baseline_sf100`，空 action = `df.write.parquet(...)` |
| end-to-end 五次 / s | 3288.7, 3188.2, **3205.0（median）**, 3228.3, 3112.9 |
| mean / stdev / CV | 3204.6 s / 63.9 s / **1.99%** |
| 每 run IO（差分） | ≈ 64032 ranged GET、149.3 GiB |
| 最慢 / 最快查询 median | Q21 295.7 s / Q16 29.4 s |
| Q6 median | 151.1 s（与冒烟 156.6 s 同量级） |
| 冷缓存 | 每 run 新 JVM；无 sudo，`drop_caches` 未执行 |

**基线写出**（同日，约 43 min）：八张表 `--verify` 全部 `offset_index_present: true`，bloom 关。不再用 DuckDB 源当 E2 基线。

**库默认（不是我们的旋钮）**：Spark 4.1 planned write 把 lineitem 写成 **200 文件**，RG 未压缩中位数约 247 MiB；nation / region 为 13 / 5 个百字节级小文件。S3A 走 `FileOutputCommitter` copy-rename（发行版无 `spark-hadoop-cloud`）。

**第一次全量被 `/tmp` ENOSPC 打断**（Q8 shuffle spill）。已把 `spark.local.dir` 指到 `/data/home/haoyueli/track2-scratch` 后重跑；这不是查询方言问题，也不改 Reader 冻结项。作废日志：`/data/home/haoyueli/track2-data/logs/e2_baseline.log.enospc`。

**`report.json` 的 `io` 字段是累加的**：五次 run 写进同一个 NDJSON，后一次包含前一次。每 run 应用相邻两次之差。墙钟数字不受影响。

阶段 E（`analyze_layout.py` / `whatif.py`）未开始。

### 2026-08-07（r3）—— 跨引擎 Writer 可移植性定为备选项，动作改用引擎中立规范名

**触发**：实际应用中 Writer 未必是 Spark，也可能是 Flink 或 Trino，是否需要增加这种扩展能力。

**结论**：**Flink / Trino 适配器列为备选项，不进入 M1–M4**，但同时做两个零成本决策把将来的成本锁低。

**判断依据——布局动作空间是格式级的，不是引擎级的**：row group 尺寸、page 尺寸、排序、分区、bloom、
dictionary、压缩都是 **Parquet 文件的属性**，不是 Spark 的属性。换引擎不改变动作空间，
只改变「同一个动作怎么写出来」。因此三层 collector 中只有 `SemanticCollector` 是引擎相关的；
`SdkIoCollector` 与 `FormatMetadataCollector` 天然引擎无关，
`WhatIfEvaluator` / `ConstraintChecker` / `CandidateSelector` 也全部作用在格式与物理层。
这与 §7.7 早已确立的「SDK-centered，engine adapter 可插拔」架构一致，无需改动组件图。

**现在就做的两个零成本决策**：

1. **候选动作用引擎中立的规范名表达**。不自创词表，直接对齐 **Iceberg 表属性**——
   它已经是设计好的引擎中立词汇（`write.parquet.row-group-size-bytes`、`write.parquet.page-size-bytes`、
   `write.parquet.bloom-filter-enabled.column.X` 等），且 Spark / Flink / Trino 写 Iceberg 表时都遵守。
   `LayoutCandidate.actions[]` 因此同时携带 `canonical` 与 `rendered`；
   分析、What-if、约束检查、候选选择**一律只看 `canonical`**，只有 `RecommendationEmitter`
   在生成 Code Diff 的最后一步才做 rendering。
2. **能力矩阵增加 writer 维度**，不再是全局常量。

理由是成本的不对称：现在做几乎不花时间；等分析器、What-if 和 Emitter 全部写成 Spark 配置字符串之后
再回头改，就是一次真正的重构。

**关键技术发现——三个主流 Parquet writer 在页级索引上的行为完全不同**：

| writer | ColumnIndex / OffsetIndex |
| --- | --- |
| parquet-mr（Spark / Flink） | **恒写**，无关闭开关 |
| Trino 原生 writer | **不写**（trinodb/trino#9359 未完成） |
| PyArrow | `write_page_index`，**默认关** |

这使 §7.9 提出的「必须建立 Writer × Reader capability matrix」从工程规范升级为**有实证支撑的必要概念**：
同一条布局建议在不同 writer 上可能根本无法生效，而且**失效方式是静默的**。
具体后果是：任何依赖页级裁剪的建议在 Trino 写出的数据上先验无效，
若将来接入 Trino，L0 检查必须据此直接否决该类候选，而不能让实验跑出一个无法解释的负结果。
这一条可直接作为论文中 capability matrix 必要性的论据。

**备选项的具体范围**（不排期）：Flink 用 parquet-mr，**writer 侧无需改动**，只缺 plan/event adapter；
Trino 需要 event listener + patch renderer + L0 能力约束。
**最省力的路径是走 Iceberg 表属性**——写一个 renderer 即同时覆盖三个引擎，且与 PTO 的动作落点完全一致。

**为什么 MVP 仍用裸 Parquet 而不是 Iceberg**：Iceberg 会引入 manifest / metadata 的额外 GET，
这些请求也会进入 SDK telemetry 并需要归因，直接抬高 E1 的 ≥95% 覆盖率难度，
而动作空间与裸 Parquet 完全相同。故 MVP 保持裸 Parquet 以保证因果归因最干净；
Iceberg 留给「需要实证跨引擎可移植性」的时候。

**对下一步与论文主张的影响**：不影响 M1–M4 的任何实验与门禁。
论文层面新增一个可回答的 generality 问题：动作空间是格式级的、只有语义采集与 patch 渲染是引擎相关的，
并有 writer 能力矩阵作为证据；这比声称「支持多引擎」更诚实，也更容易被 reviewer 接受。

### 2026-08-07（r2）—— 引擎升至 Spark 4.1.x，Writer 从 PyArrow 改为 parquet-mr

**触发**：审阅当日冻结的 M0 合同（r1）时提出两个问题——(a) 既然 Spark 3.5 同时引入 Hadoop 与 parquet
两个版本问题，为什么不升版本；(b) 既然 parquet-mr 自 1.11 起默认写 ColumnIndex/OffsetIndex 而 PyArrow 默认不写，
沿用 PyArrow 作 Writer 是否合理。两个问题都成立，合同据此修订为 r2。

**(a) 版本问题**：**Spark 3.6 不存在**——3.x 线到 3.5 终止（extended LTS 至 2027-11），社区直接进入 4.x。
Spark 4.x 自带 **Hadoop 3.4.1+ 与 parquet-mr 1.15.2**，一次解决 r1 的两个版本问题：
S3A 已在 AWS SDK v2 上（无需 `bin-without-hadoop` 手工拼 Hadoop），且 parquet-mr ≥1.14 具备 vectored IO。
小版本选 **4.1.x**：4.0.x 于 2026-11-23 EOL（实验期内即失去支持），4.2.0 于 2026-07-14 才发布过新，
4.1.x（2025-12 发布，EOL 2027-06）覆盖整个项目周期。

**(b) Writer 选型**：改用 **Spark / parquet-mr** 作 Writer。真正的理由比「PyArrow 默认值弱」更强——
r1 中的 M-1、M-4 **不是研究发现，而是跨库组合自找的集成缺陷**。同库之后：

- **M-1 与 M-4 直接消失**：写侧与读侧默认值天然自洽；`ORDER BY` / `sortWithinPartitions` 是真实排序，
  不再有「`sorting_columns` 只是声明」的问题。
- **`parquet.block.size` 以字节计**，消灭 PyArrow `row_group_size`（单位是行数）的换算问题，r1 的 O-4 取消。
- **per-column 控制未损失**：parquet-mr 支持 `parquet.enable.dictionary#col`、
  `parquet.bloom.filter.enabled#col`、`parquet.bloom.filter.expected.ndv#col`。
- **SF100 可分布式写**；单机 PyArrow 逐候选写约 30GB 是时间黑洞。
- **论文可比性提升**：PTO 的动作落点就是 Iceberg/Spark 表属性，Code Diff 目标为 Spark writer 配置时属同类对象。
- **基线无需辩护**：基线即 `df.write.parquet(...)`，r1 中「人工把 `write_page_index` 打开以对齐 parquet-mr 默认」
  这一动作不再需要。

PyArrow 保留两个用途：footer 解析（只读，不受影响）与 M-5 逃生舱。

**能力矩阵的净变化**：

| 项 | r1 | r2 |
| --- | --- | --- |
| M-1 page index 默认值相反 | 跨库错配 | **消失**；性质改为「parquet-mr 恒写且无关闭开关 → page index 不是 Writer 动作」 |
| M-2 bloom 写关读开 | 跨库错配 | 保留，但性质改为**库内既定设计**（bloom 需预知 NDV），后果相同，仍须显式开启 |
| M-3 无 vectored IO | 能力缺失，列顺序先验无效 | **消失**；改为「默认值在 parquet 1.15/1.16 间翻转，必须显式冻结」 |
| M-4 `sorting_columns` 只是声明 | 跨库错配 | **消失** |
| M-5 per-column 压缩/编码 | — | **新增**：parquet-mr 的 `parquet.compression` 仅全局 |

**恢复的动作**：vectored IO 可用后 Range 合并会真实发生，**列顺序重新成为可表达动作**，
保留为 M5 候选（首轮门禁仍不含它）。代价是归因从 1:1 变为 1:N——一次 GET 可覆盖多个 column chunk。
归因仍然精确，`ObservationBundle` 已把 `column_chunk` 改为 `column_chunks` 数组并新增 `bytes_wasted`；
后者同时为 `PROJECT3.md` §3.4 的 `g* ≈ RTT × BW` 分析提供直接测量。E1 的覆盖率目标相应表述为
「≥95% 的 GET 可映射到一个**已知的** column chunk 集合」。按 §2.1 因果归因要求与 `TRACK2_PLAN.md` §4.2，
vectored IO 全程固定，开/关对比作为独立敏感性实验 E11。

**风险变化**：r1 的首要风险「误用 Spark 默认发行版（Hadoop 3.3.4 / SDK v1）导致采集静默失效」**消失**，
因为 Spark 4.x 自带 Hadoop 3.4.2。新增风险：vectored IO 是否在 Spark 向量化 Parquet 读路径上真正生效
（列为 O-2，E0 用实际 GET 合并证据确认；未生效则列顺序动作再次失效）；以及 Spark 4.x 需要 **JDK 17**
（当前开发机为 JDK 8，EC2 需安装 17）。

**验证方法**：探针 `tools/track2/probe_capability.py` 已重写为 **PySpark 写 → PyArrow 读 footer** 的往返验证，
覆盖 row group 字节档位、page size、per-column bloom（校验 `bloom_filter_offset` 非空）、
per-column dictionary（校验 `has_dictionary_page`）与全局压缩，并把只能对真实 S3 观测的项
（拦截器装载、vectored IO 是否真的合并、持续带宽）输出为 E0 人工清单。
本机无 pyspark/pyarrow/duckdb，已验证脚本优雅降级；实际探测在 E0 于 EC2 上执行。

**对下一步的影响**：M0 门禁仍满足，可进入 M1。周末真实云验证时需一并确认 JDK 17 与 Spark 4.1.x 的环境搭建。

### 2026-08-07 —— M0 计划冻结：确立实验合同与技术栈

**做了什么**：完成 `TRACK2_PLAN.md` §6 第一步「冻结实验合同」，产出 `TRACK2_M0_CONTRACT.md`
（数据契约、指标口径、候选档位、Writer × Reader 能力矩阵、实验清单）与能力探针
`tools/track2/probe_capability.py`。

**冻结的技术栈决策**：

- 查询引擎为 **Spark 3.5.x + S3A**；`SdkIoCollector` 用 AWS SDK v2 `ExecutionInterceptor`
  经 `fs.s3a.audit.execution.interceptors` 注入，不写自定义 FileSystem。
- **Hadoop 必须 ≥ 3.4.1**：S3A 自 Hadoop 3.4.0 才迁到 AWS SDK v2；在 Spark 3.5 默认捆绑的
  Hadoop 3.3.4（SDK v1）上该配置会被忽略且只打印警告，采集会**静默失效**。列为硬前提与首要风险。
- `FormatMetadataCollector` 走 **PyArrow 离线解析 footer**，与 §7.7「不让 SDK 在热路径重复解析 footer」一致。
- SF100 由 **DuckDB dbgen** 生成，但 **Parquet 由 PyArrow 写出**：DuckDB writer 不暴露
  page size / bloom / per-column dictionary，而这些正是扩展动作所需。此分工同时把
  `tools/track2/write_layout.py` 确定为 Code Diff 的目标文件。
- 云环境冻结为 AWS S3 `us-east-2` + EC2 `m5d.4xlarge`（同 region）。
- 代码落点：`tools/track2/`（Python）+ `s3-adaptive-range-reader` 内的采集钩子。

**识别出四处 Writer × Reader 能力错配**（M0 的关键产出，均已写入能力矩阵）：

- **M-1**：PyArrow `write_page_index` 默认 `False`，parquet-mr `parquet.filter.columnindex.enabled`
  默认 `True`。不显式打开写侧则 page 级裁剪永不发生，实验会呈现为「page index 无收益」这一**错误的负结果**。
- **M-2**：bloom filter 写侧默认不写、读侧默认启用，同类静默错配。
- **M-3**：Spark 3.5 自带 parquet-mr 1.13.1，**无 vectored IO**（1.14.0 才引入），
  S3A 的合并阈值不会被触发。对首轮有利（column chunk 与 GET 近似 1:1，利于归因），
  但使列顺序优化在本栈上先验无收益——与 §7.4「Reader 不合并邻近 Range 时列顺序可能无收益」的判断吻合，
  故确认**列顺序不进入首轮门禁**。未来做列顺序专项必须先升级到 parquet-mr ≥ 1.14 并启用 vectored IO。
- **M-4**：`sorting_columns` 只是 row group 元数据声明，PyArrow 不排序也不校验，parquet-mr 不用其裁剪。
  排序收益只能经由「DuckDB `ORDER BY` 改变行序 → min/max 收紧 → row group 裁剪」产生。

**基线口径的修正**：基线布局对齐 **parquet-mr 默认行为**而非 PyArrow 默认值，特别是显式
`write_page_index=True`。理由是 parquet-mr 自 1.11 起默认写 ColumnIndex/OffsetIndex；若沿用 PyArrow 默认，
基线将是一个连主流 writer 默认能力都不具备的弱布局，后续改动会因「补上本该有的 page index」而虚高——
即 §7.4 评测教训中「避免在较差 baseline 上得到漂亮百分比」。相应地，page index 的消融改为**从基线关掉**。

**验证方法**：能力矩阵不得仅凭文档采信。`tools/track2/probe_capability.py` 用实际安装版本逐项写读回验
（page index 用文件字节增量、bloom 用 `bloom_filter_offset`、dictionary 用 `has_dictionary_page` 与 encodings），
并把 JVM 侧不可探测的 Reader 配置输出为 E0 人工核对清单。探针失败的能力在 L0 静态检查中直接判为不可行。
本机无 pyarrow/duckdb，已验证脚本在缺依赖时优雅降级；实际探测在 E0 于 EC2 上执行。

**尚未解决的问题**：

- `m5d.4xlarge` 的 10 Gbps 是**突发**带宽，持续基线更低，可能使运行间方差超过 CV<5% 门槛。
  处置原则已写死：不达标就换实例类型，**不放宽 CV 门槛**。
- PyArrow / DuckDB / AWS SDK bundle 的实际版本号、`bloom_filter_options` 可用性、Spark 并行度标定、
  各表 `compressed_bytes_per_row`（PyArrow `row_group_size` 单位是行数而非字节，需换算）、
  语义关联主路径覆盖率，均列为 O-1..O-6 由 E0/E1 标定后回填，不影响主目标与边界。

**对下一步与论文主张的影响**：M0 门禁「主目标与边界无未决项」已满足，可进入 M1（观测与基线）。
M-3 意味着扩展动作空间中的列顺序一项在当前栈上不可能产生收益，论文中须如实记录为栈限制，
而不能表述为该动作本身无效。

### 2026-08-05 —— 将 Qd-tree 降为 related work

- 确认 Qd-tree 的 record-level routing、BID、非均匀叶子分块和查询路由与现有 Writer 参数优化主线适配成本过高。
- 不实现 Qd-tree Greedy、Woodblock、Writer-only/no-route 变体，不将其列为 MVP、baseline 或消融实验。
- 从当前 Advisor 组件图中移除 Qd-tree，只保留其 predicate-aligned cuts、长期组合收益和 anytime search 作为概念对照。
- 论文中将其定位为高自由度 workload-aware record-to-block assignment；用接入成本、Reader 依赖和优化目标与 Track 2
  的标准 Parquet Writer 布局优化区分。

### 2026-08-05 —— 分析 Qd-tree 的层次化分块与 RL

> 本条保留初始分析轨迹；其中“作为可选 CandidateGenerator 并开展 no-route 实验”的判断已由上方范围修正覆盖。

- 明确 Qd-tree 优化记录到块的分配：从 workload predicate 构造二叉 cut tree，叶子作为逻辑块；块内 Parquet 布局与其
  正交。
- 还原 Woodblock MDP：节点子空间为 state、predicate 为 action、完整树为 episode、normalized skipped records 为
  reward，PPO 负责长期切分组合的 credit assignment。
- 将 Qd-tree/Woodblock 定位为可选 `CandidateGenerator`，而非 What-if 成本模型；Greedy 是必须先通过的 baseline。
- 识别其与 Track 2 边界的冲突：BID、routing tree、查询改写和 replication。MVP 只允许 Writer-only/no-route 变体，
  routed 版本仅作为越界性能上界。
- 将最小行数约束改为压缩字节、文件数、大小偏斜和 GET 约束；RL reward 若扩展，应使用低保真 S3 成本并继续由真实云
  top-k canary 验收。
- 增加 `Greedy no-route → RL no-route → S3-aware reward → routed upper bound` 验证阶梯，并以同预算真实云
  selection regret 决定是否保留 RL。

### 2026-08-05 —— 将布局模式细化改为可证伪实验

- 不再预设真实统计分支必然优于 ATUN-HL 的有序 / 无序二元分支。
- 增加 `ATUN-HL binary` 与 `empirical branch` 对照；固定候选空间和其他成本项，比较中间量误差、候选排序、top-k recall
  与最终 selection regret。
- 只有真实统计稳定改善最终布局选择且覆盖额外 profiling 成本时才保留为小贡献；否则沿用论文的简单分支，并记录负结果。

### 2026-08-05 —— 分析 ATUN-HL 并补充解析型 What-if 层

- 明确 ATUN-HL 的 hybrid layout 是 row-group 横向切分加组内列存，不是多种异构布局共存。
- 借鉴其“系统常量、数据统计、工作负载统计、布局变量”四类成本输入，以及
  `pruned RGs → fetched bytes → request pattern → time` 的可解释分解。
- 将 ATUN-HL 式解析模型放入多保真 What-if 的低成本层，与 PTO 式 sample rewrite / surrogate 组成模型先验加残差校准，
  而非二选一。
- 决定重写 HDFS chunk/seek/locality 成本，改为对象存储 Range GET、RTT、有效带宽、合并、并发、重试和长尾模型。
- 将 sorted/unsorted 二值假设改为实测聚簇质量，将 dictionary 决策改为 per-column，并要求验证 Writer fallback 与
  Reader dictionary filtering 能力。
- 明确只联合调优 RGS 与 dictionary 已有先例；Track 2 的差异化必须来自对象存储请求计划、扩展 page/index/Bloom 动作、
  解析与学习模型结合及真实云 wall-clock 校准。

### 2026-08-05 —— 修正 DB2 Advisor 的借鉴范围

- 明确 DB2 论文只用于借鉴 Advisor 工程实现，不把其数据库物理设计目标、成本函数、动作空间或依赖分类迁移到 Track 2。
- 将动作依赖图和跨组件联合搜索降为后续可选优化，不再作为第一版标准组件或 MVP 前置条件。
- 第一版控制面聚焦 `WorkloadRepository → CandidateGenerator → ConstraintChecker → WhatIfEvaluator →
  CandidateSelector → RecommendationEmitter → CanaryValidator`。
- 将多保真 What-if 层确认为架构重点：不同候选生成方法必须经过统一的逐级估价和真实云验证后才能进入建议包。

### 2026-08-05 —— 分析 DB2 Design Advisor 并确定 Writer Advisor 控制面

> 本条保留最初分析轨迹；其中“依赖规划器作为标准组件”的判断已由上方“修正 DB2 Advisor 的借鉴范围”覆盖。

- 借鉴其 `RECOMMEND / EVALUATE` 分离思想，将 Track 2 拆为 workload repository、候选生成器、依赖规划器、
  约束检查器、What-if 评估器、全局枚举器、建议输出器和 canary 验证器。
- 决定以动作依赖图控制搜索：强耦合联合搜索，单向依赖决定顺序，弱依赖通过已接受状态传递；不对全部 Writer /
  Reader 参数做无差别笛卡尔积。
- 将 PTO 四参数实现为一个可替换的联合候选组件，扩展动作空间使用相同候选中间表示和统一评估接口。
- 引入多维搜索预算、边际收益停止和 best-so-far；压缩 workload 只用于搜索，最终必须回到完整 workload 复核。
- 明确湖仓缺少 DB2 式成熟虚拟布局估价能力，采用“静态检查 → 解析型 IO 模拟 → 样本改写 / surrogate →
  真实云 top-k canary”的多保真 What-if 层。
- 因观测窗口可能漏掉低频关键查询，不自动删除旧布局或历史数据；最终交付为含证据、Code Diff、canary、迁移和回滚的
  recommendation package。

### 2026-08-05 —— 明确双观测面采集架构与 wall-clock 目标

- 将系统定位修正为 SDK-centered、非 SDK-only：SDK 直接采物理 IO，engine adapter 采查询语义，format adapter / Advisor
  解析 Parquet metadata。
- 确定以 query/span id、object key、version/snapshot 和时间窗口关联三层 telemetry，并设置关联覆盖率实验。
- 将系统拆为 Adaptive SDK Runtime 与 Writer Advisor Control Plane，避免声称通用 S3 SDK 单独理解 SQL/Parquet。
- 明确相对 PTO 的扩展动作空间必须通过增量消融证明，而不能仅以参数覆盖面更大作为贡献。
- 冻结两级目标：scan-stage/IO critical-path wall-clock 用于候选排序，端到端 workload wall-clock 用于最终验收；
  skipped rows 保留为解释指标。
- 增加目标相关性实验、单查询与 P95/P99 guardrail、rewrite payback、cold-cache/steady-state 隔离。

### 2026-08-04 —— 分析 SIGMOD'17 宽表列顺序与复制论文

- 确认可复用其“工作负载加权 + 列大小 + 全局列序搜索 + 增量成本 + 漂移触发”的方法骨架。
- 决定不复用 HDD seek-distance 成本函数；S3 版改为实际 Reader 合并 Range 下的 RTT、传输字节与 wall-clock 成本。
- 决定暂不实现列复制，因为其需要 replica schema 与查询重写，超出 Track 2 MVP 的低侵入边界。
- 修正负载定位：TPC-H 继续承担一般 Writer 布局 MVP，但列顺序优化需要单独的真实或合成宽表负载。
- 增加评测要求：同时报告 Reader 与端到端结果，并以绝对 wall-clock 比较联合配置，不能只看相对收益。

### 2026-08-04 —— 确定布局优化器的定位

- 将模拟退火降级为可替换的候选搜索器；Track 2 的关键资产是候选空间、硬约束、S3 版成本函数和真实云校准闭环。
- 明确大模型 API 只用于代码 / schema 理解、约束提取、Diff 与报告生成，不作为最终列排列器或性能评判器。
- 确定三层验证：约束校验 → 代理成本筛选 / 搜索 → 真实云 wall-clock 复测；积累真实样本后可训练轻量 surrogate。

### 2026-08-04 —— 分析 PTO 工作负载驱动预测表优化器

- 确认 PTO 已覆盖 partition、TFS、RGS、multidimensional sort 的联合发现，应作为 Track 2 的直接 baseline。
- 借鉴两阶段结构：scan-fragment 驱动候选缩减 → 少量 sample layouts 实测标注 → GBT 预测完整候选空间。
- 决定不使用 skipped rows 作为最终标签；改为双观测面下的真实云 wall-clock 主目标与多指标成本向量。
- 将输入范围从纯 offset trace 扩展为 scan fragments、Parquet footer map 和 SDK physical trace。
- 增加结构保真采样、rewrite 回收周期、单查询回归与 P95/P99 guardrail。
- 修正论文差异化定位：在 PTO 之外研究查询语义跳过与对象存储实际 Range/GET 成本的联合优化，并覆盖列物理顺序、
  page/index 和 Reader 合并联动。

### 2026-08-04 —— 明确列式布局的影响面

- 将布局问题拆成表 / 数据集、文件、row group / stripe、page / 编码四层。
- 确认 Track 2 的布局建议必须归因到少对象、少 GET、少字节或更好局部性中的至少一项。
- 确认 TPC-H MVP 优先评估分区 / 排序、row-group 大小、文件大小与 page index；列顺序排在其后。
- 增加关键限制：共访问不等于物理邻近必然有收益；列顺序建议必须结合 column chunk 映射和实际 Reader 的 Range 合并行为验证。

### 2026-08-04 —— 建立 Track 2 专用工作文档

- 通读 `PROJECT2.md` 与 `PROJECT3.md`，确认当前路线以 `PROJECT3.md` 为准，`PROJECT2.md` 只作历史证据库。
- 确认 Track 2 已从早期的“布局报告”提级为 8 月优先的 **Writer 重写建议 + AI 辅助 Code Diff / PR** 路径。
- 固化非侵入式、建议式、量化驱动、真实云验证和人工审核边界。
- 固化 TPC-H Parquet 作为首个 MVP 负载，以及真实云 wall-clock 改善 ≥10% 的止损门槛。
- 明确从本次起，后续所有 Track 2 变更均记录在本文件中。
s