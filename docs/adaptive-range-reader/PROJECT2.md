# 湖仓 S3 自适应 IO —— 项目总览（PROJECT2）

> 用途：给后续 agent / 协作者一份**精简可交接**的项目全景。原始逐日纪要见 `project_notes.md`
> （919 行，含全部实验细节与推导过程），本文件只保留**当前有效的结论、资产与方向**。
> 创建：2026-07-09。权威路线以本文件 + `project_notes.md` §19/§20 为准。

---

## 1. 项目背景与演进

**一句话**：目标是在 AWS SDK 层提升对 S3（湖仓对象存储）的访问效率；经过访问模式刻画与策略
评测，方向从"加统一缓存"演进为"**按负载/AppID 自适应选择 IO 访问模式 + 非侵入式文件布局推荐**"。

演进脉络（每一步都有实验支撑）：

1. **初衷**：S3 首字节延迟高（~20–100ms/GET），想在 SDK 侧加 buffer/prefetch/cache 提速。
2. **打热力图**：对四类真实负载采 offset 级 trace 并可视化，发现**每类负载的访问模式典型且稳定**，
  且**随数据规模线性缩放**（相对模式不变）。→ 见 §5 数据、§6 结果。
3. **评测访问策略**：用离线 replay simulator 比较 s3a fadvise(seq/random/normal)、s3a prefetch、
  aws range get 等对**读放大(read amplification)与时延**的影响。结论：**不存在通用最优策略，
   每类负载各有一个独立最优**。→ 见 §6.2。
4. **缓存降级**：热力图显示负载中期常**全量访问**、且访问模式**几分钟即变**，"静态/统一缓存"主线
  被放弃。注意：缓存并非完全否定——**embedding/Zipf 负载的行级 LRU 仍有显著价值**（§6.1），
   它作为"某类负载的一种策略"保留，不再是项目主线。
5. **新方向（双轨）**：既然每类负载有独立最优，就让 SDK **自适应地选**；并进一步做**文件布局推荐**。

**当前两个关键点（项目目标，见 §2）**：

- **AWS SDK 上的自适应 IO 访问模式优化**；
- **非侵入式的文件布局推荐**。

---



## 2. 新的项目目标（双轨，2026-07-07 会议确立）



### Track 1 —— 自适应 IO 访问模式优化（主线，先做）

- SDK 侧采集 IO 特征 → 用**轻量统计模型（决策树 / SVM）**实时选择 IO 策略（预读窗口、
顺序/随机 fadvise、异步、并发等），**以 AppID（或SDK原生certification） 为组织维度**（沉淀每个应用的历史模式为先验）。
- 替代 S3A 的固定策略。目标性能提升 **~15%**。
- contextual bandit / LinUCB 曾是主推，现**降级为可选兜底**：仅当统计模型在混合负载/负载切换
场景明显不足时再引入。



### Track 2 —— 非侵入式文件布局推荐（第二阶段）

- 基于历史访问数据挖掘**共访问模式**，**生成优化的 Writer 代码 / 物理布局建议**
（把常被一起访问的列/块物理放近；调 row-group 大小、列排序、共置）。
- **非侵入"建议式"**：只产出报告/代码片段交应用层采纳，**不改文件格式本身、不做地址重映射**。
- **否决地址映射/重排方案**（需维护映射表，表丢失即文件不可读 + 查表开销）。
- 目标在 Track 1 基础上**叠加 ~15%**，合计约 **30%**。



### 工程约束（贯穿双轨）

- 功能开关**默认关闭**（opt-in）；失败可**回退纯 demand read**；
- **预算硬约束**先行（内存 / in-flight bytes / 并发 GET 上限）；
- **指标观测内建**（避免"效果不可证伪"）。



### 交付

- **7 月产出第一版 POC**（系统架构设计 + 基本功能 POC）验证方向有效性。

---



## 3. 核心洞察（贯穿全项目）

1. **两层问题分治**：文件结构层（格式规范公开，几乎无不确定性）→ 用 **format-aware 规则**；
  查询负载层（用户下一秒查什么）→ 用**轻量在线学习**。
2. **S3A prefetch 慢的根因是不感知上层文件格式**，不是块大小。
3. **没有通用最优策略**：顺序最优的 `s3a_prefetch` 在多模态上读放大 94x；`s3a_seq` 在随机
  embedding 上读放大 143x。→ 这正是"自适应选策略"的立项依据。
4. **重型序列模型（LSTM）不适合 SDK 部署**（µs~ms 决策、几十 MB 内存、无 warm-up、需可解释）。
5. **访问模式随负载类型区分、随规模线性缩放**：算法开发用小规模即可，只有最终延迟验证才上 MinIO/AWS。

---



## 4. 已有代码与工具

> 运行环境：`./.venv`（Python 3.11.9，`duckdb / pyarrow / pandas / numpy / matplotlib / lance / h5py`）。
> 统一用 `./.venv/bin/python <script>`。Docker 可用（留给 MinIO 延迟验证）。



### 4.1 采集口子（两条通用链路）


| 脚本                   | 作用                                                                                                                                                                                     |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `logging_fs.py`      | pyarrow `LoggingFileSystem` wrapper，进程内记录每次 `read(offset,length)`。**适用走 pyarrow 的负载（TPC-H / ML）**。关键坑：须 `use_threads=False` + `pre_buffer=False` 拿到未 coalesce 的原始 range，且避免 core dump。 |
| `strace_to_trace.py` | 在 syscall 层抓 `pread64` → trace CSV，**引擎无关**。用于 Rust 原生 IO 的 **Lance / 多模态**（不走 pyarrow）。处理多线程 unfinished/resumed 配对 + marker 打 query_id。                                               |




### 4.2 各负载流水线（生成数据 → 采 trace → 出热力图）


| 负载       | 生成                                                                                  | 驱动查询/负载                                                                                        | 端到端入口                        |
| -------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ---------------------------- |
| TPC-H 分析 | `generate_tpch.py`（DuckDB dbgen）                                                    | `run_tpch_full.py`（完整 22 查询，DuckDB←arrow-dataset 桥接挂 logging_fs）；`run_tpch_trace.py`（4 查询轻量冒烟） | `run_pipeline.sh`            |
| Lance 向量 | `make_lance_dataset.py`（合成/`--from-fvecs`/`--from-hdf5` + IVF_PQ）                   | `run_lance_queries.py`（ivfpq/flat × k，`--query-file` 真实 query）                                 | `run_lance_pipeline.sh`      |
| ML 训练    | `make_ml_dataset.py`；`make_movielens_embedding.py`（真实 Zipf 序列）                      | `run_ml_workload.py`（`epoch_scan` / `embedding_gather`，`--access-file`/`--zipf`）               | `run_ml_pipeline.sh`         |
| 多模态      | `make_multimodal_dataset.py`（Lance blob + 向量 + caption，`--blob-size-file` 接真实 size） | `run_multimodal_workload.py`（`train_load` / `retrieval`）                                       | `run_multimodal_pipeline.sh` |




### 4.3 分析与评测工具


| 脚本                            | 作用                                                                                                                                                                                                                                                           |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `lakehouse_access_heatmap.py` | 核心可视化：偏移直方图 / offset×time 热力图（LogNorm）/ 请求大小分布 / 热点对象 / 按对象分面 + `summary.txt`。吃统一 trace CSV（列 `timestamp,object_key,offset,length[,file_size,query_id,file_type]`）。                                                                                          |
| `prefetch_simulator.py`       | **trace-driven replay 评测台**（离线）。10 种策略（见 §6.2）+ `stat_selector`**（Track 1 学习式选择器，**`--model`**）**，成本模型 `read_latency≈RTT + bytes/BW`，输出读放大 / GET 数 / remote bytes / 浪费字节 / 缓存命中率 / P50/P95/P99 时延。含 `extract_features`（纯 IO 特征）与 O(log n) `BlockCache` 区间索引。 |
| `train_policy_selector.py`    | **Track 1 训练**：回放单负载 trace 抽特征、按 §6.2 最优策略打标签，训练决策树(主)/SVM(对照)，产出 `models/policy_selector.joblib` + report。                                                                                                                                                  |
| `eval_track1.py`              | **Track 1 评测**：四类单负载 + mixed trace 上对比 `stat_selector` vs `template_auto` vs per-workload best vs baseline，产出 `access_report/track1_eval.json` + 收敛图。                                                                                                        |
| `data_io.py`                  | 真实数据 loader：`.fvecs/.ivecs`(SIFT)、ann-benchmarks HDF5、`.npy`。                                                                                                                                                                                                |




### 4.4 文档 / 报告

- `project_notes.md`：完整逐日纪要（权威细节，§19/§20 是最新路线）。
- `阶段汇报_访问热力图.md`：四类负载热力图汇报。
- `阶段汇报_现有实现系统测试.md`：AWS SDK / S3A 系统 baseline 测试。
- `阶段汇报_访问策略对比.md`：replay simulator 10 策略 × 7 trace 评测。
- `FIGURES.md`：5 张热力图坐标系解读。
- `prefetch-policy-eval.canvas.tsx`：策略评测交互视图（Cursor Canvas）。
- `README.md`：复现说明 + 脚本职责表。

---



## 5. 数据集（`data/`，均已在盘）


| 目录                                 | 负载            | 说明                                                                 |
| ---------------------------------- | ------------- | ------------------------------------------------------------------ |
| `data/tpch/sf1/`                   | TPC-H         | DuckDB dbgen 官方生成（**本就是真实数据**）；lineitem 为主力（49 row groups × 16 列）。 |
| `data/ANN_SIFT1M/sift/`            | Lance 向量      | 真实 SIFT1M（base 1M×128 + query 1万 + groundtruth），**锚点验证通过**。        |
| `data/fashion-mnist/`              | Lance 向量      | 真实 fashion-mnist-784（train 6万 + test 1万自带近邻），第二锚点。                 |
| `data/lance/`                      | Lance 向量      | 合成向量集（1M×128，SIFT-like）。                                           |
| `data/ml/`                         | ML 训练         | 合成特征表 Parquet。                                                     |
| `data/ml-1m/`                      | ML embedding  | MovieLens-1M（真实 Zipf 访问序列，Top-10% 热门占 44.4%）。                      |
| `data/nyc_yellow_taxi_dataset/`    | ML epoch_scan | NYC taxi（10 月真实 tabular，少而巨大 row group）。                           |
| `data/raw/`, `data/multimodal(合成)` | 多模态           | blob size 默认 lognormal 长尾，`--blob-size-file` 可接真实 COCO/LAION size。 |


trace 产物在 `traces/`，热力图报告在 `access_report/`（各负载一个子目录 + `summary.txt`）。

**合成可信度**：访问的 offset/length/时序由"格式 + reader + 索引/查询结构"决定，与数值无关 →
合成可泛化；**唯一例外是"数值决定读哪里"的场景**（embedding 的 Zipf 偏斜），已用 MovieLens
真实序列覆盖。Lance 已用 SIFT1M + fashion-mnist 两个真实锚点验证形态一致。

---



## 6. 测试结果（关键素材）



### 6.1 四类负载访问模式对照（热力图结论，bandit/统计模型的 context 特征空间）


| 负载       | 典型 req_size                 | 顺序性 | 时间局部性                 | 字段异构度        | 预取/缓存要点                 |
| -------- | --------------------------- | --- | --------------------- | ------------ | ----------------------- |
| TPC-H 分析 | MB 级大 chunk                 | 高   | 无                     | 同构           | 大块顺序预取（列 coalesce）      |
| Lance 向量 | ~512B 海量随机                  | 低   | 弱                     | 同构           | coalesce 小读为大 GET（收益最高） |
| ML 训练    | KB(embedding) / 数百 KB(scan) | 中   | **强**（多 epoch / Zipf） | 同构           | 行级 LRU / 块级 buffer      |
| 多模态      | **双峰 2KB + 190KB**          | 混合  | 检索弱/训练中               | **异构（跨数量级）** | 按命中字段自适应粒度              |


- ML 内部还需再分：`embedding_gather`（行级随机 + Zipf，**IO 层抹平偏斜 → 须行级 LRU**）
vs `epoch_scan`（块级顺序 + 多 epoch 重复 → **块级 prefetch/buffer**）。判据 = `batch_size / row_group_count`。
- MovieLens LRU 实测：25% 缓存容量下真实 Zipf 命中 57.9% vs 均匀基线 25% → **均匀随机严重低估缓存价值**。



### 6.2 Replay Simulator 策略评测（`prefetch_simulator.py`，7 条 trace）

策略：5 系统 baseline（`aws_range_get / s3a_random / s3a_seq / s3a_normal / s3a_prefetch`）

- 5 规则模板（`template_seq / template_small_random / template_locality / template_multimodal / template_auto`）。
成本模型：RTT=50ms、BW=100MiB/s、cache_budget=256MiB。

**每类负载最优策略（印证"无通用最优"）**：


| 负载                  | 最优策略                   | 关键优势                                    |
| ------------------- | ---------------------- | --------------------------------------- |
| TPC-H 分析            | `s3a_random`           | 读放大 0.16x，waste 仅 6MB；后向 seek 抑制大窗口     |
| ML taxi epoch_scan  | `template_locality`    | remote 253MB（-75% vs aws），多 epoch 局部性命中 |
| Lance SIFT1M（大文件）   | `template_locality`    | GET 最少，时延最低；工作集>预算时退化最优雅                |
| Lance fmnist（小文件）   | `s3a_prefetch`         | 读放大 0.04x，26 次 GET（工作集<预算）              |
| ML embedding_gather | `s3a_prefetch`（任意缓存均可） | 67MB 工作集远小于预算，1 次 GET 即全缓存              |
| 多模态检索               | `template_multimodal`  | remote 428MB，waste 1MB；两段式分流最干净         |


**三条核心结论**：

1. **没有通用最优策略**，最优随负载类型翻转 → 自适应选策略成立。
2. **规则模板在单一负载上已接近最优**，bandit 的增量价值主要在**混合负载/负载切换**（待验证）。
3. `s3a_prefetch` **成败由工作集/预算比决定**：小于预算接近最优（0.04x），超出即灾难（68~94x）。

**里程碑状态**：M1 评测台可用 ✅；M2 规则自动策略(`template_auto`)基本正确 ✅（负载边界有 1.1–1.4x 小误判）；
M3（统计模型在 mixed trace 上是否更优）**已验证 ✅**：默认决策树选择器（v3）在 mixed 上相对
`template_auto` **时延 −7.8%、远端字节 −3.5%**（读放大 1.03 vs 1.07），切换尖峰从首版 291x 降至 12.7x。
→ **Path 2 目标达成：决策树可优化到优于手写自适应**。详见 `阶段汇报_Track1统计策略选择器.md`。

---



## 7. 下一步 —— Track 1 从离线 POC 迁移到 AWS SDK（新工作区交接计划）



### 7.1 目标与边界

- **目标 SDK**：AWS SDK for Java v2，基线版本 **v2.25.70**（与
`阶段汇报_现有实现系统测试.md` 保持一致）；在新工作区 fork/clone
`aws/aws-sdk-java-v2`，从对应 tag/commit 建开发分支。
- **目标形态**：在 SDK 仓库内新增一个 opt-in 的 `AdaptiveRangeReader`（或独立实验模块），位于
`S3AsyncClient/CRT S3` 之上，为应用提供 `read(offset,length)` / `seek` 语义；不要直接改变现有
`S3Client.getObject` 的通用行为，避免破坏 SDK API、重试及兼容性。
- **POC 判据**：真实实现中决策树相对手写 `template_auto` 在至少一组标准化 mixed workload 上实现
**端到端时延与远端字节同时不劣**；功能关闭时行为与原 SDK 一致。
- **暂不做**：不继续针对本仓库非标准 tpch 等单负载调参；不引入 bandit；不做 Track 2；不直接把
Python/sklearn 运行时带入 Java SDK。



### 7.2 重要技术约束（新 agent 必须先读）

1. **不能直接嵌入** `.joblib`：`models/policy_selector.joblib` 是 Python/sklearn 序列化对象。
  应新增导出脚本，将树导出成稳定、可审计的 JSON（节点、阈值、左右子树、叶子策略、特征 schema/version），
   Java 侧实现无依赖推理器；也可在模型稳定后生成 Java 常量数组。
2. **必须保持特征完全一致**：Java 端复刻 `prefetch_simulator.py::extract_features` 的 10 维特征、
  64-read 滚动窗口、数值边界和缺失值规则；用 golden vectors 做 Python↔Java 一致性测试。
3. **模型只负责选策略，执行层另行实现**：模拟器标签不能原样调用 S3A：
  - `s3a_random` → 精确/小窗口 Range GET；
  - `s3a_prefetch` → 分块异步预取 + 有界 block cache；
  - `template_locality` → 检测复访后扩大页并缓存；
  - `template_multimodal` → 小请求页对齐、大请求 demand read。
4. **安全约束先于性能**：总内存、in-flight bytes、并发 GET 均设硬上限；模型异常/未知标签/预算不足时
  回退 demand read；功能默认关闭；对象缓存 key 必须包含 bucket/key + VersionId 或 ETag。
5. **指标内建**：至少记录 logical/remote bytes、GET count、read amplification、cache/prefetch hit、
  wasted/cancelled bytes、P50/P95/P99 logical latency、当前策略与切换次数。
6. 一句话总结：**模型离线训练、SDK 在线推理；模块由用户主动启用；策略在客户端模块执行，S3 只负责响应 Range GET**。



### 7.3 分阶段实施计划



#### S0：建立 SDK 工作区与交接资产（0.5–1 天）

1. clone/fork `aws/aws-sdk-java-v2`，checkout **2.25.70**，记录 upstream commit；确认 JDK/Maven/Gradle
  构建和 S3 模块测试可运行。
2. 在新仓库建立 `docs/adaptive-range-reader/`，复制本文件和
  `阶段汇报_Track1统计策略选择器.md`；记录本 POC 仓库路径/commit。
3. 从本仓库复制（不要重新训练）：
  - `models/policy_selector.joblib`、`models/policy_selector_report.json`（模型来源/审计）；
  - `access_report/track1_eval.json`（离线 baseline）；
  - `prefetch_simulator.py`、`train_policy_selector.py`（特征与训练参考）。
4. 先写一页 ADR：选择“SDK 内实验模块”还是“独立 wrapper module”。默认推荐**独立、opt-in 模块**，
  等 API 和收益稳定后再讨论并入 SDK core。



#### S1：模型导出与 Java 推理一致性（1–2 天）

1. 本仓库侧新增 `export_policy_tree.py`，输出 `policy_selector_v1.json` + schema/version/checksum。
2. Java 侧实现 `FeatureWindow`、`PolicyFeatures`、`DecisionTreePolicySelector`、3 次一致的 hysteresis。
3. 生成不少于 1000 个 golden feature/prediction 样本；Java 单测要求 10 维特征在容差内一致且
  **预测标签 100% 一致**。
4. 微基准验证：单次特征更新 + 推理达到 µs 级，无网络路径对象分配热点。



#### S2：先实现同步可 seek Range Reader（2–4 天）

1. 定义最小 API：bucket/key/version、`read(position, ByteBuffer)`、close/cancel；维护每对象/每 AppID
  的访问历史。
2. 实现 demand Range GET、64–256KiB 页 cache、区间命中、LRU 和对象版本隔离。
3. 先落地 `s3a_random` / `template_locality` / `template_multimodal` 三种低风险执行器；
  单元测试覆盖 EOF、重叠/并发 read、后向 seek、对象更新、异常重试和关闭资源。
4. 与原生 Range GET 做功能等价测试；feature flag 关闭时必须直通原 SDK。



#### S3：实现异步预取与统一预算（2–4 天）

1. 基于 `S3AsyncClient`（必要时再评估 CRT）实现分块预取、in-flight 去重、seek 后取消、失败回退。
2. 将 cache + response buffer + speculative prefetch 纳入**进程级统一预算**；优先级：
  demand > 热点 cache > 下一个顺序块 > 远端推测块。
3. 限制并发 GET / in-flight bytes；默认保守参数从 block 1–4MiB、depth 1 起测，不照搬模拟器
  8MiB×8。
4. 接入决策树四类标签；每次策略切换和预测置信状态均可观测。



#### S4：端到端验证与 POC 验收（2–3 天）

1. 先用本地 MinIO/可注入 20–100ms RTT 的代理做可重复测试，再做同 region AWS 小规模验证。
2. 固定比较四组：原生 AWS Range GET、`template_auto` Java 规则版、固定策略 best、决策树版。
3. 使用独立的标准化 holdout/mixed workload；训练样本与验收 trace 隔离，至少跑 5 次并报告
  median、P95/P99、remote bytes、GET、内存峰值和 CPU。
4. 验收条件：
  - 正确性测试全部通过，关闭开关零行为变化；
  - 决策树相对 `template_auto` 时延与 remote bytes 均不劣（建议容差 2%）；
  - 无失控预取：预算上限不可突破，异常时可回退；
  - 形成 benchmark 报告，再决定是否进入生产化/上游 PR。



### 7.4 新工作区首个提示词（可直接交给后续 agent）

> 阅读 `docs/adaptive-range-reader/PROJECT2.md` §7 和阶段报告。当前目标是在 AWS SDK for Java
> v2.25.70 fork 中完成 Track 1 SDK POC。先执行 S0/S1：确认构建与模块边界，写 ADR；将 sklearn
> 决策树导出为 versioned JSON，实现 Java 特征窗口和无依赖推理器，并用 Python golden vectors
> 做 100% 一致性测试。不要直接修改 `S3Client.getObject` 默认语义，不要先做异步预取。



### 7.5 后续（Track 1 SDK POC 完成后）

- SDK POC 达标后再决定：独立库发布、并入 SDK 实验模块、或向上游提案。
- Track 2（布局建议）与 F3 论文任务保持待办，不与本次 SDK 迁移混做。

---



## 8. 快速上手（后续 agent）

- 想了解**为什么这么做**：读本文件 §1–§3 + `project_notes.md` §19。
- 想看**访问模式素材**：`access_report/*/summary.txt` + `FIGURES.md` + `阶段汇报_访问热力图.md`。
- 想看**策略评测**：`阶段汇报_访问策略对比.md` + `prefetch_simulator.py`。
- 想**跑复现**：`README.md` + 各 `run_*_pipeline.sh`（相对模式与规模无关，小规模即可）。
- 想看 **Track 1 Path 2 POC**：`阶段汇报_Track1统计策略选择器.md`；跑复现
`./.venv/bin/python train_policy_selector.py` → `./.venv/bin/python eval_track1.py`
（默认即 v3 模型与评测结果）。
- **当前该动手的第一件事**：关闭本工作区前保存/提交资产；在新工作区 clone
`aws/aws-sdk-java-v2` v2.25.70，按 §7 的 **S0→S1** 建立独立实验模块、模型 JSON 导出和
Python↔Java golden 一致性测试。先不要直接改 `S3Client.getObject` 或实现异步预取。



## 9. 更新要求

- 每次对项目产生修改后都要在该文档中进行记录



### 更新日志

- **2026-07-17**：新增 §7“Track 1 从离线 POC 迁移到 AWS SDK”交接计划。目标仓库为
AWS SDK for Java v2.25.70；明确采用 opt-in `AdaptiveRangeReader`、模型 JSON 导出、
Java 无依赖推理、同步 Range Reader→异步预取→统一预算→MinIO/AWS 验收的分阶段路线，
并给出新工作区首个提示词。
- **2026-07-17**：将训练修复版 v3 提升为默认模型（`models/policy_selector.joblib`；
`max_per_trace=15000, max_depth=12, min_samples_leaf=10`）。mixed 上相对 `template_auto`
**时延 −7.8%、远端字节 −3.5%**；切换尖峰 12.7x。Path 2 判定：决策树可优于手写自适应。
非标准测试集上的单负载回归（如 tpch）暂不处理。评测默认产物同步为 v3。
- **2026-07-16**：完成 Track 1 首版 POC（Path 2 离线）。新增 `stat_selector`、
`train_policy_selector.py`、`eval_track1.py`、`阶段汇报_Track1统计策略选择器.md`；
`BlockCache.find_covering` 改 O(log n)。首版 v1：mixed 时延 −5.8%，但远端字节 +77.8% +
切换点 291x（后由 v3 修复）。

