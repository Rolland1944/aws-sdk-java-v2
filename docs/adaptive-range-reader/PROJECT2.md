# 湖仓 S3 自适应 IO —— 项目总览（PROJECT2）

> ⚠ **本文件已被 `PROJECT3.md` 接续（2026-07-30 研讨会）**：项目动作空间已从「在四条策略间做选择」
> 更换为「五个访存优化维度上的参数化决策」，实验平台迁往真实 AWS S3 / 腾讯云 COS 的百 GB 级数据。
> **本文件不再更新，作为证据库与资产索引继续引用**；当前路线一律以 `PROJECT3.md` 为准。
> 各项结论的留用/降级/作废处置见 `PROJECT3.md` §1.3。

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
5. **访问模式随负载类型区分、随规模线性缩放**：算法开发用小规模即可，只有最终延迟验证才上真实 AWS S3。

---



## 4. 已有代码与工具

> 运行环境：`./.venv`（Python 3.11.9，`duckdb / pyarrow / pandas / numpy / matplotlib / lance / h5py`）。
> 统一用 `./.venv/bin/python <script>`。最终延迟验证使用真实 AWS S3，见 §8.8。



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
| `build_mixed_trace.py`        | 生成 5 类均衡、保结构的综合 holdout trace → `traces/mixed_holdout.csv`（见 §8.2）。                                                                                                                                                                                          |
| `label_flip_check.py`         | **标签口径 / 预算翻转检查**：逐负载 × 四个可执行标签 × 多预算跑 replay，看最优策略是否随预算翻转（重训的前置判据）。产出 `access_report/label_flip_check.json`（见 §8.5）。                                                                                                          |
| `policy_agreement.py`         | **oracle 命中率诊断**：逐读比较手写规则 / 决策树的**选择**与 §6.2 最优策略是否一致（不跑 IO），in-sample 与 held-out 分开，回答「换模型还有多少 headroom」。产出 `access_report/policy_agreement.json`（见 §8.3）。                                                                                                    |
| `data_io.py`                  | 真实数据 loader：`.fvecs/.ivecs`(SIFT)、ann-benchmarks HDF5、`.npy`。                                                                                                                                                                                                |




### 4.4 文档 / 报告

- `project_notes.md`：完整逐日纪要（权威细节，§19/§20 是最新路线）。
- `阶段汇报_访问热力图.md`：四类负载热力图汇报。
- `阶段汇报_现有实现系统测试.md`：AWS SDK / S3A 系统 baseline 测试。
- `阶段汇报_访问策略对比.md`：replay simulator 10 策略 × 7 trace 评测。
- `阶段汇报_S2S3系统实验与有效性边界.md`：**SDK 落地后的系统实验汇报**——区分有效果（缓存+策略路由 RTT 下 −23.7%、
  学习式随预算放大到 −16.2%）、无效果（异步预取 +1.6%、滞后影响为 0）与四处评测口径缺陷；含实验产物索引与复现命令。
- `results/`：上述汇报引用的**已固化实验产物**（原在 `target/`，会被 `mvn clean` 清空，故复制留存）：
  `benchmark_results.csv`（预算扫描 20 行）、`report-rtt-*.txt`、`report-sweep-*.txt`、`policy_agreement.json`、
  `label_flip_check.json/.log`、`mixed_holdout.summary.txt`。
- `access_report/sdk_sweep_bytes.csv`、`sdk_sweep_oracles.json`、`sdk_sweep_summary.csv`：已完成的 128–1024MiB
  SDK bytes / cost proxy sweep；真实 latency sweep `sdk_sweep_latency.csv` 尚未生成，完成后须固化到 `results/`。
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



## 7. Track 1 SDK 实施计划、完成状态与 S4 路线



### 7.1 目标与边界

- **目标 SDK**：AWS SDK for Java v2，基线版本 **v2.25.70**（与
`阶段汇报_现有实现系统测试.md` 保持一致）；在新工作区 fork/clone
`aws/aws-sdk-java-v2`，从对应 tag/commit 建开发分支。
- **目标形态**：在 SDK 仓库内新增一个 opt-in 的 `AdaptiveRangeReader`（或独立实验模块），位于
`S3AsyncClient/CRT S3` 之上，为应用提供 `read(offset,length)` / `seek` 语义；不要直接改变现有
`S3Client.getObject` 的通用行为，避免破坏 SDK API、重试及兼容性。
- **POC 判据（2026-07-30 修订）**：在锁定的 `(cache budget, block, depth, network)` 工况下，以真实 AWS S3
  的端到端 wall-clock / P50/P95/P99 为**主目标**，GET 数为 RTT 主导时的辅助解释指标，remote bytes / read
  amplification 为必须同时报告的代价指标。决策树相对 `template_auto` 的主目标不得劣于 2%，且必须优于
  passthrough；功能关闭时行为与原 SDK 一致。不得再要求“时延与远端字节同时不劣”——§8.7 已证明二者可能给出相反排序。
- **暂不做**：不继续针对本仓库非标准 tpch 等单负载调参；不引入 bandit；不做 Track 2；不直接把
Python/sklearn 运行时带入 Java SDK。



### 7.2 重要技术约束（新 agent 必须先读）

1. **不能直接嵌入** `.joblib`：`models/policy_selector.joblib` 是 Python/sklearn 序列化对象。
  应新增导出脚本，将树导出成稳定、可审计的 JSON（节点、阈值、左右子树、叶子策略、特征 schema/version），
   Java 侧实现无依赖推理器；也可在模型稳定后生成 Java 常量数组。
2. **必须保持特征完全一致**：Java 端复刻 `prefetch_simulator.py::extract_features` 的 10 维特征、
  64-read 滚动窗口、数值边界和缺失值规则；用 golden vectors 做 Python↔Java 一致性测试。
  - **[已知差异 · 换模型前必读] 滚动窗口的作用域不同（S2 现状）**：训练与 S1 golden 使用的是
    `ReplayState.history`——一个**跨对象的全局 64 读窗口**（一次 trace 段里多个对象的读混在同一窗口）。
    但 S2 的 `AdaptiveRangeReader` 是**按单对象**实现的：每个 reader 实例持有各自的 `FeatureWindow`，
    窗口里只含**同一个对象**的读。逐读的原始字段（key/offset/length/fileSize）与特征公式仍与训练 1:1
    一致（`FeatureWindow.featuresFor` 已逐行核对、golden 100% 通过），**但窗口内容的分布不同**，导致：
    `distinct_obj_ratio`（第 9 维）在 S2 恒约等于 `1/n`（只有一个对象）、`sequentiality/forward_ratio/gap`
    的“同对象过滤”恒真、跨对象交错消失。**不影响正确性**（返回字节永远正确），但会让在线特征分布偏离训练分布，
    从而可能选到与训练时不同的策略。**换模型 / 重训时务必对齐这一口径**：要么训练侧也改成按对象窗口，
    要么把 SDK 侧改成跨对象的 session 级共享 window。
    - **✅ 已在 S3 路径消除（2026-07-23）**：`AdaptiveReaderRuntime`/`AppContext` 把 selector 提升为
      **per-app 跨对象共享**（该 app 所有 reader/所有对象喂进同一 64 读窗口，`LockingPolicySelector` 串行化），
      恢复训练一致的跨对象口径。**S2 同步 reader 仍是按对象窗口**（该已知差异只对 S2 路径成立）。
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

#### 当前阶段状态（2026-07-30）

| 阶段 / 工件 | 状态 | 第三阶段（S4）门禁 |
| --- | --- | --- |
| S0 工作区与交接资产 | ✅ 模块、模型、golden 已迁入；ADR / provenance 仍待补录 | 否 |
| S1 模型导出与 Java 推理 | ✅ 完成 | 否 |
| S2 同步 reader 与四策略执行器 | ✅ 完成 | 否 |
| S3 异步预取、统一预算、appID 隔离 | ✅ 完成 | 否 |
| SDK bytes / cost proxy sweep（128–1024MiB） | ✅ 完成，见 §8.6–§8.7.1 | 否 |
| SDK latency sweep（`sdk_sweep_latency.csv`） | ⏳ 未执行 | 是 |
| 真实 AWS S3 provision / replay | ⏳ 未执行 | 是 |
| S4 POC 签收 | ⏳ 未开始 | — |



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

> **现状（2026-07-23 完成，含 appID 环境隔离扩展）**：见 §9 更新日志 2026-07-23 条。要点：
> - **异步 IO SPI**：`AsyncObjectStore`（`internal.io`）+ `S3AsyncClientObjectStore`（唯一接触 `S3AsyncClient`，
>   `AsyncResponseTransformer.toBytes()` + `If-Match`/412→`ObjectChangedException`）+ 测试双件 `InMemoryAsyncObjectStore`
>   （可控延迟/失败/manual 完成/并发计数）。
> - **预取**：`Prefetcher`（per-reader，in-flight 去重、seek 取消、失败回退），策略脑 `PolicyPlanner`（4 个 planner，
>   `prefetch`/`locality` 支持 depth≥1 的块 lookahead，默认 block 1MiB、depth 1，不照搬 8MiB×8）。**S2 执行器改为委托同一
>   planner**，行为不变。
> - **work-conserving 统一预算**：`GlobalBudget`（全局硬上限 + 每 app 预留 + 借用/按需回收，回收只淘汰他 app 超预留的
>   借用块，保证任一 app 不被他人挤到预留以下）；`AppBudgetLease`、全局 `ConcurrencyLimiter`（并发 GET 上限）、每 app
>   `InflightLimiter`（推测预取 in-flight 字节上限）。
> - **appID 环境隔离**：`AdaptiveReaderRuntime`（进程级，持全局预算/并发/共享模型）→ `register(engineName)` 分配
>   appID → 返回 `AppContext`（独占 per-app 多对象 `AppCache`、预算 lease、in-flight 限额、**per-app 跨对象共享且线程安全的
>   selector**、reader 工厂）。同一 JVM 内 flink/spark 各拿独立环境，cache/预算/并发/指标互不串。
> - **采集口径修正**：selector（`FeatureWindow`+`Hysteresis`）由 S2 的“每 reader/每对象”提升为 **per-app 跨对象共享**
>   （`LockingPolicySelector` 串行化 `onRead`），恢复训练一致的跨对象口径——**§7.2 的“窗口作用域”已知差异在 S3 路径被消除**。
> - **指标**：`ReaderMetrics` 增 prefetch GETs/bytes/useful/wasted/cancelled；新增 `AppMetrics`/`RuntimeMetrics`（每 app
>   预留/用量/块数/in-flight 峰值，证明隔离）。
> - **验证**：`PrefetcherTest`（去重/取消/失败回退/超限跳过）、`GlobalBudgetTest`（预留保护/借用回收/硬上限/释放）、
>   `PrefetchingReaderTest`（混合模式字节正确/depth=0 退化/seek/ObjectChanged）、`AppIsolationTest`（预留被尊重且无跨 app
>   淘汰/特征窗口分离/并发 GET 封顶）、`CrossObjectSelectorTest`（跨对象共享 selector + 并发线程安全）、
>   `MultiAppTraceReplayTest`（双 app 回放 tpch）。`mvn -pl … -Djapicmp.skip=true verify` 全绿。
> - **flag off / depth=0** 仍等价 S2 语义（无投机预取；但 cache 为 per-app 多对象、selector 为 per-app 跨对象）。



#### S4：真实 AWS S3 端到端验证与 POC 验收（当前阶段）

1. 先完成 §8.6 的 latency sweep，书面冻结验收工况 `(budget, block, depth, objective)`；当前仅有零 RTT
   字节/成本代理结果，不能据此签收。
2. 在同 region 的真实 AWS S3 上回放独立 `mixed_holdout`，固定比较：passthrough、`template_auto`、
   decision tree（主路径 depth=0）、同工况的 `oracle_perworkload[cost]`；depth≥1 仅作预取对照。
3. 使用同一批不可变对象、相同 trace 与 JVM 参数；每个配置运行至少 5 次，单独保存每次 `results-v2.csv`
   行，报告 median、P50/P95/P99、GET、remote bytes、预算峰值与 CPU。
4. 验收条件：
   - 正确性测试全部通过，关闭开关零行为变化；
   - 决策树相对 `template_auto` 的 wall-clock 不劣于 2%，且优于 passthrough；
   - 无失控预取：预算上限不可突破，`fallbackReads=0`，激进实验的 `demandClampReads=0`；
   - 归档 AWS S3 环境元数据、原始结果和 S4 报告，再决定是否进入重训 / 动态 oracle / bandit 路线。



### 7.4 当前阶段交接提示词（可直接交给后续 agent）

> 阅读 `docs/adaptive-range-reader/PROJECT2.md` §7、§8.6–§8.7.1 和阶段报告。S0–S3 已完成；当前是
> S4：先完成 latency sweep，再冻结验收工况并在真实 AWS S3 上回放 mixed holdout。不要重做 S1/S2/S3，
> 不要仅用远端字节给模型打分，也不要在未测 dynamic oracle 前直接引入 bandit/RL。



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
- **当前该动手的第一件事**：按 §7 S4 完成 `run_sdk_sweep.py latency`，冻结验收工况后在真实 AWS S3 上
  回放 `mixed_holdout`；所有结果从 `target/` 复制到 `docs/adaptive-range-reader/results/` 再做结论。

### 8.1 S2/S3 系统基准（手动跑，不进 `mvn verify`）

类名 `AdaptiveReaderSystemBenchmark`（**不带 `Test` 后缀**，Surefire 默认不会跑）。对比**四列**，
后三列跑在**同一套 `AdaptiveReaderRuntime`**（同 appID 组织、同 work-conserving 全局预算按 apps 均分、同 4 个执行器），
**只差"选择器大脑"与预取深度**——所以是**公平预算**对比：

| 列 | 含义 |
| --- | --- |
| `passthrough(off)` | 原生精确 demand Range GET，无选策略/无缓存（下界基线；读放大恒 1.0，GET=逻辑读数） |
| `template_auto(d0)` | **手写规则基线**：`RuleBasedPolicySelector` 按 IO 形状阈值路由到同 4 执行器，无预取 |
| `S2(learn,d0)` | **学习式**：决策树选策略 + per-app 多对象 cache，无预取 |
| `S3(learn,dN)` | 在 S2 之上加异步预取（depth=N），额外打印 prefetch useful/wasted/cancel |

报告尾部打印两组关键增量：`S2(learn) vs template_auto`（学习 vs 手写规则）、`S2 vs passthrough`（缓存效果）、
`S3 vs S2`（纯预取效果），以及每 app `RuntimeMetrics` 隔离明细。

> **公平预算说明**：早期 S2 用的是"每对象各一份 `PageCache`"，与 S3 的"全 app 共享预算"不可比。现基准的 S2/S3/template_auto
> 一律走 runtime、共用同一全局预算（每 app 预留 = 全局/apps），差异被收敛为"大脑 + 预取"两项。`AdaptiveRangeReaderImpl`
> （旧的每对象同步 S2）仍保留于生产代码，但基准不再用它。
>
> **selector 模式为 public API**：`AdaptiveReaderRuntime.builder().selectorMode(SelectorMode.DECISION_TREE|TEMPLATE_AUTO)`，
> 可在真实部署里切换"学习式/手写规则"，不止基准可用。

在仓库根目录执行：

```bash
export PATH="$HOME/apache-maven-3.9.16/bin:$PATH"   # 按本机 Maven 路径调整
cd /path/to/aws-sdk-java-v2
mvn -pl services-custom/s3-adaptive-range-reader \
    -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
    -Dtest=AdaptiveReaderSystemBenchmark test
```

可选系统属性：

| 属性 | 默认 | 说明 |
| --- | --- | --- |
| `-Ds3arr.trace=` | 向上查找 `traces/tpch_sf1_full.csv`，否则用 jar 内 `tpch_slice.csv` | 回放的 CSV trace（**注意：相对路径按模块目录解析，跑 `traces/` 建议用绝对路径**） |
| `-Ds3arr.label=` | `S2` | 写入结果的阶段标签 |
| `-Ds3arr.backend=` | `synthetic` | `synthetic` 使用确定性内存对象；`s3` 使用环境变量提供的真实 S3 endpoint/bucket/静态凭证，禁止叠加 `rttMs/bwMiBps` |
| `-Ds3arr.warmup=` / `-Ds3arr.iters=` | synthetic 时 `2` / `5`，真实 `s3` 时 `0` / `1` | 预热与计时迭代次数；代码选择最快一轮，S4 应单独重复命令并自行汇总 median |
| `-Ds3arr.cacheBudgetMiB=` | `64` | 全局缓存预算（每 app 预留 = 全局/apps，template_auto/S2/S3 同此预算） |
| `-Ds3arr.prefetchBlockMiB=` | `1` | `s3a_prefetch` demand 块大小；与 `prefetchDepth` 共同构成固定的激进度工作点 |
| `-Ds3arr.maxFetchMiB=` | 自动 | 可选的单次抓取上限；未设时自动取 `max(8MiB, 2×blockSize+trace最大请求)`，避免大块策略静默退化为精确读 |
| `-Ds3arr.apps=` | `1` | 隔离 app 数（trace 的 key 按 hash 分派到各 app，验证隔离；template_auto/S2/S3 同数） |
| `-Ds3arr.prefetchDepth=` | `1` | S3 推测预取深度（块数），`0` 等价 S2 |
| `-Ds3arr.forcePolicy=` | 无 | 只运行指定固定策略（`s3a_random` / `template_locality` / `s3a_prefetch` / `template_multimodal`），用于静态 oracle |
| `-Ds3arr.selector=` | 无 | 只运行 `passthrough` / `template_auto` / `decision_tree` 中的一列，且保留本次配置的 block/depth |
| `-Ds3arr.oracleMap=` | 无 | `prefix:policy` 逗号表；按 mixed trace 的裸 key 前缀（如 `tpch`）最长匹配。synthetic backend 自动补 `bench/`，真实 backend 自动补实际 bucket 前缀，用于可实现的逐负载 oracle |
| `-Ds3arr.rttMs=` | `0` | 每次 GET 的固定往返时延（ms），注入为**真实 sleep** |
| `-Ds3arr.bwMiBps=` | `0` | 传输带宽（MiB/s），每次 GET 额外耗 `bytes/BW`；`0`=不限（只算 RTT） |
| `-Ds3arr.thinkMs=` | `0` | 每读之间的合成"计算间隙"（ms，四模式同注），给异步预取一个重叠窗口——无它则紧凑回放永远掩盖不了 RTT |

`rttMs/bwMiBps/thinkMs` 三者构成**远端读成本模型**（`SimLatency`，形同 simulator 的 `RTT + bytes/BW`），注入后
`ns/op`、p50–p99 与新增的 **`IO latency sum (s)`** 才反映网络成本，预取掩盖 RTT 的收益才可测。三者留 0 即保持原先零-RTT
的快速「IO 形状」对比。开延迟时 `warmup/iters` 默认降为 `0/1`（每趟约 `reads×(rtt+think)` 长）。

用综合 holdout 跑（推荐，见 §8.2）：`-Ds3arr.trace=/abs/path/traces/mixed_holdout.csv -Ds3arr.apps=2`。
量**延迟维度**（预取到底有没有用）：加 `-Ds3arr.rttMs=5 -Ds3arr.bwMiBps=200 -Ds3arr.thinkMs=5`。

常规四列结果仍写入旧的 `results.csv`；新的 `results-v2.csv` 追加 all-scope 行与按 key 前缀分组的行，新增
`scope/prefix/demandClampReads`。`demandClampReads` 非零表示 planner 的 demand 范围曾超过单次抓取上限并被静默改为
精确读；激进块实验必须检查它为零，否则不能把该格解释为策略效果。

**位置**：

- 代码：`services-custom/s3-adaptive-range-reader/src/test/java/.../AdaptiveReaderSystemBenchmark.java`
- 输入数据：仓库 `traces/*.csv`（原地读）或 `-Ds3arr.trace=`
- 输出：`services-custom/s3-adaptive-range-reader/target/s3arr-benchmark/`
  （`report-<label>-*.txt` + `results.csv` + 追加写的 `results-v2.csv`）；stdout 也会打印完整报告

**如何读结果（有效性边界）**：

- **有效、应优先看**：logical 字节两端一致；`remote GETs` / `remote MiB` / `read amplification` /
  `cache hit rate` / 每策略读次数 / `fallback reads`（应为 0）。这些刻画「开 adaptive 后 IO 形状是否变省」，
  与是否真连 S3 无关（基准用合成 `GeneratedObjectStore`）。
- **仅在 synthetic backend 下作相对参考**：`ns/op`、p50/p95/p99 含本地字节物化成本、**不含 S3 网络 RTT**；
  adaptive 往往因缓存命中而更快，不代表线上绝对时延。真实 `backend=s3` 下这些指标包含端到端网络时延，
  是 S4 的主结果；绝对推理开销仍单看 `InferenceMicroBenchmarkTest`。
- **不能直接等同离线 simulator 数字**：simulator 成本模型（RTT/BW）与窗口作用域（跨对象全局 vs
  S2 按对象）不同；例如 TPC-H 上 simulator 报 `s3a_random` 读放大 0.16x，本基准读放大 ~0.5x 量级仍属合理，
  但不宜逐位对比。
- **预取的收益是延迟、不是字节**：零-RTT 下主指标是 GET/字节，正是预取吃亏的维度。现已可用 `-Ds3arr.rttMs/bwMiBps/thinkMs`
  注入远端读成本模型，用 `IO latency sum (s)` 直接量延迟维度（见 §8.1 属性表 & §9 2026-07-28 RTT 结论）。
  结论：**在 RTT 下真正的赢家是缓存（S2），不是 depth-1 预取**——mixed 上 S2 相对 passthrough IO 时延 −24%，
  而 `S3 vs S2` 仅 +1.6%（≈打平）。原因不是 RTT 掩盖机制坏了（预取确实在后台异步发出、不阻塞主线程），而是**推测命中率极低**
  （mixed 上预取 useful 仅 ~4%、epoch 上 0%）：depth-1「下一 1MiB 块」几乎不等于真正的下一次读。

### 8.2 综合 holdout trace（`traces/mixed_holdout.csv`）

只测 TPC-H 会误判（TPC-H 几乎全 `s3a_random`，触发不了预取/局部性）。`tools/build_mixed_trace.py` 生成
**5 类均衡、保结构**的综合 trace 用于更客观地评价 S2/S3：

- 组成（**分段拼接**，各段内保序，不交错）：tpch（tpch+clickbench）/ lance（fmnist+sift）/ ml_emb（emb_real+lastfm）
  各 ~2.4k；ml_epoch（taxi 整条 659）；multi_model（mm 整条 2785）——共 **11189 reads**。
- **保结构采样**：tpch/clickbench 按整 query 组保留；lance/ml_emb 按 64 块系统采样；epoch/mm 整条。**不随机丢行**
  （否则破坏 epoch 复访与顺序段）。
- **源前缀命名空间**：每源 key 加 `fmnist/`、`sift/` 等前缀，根治跨源 `_versions/...manifest` 撞名；`query_id` 改写为
  `<class>:<source>:<orig>` 便于分类汇总；时间戳段间单调化。
- 重跑：`python3 tools/build_mixed_trace.py`（可调 `--per-source`/`--block`）；成分见 `traces/mixed_holdout.summary.txt`。

### 8.3 Oracle 术语表与 simulator 标签命中率验证

本项目的 “oracle” 不是一个单一对象；以下命名必须严格区分，避免把标签命中率、SDK 上界和后续动态上界混为一谈：

| 名称 | 定义 | 用途 / 边界 |
| --- | --- | --- |
| `simulator_label` | §6.2 离线 simulator 为每条单负载 trace 给出的常量策略标签 | 训练历史模型与 `policy_agreement.py` 的参照；参数、缓存与 SDK 不完全一致，不是 SDK 性能上界 |
| `agreement_oracle` | `policy_agreement.py` 所用的 `simulator_label` | 93.1% 等数字仅表示标签一致率，不表示逐读 cost / wall-clock 最优 |
| `oracle_static[objective]` | 固定一个 policy，在同一 SDK `(budget, block, depth)` 下的最佳静态基线 | 用于度量“不做自适应”时的最佳表现；最小化问题中是基线，不称为绝对上界 |
| `oracle_perworkload[objective]` | 按 key 前缀固定路由、在同一共享 cache 上实测的 SDK 策略表 | 当前四策略动作空间下、完美负载识别器可实现的参照 |
| `dynamic_oracle`（待测） | 策略可随窗口、预算压力和 cache 状态变化的事后最优 | 只有它相对 `oracle_perworkload` 仍有明显空间时，bandit/RL 才有研究必要 |

回答「手写规则是不是已经足够准、换模型还有没有空间」。逐读比较各 selector 的**选择**与该负载 oracle 最优策略
（即 `agreement_oracle` / §6.2 `simulator_label`）是否一致，不执行任何 IO：

```bash
PYTHONPATH=tools .venv/bin/python tools/policy_agreement.py     # 产出 access_report/policy_agreement.json
```

四个被比较对象：`ta(java)`（=线上/基准用的 `RuleBasedPolicySelector`）、`ta(sim)`（模拟器原版，含
`query_id` 泄漏 + seen_pages 只在 locality 分支自增）、`tree+hyst`（部署形态）、`tree(raw)`（去滞后）。
in-sample（决策树训练用过的 6 条）与 held-out（clickbench / ml_lastfm_emb / mm_all / mixed_holdout）**分开报告**，
只信后者。

**移植保真度已交叉验证**：本工具在 mixed 上算出的 `ta(java)` 四策略分布 1006/1622/5129/3432 与 `tree+hyst` 的
3497/2237/2142/3313，与 Java 基准打印的计数**逐读完全一致**，故命中率数字可信。

读法边界：`agreement_oracle` 是**每条 trace 一个常量标签**，所以「命中率」= 「是否收敛到该负载标签」，不是「这一次读是否
路由最优」；且标签来自 simulator 口径（256MiB、不同块参数），未必等于 SDK 实现下的真最优。它是**模型诊断**，不是性能指标。

### 8.4 缓存预算扫描（决定性实验：策略选择的价值随预算变化）

**必读**：`-Ds3arr.cacheBudgetMiB` 不是一个普通调参，它决定**整个评测处于哪个工况**，进而决定策略选择这个杠杆有多强。
mixed_holdout 上零-RTT 扫描（`-Ds3arr.warmup=0 -Ds3arr.iters=1 -DargLine="-Xmx12g"`，大预算必须加堆）：

| 预算 | 读放大 pt / ta / S2 / S3 | S2 vs ta（远端字节） | 命中率 S2 |
| --- | --- | --- | --- |
| 8MiB | 1.000 / 1.429 / **1.444** / 1.770 | **+1.0%（学习式反而更差）** | 0.349 |
| 32MiB | 1.000 / 1.394 / **1.412** / 1.679 | **+1.3%（仍更差）** | 0.375 |
| 64MiB | 1.000 / 1.366 / **1.293** / 1.601 | −5.3% | 0.437 |
| 256MiB | 1.000 / 1.094 / **0.917** / 0.953 | **−16.2%** | 0.549 |
| 1024MiB | 1.000 / 0.716 / **0.610** / 0.632 | −14.8% | 0.608 |

三条结论：

1. **读放大在 256MiB 起跌破 1.0**（S2 0.917 → 1024MiB 0.610），即缓存开始实现**字节级复用**，印证 §6.2 的 0.04x/0.16x 档
   本就需要「工作集 < 预算」。64MiB 及以下全部 > 1.0：缓存只减 GET/RTT，字节反而多抓 29–44%。
2. **学习式相对手写的字节价值随预算显著、且在当前采样点非单调**：+1.0%（8MiB，更差）→ −5.3%（64MiB）→
   **−16.2%（256MiB）**→ −14.8%（1024MiB）。这只是零 RTT / 字节目标下的曲线；后续完整 SDK sweep
   已在 §8.6–§8.7.1 给出 128/256/512/1024MiB 的双目标结果，不能将 256MiB 机械设为所有部署的默认预算。
3. **工况错配仍是重要解释，但不是唯一结论**：历史 `simulator_label` 在 256MiB 条件下与 byte-argmin 高度重合，
   因此用 64MiB 的字节指标给它评分会失真；但 RTT 主导部署还必须以 `cost` / 真实 wall-clock 复核，见 §8.6。

→ 推论：评测必须显式报告工作集/预算比与 objective；默认预算由目标部署和 S4 latency/AWS S3 结果冻结，而非由历史标签反推。

### 8.5 标签口径与预算翻转检查（`tools/label_flip_check.py`）

回答「在 64MiB 重训决策树是否有意义」的**前置判据**：不重训，先看各负载的最优策略是否随预算翻转。
逐负载 × 四个可执行标签 × 预算 {64,256} 跑模拟器 replay：

```bash
PYTHONPATH=tools .venv/bin/python tools/label_flip_check.py --budgets 64 256   # → access_report/label_flip_check.json
```

**副产物（重要）：历史标签与「256MiB 下最小远端字节」高度相关，但不能据此把字节规定为唯一重标定目标。**
字节口径在 256MiB 的 argmin **5/6 精确复现** `simulator_label`；唯一不符的 tpch 差 1.6%（878.1 vs 892.0 MiB，
实质并列）。这说明历史 simulator 标签受工作集/预算比强烈影响；它并不推翻 §8.6 的发现：RTT 主导时，减少 GET 的
cost/wall-clock 可能比减少 bytes 更重要。

**后续重标定规则**：必须声明 `objective`。带宽受限时可用 bytes；RTT 主导时用 `cost`，最终以真实 AWS S3 的
wall-clock 标签为准。若两者给出不同策略表，应并列报告 trade-off，而不是把其中之一称为普遍“最优”。

跨预算翻转（64 → 256）：

| 负载 | §6.2 标签 | 字节口径 @64 | 字节口径 @256 | 64MiB 下沿用旧标签的代价（字节） |
| --- | --- | --- | --- | --- |
| lance_small | `s3a_prefetch` | `s3a_random` | `s3a_prefetch` ✓ | **+11073.7%**（925979 vs 8287 MiB，amp 228.6） |
| lance_large | `template_locality` | `template_multimodal` | `template_locality` ✓ | +40.0% |
| tpch | `s3a_random` | `template_multimodal` | `template_multimodal`（并列） | +1.6% |
| ml_epoch_scan / ml_embedding / multimodal | — | 不变 | ✓ | 0.0% |

三条结论：

1. **只有 lance 两类会翻**，6 个负载里 4 个标签与预算无关。所以「在 64MiB 重训」有实质内容，但**范围有限**。
2. **lance_small 翻得极其剧烈**：`s3a_prefetch` 在 64MiB 下读放大 228.6x、字节多 111 倍——正是 §6.2 结论 3
   「prefetch 成败由工作集/预算比决定，超出即灾难」的活案例。
3. **一个此前的猜测被证伪**：原以为 ml_embedding（工作集 67MB）会在 64MiB 翻转，实测**没翻**——它的远端字节只有
   0.9–1.3 MiB（amp 0.014–0.020），工作集远小于估计，64MiB 绰绰有余。

**⚠ 但重训未必能在 SDK 上兑现同等收益**：模拟器的 `s3a_prefetch` 是 8MiB×depth8，SDK 执行器是保守的
1MiB×depth1（§7.3 明确"不照搬"）。所以 SDK 在 64MiB 下读放大只有 1.293，**根本不会出现 228x 的灾难**——
灾难被参数掐住了。这意味着**模拟器标签所反映的策略代价与 SDK 执行器的实际代价并不对应**。
→ 因此更根本的修法不是"在 64MiB 重训"，而是**用 SDK 自己的执行器在目标预算下重新标定标签**（口径对齐），
否则学到的仍是"另一套执行器的最优"。



### 8.6 SDK 侧策略收益测量（bytes / cost proxy 已完成，真实 latency 待执行）

模拟器与 SDK 的 `s3a_prefetch` 参数此前不一致：模拟器默认 8MiB × depth8，SDK 默认 1MiB × depth1。因此不能将
模拟器标签直接当作 SDK 的策略 oracle，也不能把「depth1 无效」外推成全部异步预取无效。

本轮评测固定同一个工作点 `(budget, aggr)`，其中 `aggr=(prefetchBlockSize, prefetchDepth)`。激进度不能进入逐读动作
空间：它在 runtime/reader 创建时已固定，selector 只能选择四种 policy。预算取 **128/256/512/1024MiB**；激进度工作点取
`(1MiB,0)`、`(1MiB,1)`（当前默认）、`(4MiB,4)`、`(8MiB,8)`（模拟器等价）。所有运行使用 `apps=1` 的真实共享缓存。

**两个目标函数必须并列报告**（这是本轮最重要的方法论修正，详见 §8.7）：

- `bytes`：仅远端字节。
- `cost`：`remoteGets × RTT + remoteBytes / BW`，默认 RTT=50ms、BW=100MiB/s。

mixed holdout 平均读只有 226KB，在 100MiB/s 下传输 2.2ms，而 RTT 是 50ms，**RTT 项压倒字节项约 23 倍**。因此
「最小远端字节」优化的是次要项，两个目标会给出不同的 oracle 表与不同的策略排序。`cost` 由零 RTT 计数器换算，
每个 GET 记一次 RTT：在 `prefetchDepth=0` 下精确（无投机，所有 GET 都在关键路径上），在更高 depth 下对投机 GET
偏悲观（真实 reader 中它们会重叠）。它只用于**挑选** oracle 表，注入真实 sleep 的 latency 阶段才是最终判据。

三个量全部在同一个 `(budget, aggr, objective)` 下定义：

- `amp_floor=0.566`：mixed holdout 64KiB 粒度工作集 1398MiB / 2468MiB 逻辑字节；仅表示无限缓存 + 精确读的字节地板。
- `oracle_static`：整条 trace 锁定一个 policy，四种固定策略中该目标下最优者；这是完全不自适应的**最佳静态基线**。
- `oracle_perworkload`：先按 8 个 key 前缀（tpch/clickbench/fmnist/sift/emb_real/lastfm/taxi/mm）从固定策略成绩取 argmin，
  再用该前缀→策略表在**同一共享缓存**上实际回放。它是完美负载识别器可以达到的构造性、可实现上界；若受缓存互扰影响而低于
  `oracle_static`，脚本会自动触发一次真实共享缓存的逐前缀坐标下降（逐前缀试四个策略）后重放。

`oracle_perworkload - oracle_static` 是当前“按前缀固定路由”动作空间的收益；实际 selector 到
`oracle_perworkload` 的差距才是该模型/训练可以补的空间。它不是 future `dynamic_oracle` 的总上界。
结果写入 `access_report/sdk_sweep_bytes.csv`（含 `costSeconds` 列）、前缀策略表写入
`access_report/sdk_sweep_oracles.json`（按 objective 分层），三层界汇总写入 `access_report/sdk_sweep_summary.csv`
并在运行结束时打印。

人工运行：

```bash
# 零 RTT 阶段的字节/GET/amp 全部是确定性的，多次迭代只重复制造同一个 IO 形状，
# 所以固定用 warmup=0 iters=1；(8,8) 强制预取单次就要搬 206GB，默认 7 遍会拖到小时级。
python3 tools/run_sdk_sweep.py bytes --warmup 0 --iters 1
# bytes/cost proxy 已完成；下一个门禁是实际延迟收窄复测
python3 tools/run_sdk_sweep.py latency
```

重跑前必须把 `target/s3arr-benchmark/results-v2.csv` 移走：该文件是累加的，而 label 会复用，同名 label 残留会让
驱动脚本读到两行 all-scope 行而报错。

RTT 阶段在每个预算测试 `cost` 目标下最优的激进度与当前 `(1MiB,1)`，各比较静态最佳/最差、**两个目标各自的**逐负载
oracle（表不同时都回放，表相同时复用一次测量）、手写规则、决策树，结果写入 `access_report/sdk_sweep_latency.csv`。
不在实现阶段自动运行，避免把十分钟级的人工系统实验混入普通测试。

结果判定：

1. 两个 oracle 始终接近：自适应天花板低，应扩展动作空间，不应优先重训。
2. oracle 差距显著且现有 selector 离逐负载 oracle 很远：按该 `(budget, aggr)` 重新用 SDK 执行器打标签、训练。
3. oracle 差距显著但 selector 已接近：模型不是瓶颈，应选择合适的预算/激进度默认值。

### 8.7 128MiB 首批结果：目标函数选错了，不是模型选错了

128MiB × 四个激进度工作点跑完后的结论。**同一批测量在两个目标下给出方向相反的答案**，这解释了此前所有
「学习式收益不大」「异步预取无效」的混乱。

字节目标（remote MiB，budget=128MiB）：

| 量 | (1,0) | (1,1) | (4,4) | (8,8) |
|---|---|---|---|---|
| `oracle_static` = `template_multimodal` | 2324.98 | 2324.98 | 2324.98 | 2324.98 |
| `oracle_perworkload` | 2321.73 | 2321.73 | 2321.73 | — |
| passthrough | 2468.16 | 2468.16 | 2468.16 | 2468.16 |
| decision_tree | 2748.67 | 3126.65 | 5168.31 | 7591.19 |
| template_auto | 3037.28 | 4043.82 | 13335.51 | 33891.68 |
| forced `s3a_prefetch` | 6567.68 | 10823.24 | 67032.81 | 206307.27 |

- 两个 oracle 只差 **0.14%**。8 个前缀里 6 个的字节最优都是 `template_multimodal`，而它没有激进度旋钮，所以
  `oracle_static` 在四个工作点完全相同，激进度只会伤害真实 selector、对 oracle 无影响。
- 逐前缀 argmin 之和恰好等于实测的 2321.73MiB，**共享缓存互扰为零**，构造性合成精确，不需要坐标下降。
- 字节口径下两个 selector 都**输给 passthrough**（决策树 +11.4%、手写 +23.1%），而两个 oracle 都比 passthrough 好约 5.9%。
- oracle 到 `amp_floor` 还差 923.7MiB，而策略路由能触及的只有 3.25MiB，**相差 284 倍**——剩余字节空间在缓存粒度与
  淘汰策略里，不在策略选择里。

成本目标（秒，RTT=50ms/BW=100MiB/s，budget=128MiB，(1MiB, depth 0)）：

| 配置 | GETs | remote MiB | cost |
|---|---|---|---|
| `oracle_perworkload[cost]`（实测，与逐前缀和一致） | 5283 | 3062.80 | **294.8** |
| decision_tree | 5595 | 2748.67 | **307.2** |
| template_auto | 5860 | 3037.28 | 323.4 |
| `oracle_static[cost]` = `template_locality` | 5996 | 2653.64 | 326.3 |
| `oracle_perworkload[bytes]` | 6211 | 2321.73 | 333.8 |
| `oracle_static[bytes]` = `template_multimodal` | 6248 | 2324.98 | 335.6 |
| passthrough | 11189 | 2468.16 | 584.1 |

三条决定性结论：

1. **自适应天花板从 0.14% 变成 10.7%**（`oracle_static[cost]` 326.3s vs `oracle_perworkload[cost]` 294.8s），
   放大 76 倍。按字节测出来的「天花板极低」是口径产物。
2. **决策树已吃掉 61% 的可用空间**（326.3→307.2，占 326.3→294.8 的 60.6%），手写规则只吃掉 9%。
   两者差 6.6 倍——这才是学习式相对手写规则的真实价值。且决策树 307.2s 优于**所有**静态策略与两个字节 oracle。
3. **按字节标定的 oracle 是负收益的**：`oracle_perworkload[bytes]` 在成本口径下比最优静态策略还差 2.23%。

`fmnist` 是最干净的例子，也是前一轮结论需要修正的地方：字节口径下选 `s3a_prefetch` 看起来是 320MiB 的巨大失误
（384.1MiB vs `s3a_random` 的 60.6MiB，且刚好对应 §8.5 的 256MiB 标签），像是标签工况错配。但成本口径下
`s3a_prefetch` 把 951 个 GET 压到 386 个，**23.1s vs 48.2s，是最优选择，省 52%**。`sift`（26.0s vs 36.6s）与
`tpch`（locality 30.5s vs 33.9s）同理。成本口径下 4 个小读前缀（fmnist/sift/emb_real/lastfm）的最优都是
`s3a_prefetch`，与 §6.2 模拟器标签一致。

**所以 §6.2 的标签没错，模拟器与 SDK 也没有矛盾：模拟器优化的目标接近时延/成本，而我们此前的 SDK 评测一直在优化字节。**
需要注意 `(1MiB, depth 0)` 下 `s3a_prefetch` 并非投机，而是按块对齐把需求读合并成更大的单次读、后续读命中缓存，
因此「每 GET 一次 RTT」的记账是成立的，省 RTT 是真实机制。

其他两点：

- **`cacheHitRate` 在这里是误导性指标**：强制预取的命中率随激进度单调上升（0.526 → 0.534 → 0.645 → 0.733），
  而同期总成本恶化 31 倍。不能用命中率评价策略。
- **激进度在两个目标下都是单调有害的**，`(1MiB, depth 0)` 是 128MiB 下的最优工作点。此前「异步预取无效」的结论
  在方向上是对的，但要改述为：投机深度无效，而块对齐的需求读合并（depth 0 的 `s3a_prefetch`）在 RTT 下高度有效。

#### 8.7.1 完整 bytes / cost proxy sweep（128–1024MiB）

完整结果在 `access_report/sdk_sweep_summary.csv`；下表只摘录无投机、代理精确的 `(block=1MiB, depth=0)`。
`cost` 固定为 RTT=50ms、BW=100MiB/s 的 `GET×RTT + bytes/BW`，不是已测真实网络 wall-clock。

| 预算 | bytes：static→per-workload gap | cost：static→per-workload gap | cost 下决策树距 per-workload | 结论 |
| --- | --- | --- | --- | --- |
| 128MiB | 0.14% | 10.71% | +4.23% | 树接近 cost 参照，吃到约 61% 的前缀级空间 |
| 256MiB | 0.16% | 9.93% | +15.36% | 最佳静态策略已迁移；现树未适配预算 |
| 512MiB | 0.24% | 7.99% | +35.43% | 字节工作集近乎覆盖，树仍偏离 cost 参照 |
| 1024MiB | 0.24% | 0.80% | +42.36% | cache 充足后前缀级 cost 空间基本消失 |

由此冻结三条当前有效结论：

1. **仅以 bytes 为目标时，策略路由空间始终极小**（0.14%–0.24%）；不能据此证明时延目标也无空间。
2. **以 cost proxy 为目标时，128–512MiB 仍有 8%–11% 的前缀级空间**，但它随预算变化，现有树不能作为跨预算通用策略。
3. 深度 ≥1 的代理排序受异步重叠影响，不能用 `GET×RTT` 直接冻结默认工作点；真实 latency sweep 和 AWS S3 replay 是下一门禁。

### 8.8 真实 AWS S3 Replay（手动 S4 验证，绝不进 CI）

当前 synthetic `rttMs/bwMiBps` 仅是一阶代理；S4 的最终结论以真实 AWS S3 的每次 `readAt()` wall-clock、
P50/P95/P99 和整条 trace wall-clock 为准。该测试不进入默认 `mvn verify`，也不得与 synthetic 延迟注入叠加。

`-Ds3arr.backend=s3` 已将 passthrough 接到 `S3ClientObjectStore`、将 S2/S3 接到
`S3AsyncClientObjectStore`。endpoint、bucket 与静态 access key 只从环境变量读取：

```bash
export S3ARR_ENDPOINT="https://s3.${S3ARR_REGION}.amazonaws.com"
export S3ARR_BUCKET='s3arr-bench'
export S3ARR_ACCESS_KEY='...'
export S3ARR_SECRET_KEY='...'
export S3ARR_REGION='us-east-1'
```

当前实现只接受上述静态 key/secret 配置，尚未接入 instance profile、role 或 session token。bucket 必须预先含有与
trace 同名、大小至少为 `max(file_size, offset + length)` 的不可变对象；benchmark 会逐 object HEAD 预检，并以
ETag 的 `If-Match` 防止测试中途覆盖。

使用 `mixed_holdout` 时需要约 16GiB 的对象数据。仓库中的 `MinioTraceObjectProvisioner` 是历史命名的
S3-compatible 预置器，也可指向 AWS S3 endpoint；它以 16MiB multipart 上传确定性字节。该工具按**对象大小**幂等跳过，
不校验已存在对象内容；若要使用它，预置与 replay 必须提供同一份绝对 trace 路径。

S4 主比较应运行默认四列（不要设置 `selector`，否则 benchmark 只跑单列）：

```bash
mvn -q -pl services-custom/s3-adaptive-range-reader \
  -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
  -Dtest=AdaptiveReaderSystemBenchmark test \
  -Ds3arr.backend=s3 \
  -Ds3arr.trace=/absolute/path/to/traces/mixed_holdout.csv \
  -Ds3arr.label=aws_s3_b256_default_d0 \
  -Ds3arr.cacheBudgetMiB=256 \
  -Ds3arr.prefetchBlockMiB=1 \
  -Ds3arr.prefetchDepth=0 \
  -Ds3arr.apps=1
```

固定策略与 `oracleMap` 使用单列命令另跑；每个配置独立重复至少 5 次，并在外部汇总 median，而不是设置 `iters>1`
（代码会选择最快一轮）。原始 `report-*.txt`、`results-v2.csv` 和环境元数据必须从 module `target/` 复制到
`docs/adaptive-range-reader/results/aws-s3_<date>/`，避免被 `mvn clean` 删除。

注意：`backend=s3` 与 `rttMs/bwMiBps` 互斥；真实运行的 `ns/op` / P50/P95/P99 包含网络时延，bytes 和 GET 仍须与
wall-clock 一同报告。真实 S3 的 cache budget、block/depth 与并发 app 数必须在每次实验中记录。

## 9. 更新要求

- 每次对项目产生修改后都要在该文档中进行记录



### 更新日志

- **2026-07-30（晚）**：**第三阶段/S4 文档收口：真实 AWS S3 验证为主线**。
  - §7 改为实施计划、完成状态与 S4 路线：明确 S0–S3 与 bytes/cost proxy sweep 已完成，latency sweep 与真实
    AWS S3 replay 是 S4 门禁；替换过期的 S0→S1 交接提示。
  - 统一 objective 叙事：bytes、cost proxy、真实 wall-clock 分层；撤销“未来重标定必须沿用字节口径”的绝对表述。
  - 新增 Oracle 术语表，区分历史 simulator 标签、agreement oracle、SDK static/per-workload oracle 与待测
    dynamic oracle；避免将标签命中率误作 SDK 性能上界。
  - 新增 §8.7.1，汇总完整 128–1024MiB sweep；确认 bytes 路由空间仅 0.14%–0.24%，而 cost proxy 下
    128–512MiB 仍有 8%–11% 前缀级空间，真实 latency/AWS S3 仍待验证。
  - 删除 MinIO 专用验证主线，§8.8 改为真实 AWS S3 replay；保留既有 S3-compatible 预置器的历史名称说明。

- **2026-07-30**：**新增 S3-compatible 真实 replay 预置与接线（未执行真实 endpoint）**。
  - `AdaptiveReaderSystemBenchmark` 新增 `s3arr.backend=synthetic|s3`，默认完全保持 synthetic 行为；真实 backend
    从 `S3ARR_ENDPOINT/S3ARR_BUCKET/S3ARR_ACCESS_KEY/S3ARR_SECRET_KEY[/S3ARR_REGION]` 读取连接信息，使用
    path-style `S3ClientObjectStore`（passthrough）和已有 `S3AsyncClientObjectStore`（S2/S3），启动前逐 object HEAD
    校验，并拒绝与 `rttMs/bwMiBps` 的双重延迟注入。
  - 新增显式手动 `MinioTraceObjectProvisioner`（历史命名，可用于 S3-compatible endpoint）：按 trace 用 16MiB multipart streaming 上传确定性对象，支持幂等跳过，
    对 mixed holdout 生成 33 个对象的 manifest；不依赖 AWS CLI/Python 第三方库，不进入默认 Surefire。
  - 新增 `MinioEnvironment`、sync/async S3 ObjectStore WireMock 覆盖及必要 test HTTP client 依赖；新增 §8.8，记录
    环境变量、命令、约 16.9GiB 容量要求、结果位置和真实网络实验注意事项。

- **2026-07-29（晚）**：**给 sweep 驱动加成本目标，并据 128MiB 首批结果修正核心结论**。
  - `tools/run_sdk_sweep.py`：新增 `cost = remoteGets × RTT + remoteBytes / BW` 目标，与 `bytes` 并列。每个
    `(budget, aggr)` 分别按两个目标做逐前缀 argmin、各自实测 oracle（表相同则复用一次测量，表不同则都回放）；
    所有行新增 `costSeconds` 列；新增 `--workpoints`/`--objectives` 开关；新增三层界汇总
    `access_report/sdk_sweep_summary.csv` 并在结束时打印；latency 阶段改由 `cost` 目标挑选工作点与静态最佳/最差。
  - 新增 §8.7：128MiB 全部四个工作点的结果。**核心修正——此前的问题是目标函数选错了，不是模型选错了。**
    字节口径下自适应天花板只有 0.14%、两个 selector 都输给 passthrough；成本口径下天花板是 10.7%，
    决策树吃掉其中 61%（手写规则只 9%），且优于所有静态策略与两个字节 oracle。
  - **撤回上一轮关于 `fmnist` 标签错配的判断**：字节口径下选 `s3a_prefetch` 看似 320MiB 的失误，成本口径下
    实为最优（23.1s vs 48.2s，把 951 个 GET 压到 386 个）。§6.2 标签正确，模拟器与 SDK 无矛盾——
    模拟器优化的目标接近成本，而此前 SDK 评测一直在优化字节。
  - 「异步预取无效」改述为：投机深度无效且单调有害，但 `depth 0` 下块对齐的需求读合并在 RTT 下高度有效。
  - 记录 `cacheHitRate` 是误导性指标（随激进度上升而总成本恶化 31 倍），以及重跑前必须移走累加的 `results-v2.csv`。

- **2026-07-29**：**产出阶段汇报 + 固化实验产物**（无代码改动）。
  - 新增 `阶段汇报_S2S3系统实验与有效性边界.md`：面向汇报的完整总结，明确区分**有效果**（缓存+策略路由 RTT 下
    IO 时延 −23.7%/GET −43.7%；学习式相对手写随预算从 +1.0% 放大到 −16.2%；256MiB 起读放大跌破 1.0；
    判准率 93.1% vs 12.4%；appID 隔离与关闭零变化）与**无效果/被证伪**（异步预取 +1.6%、推测命中率 4%/0%；
    滞后对判准率影响为 0 且切换无代价；「手写已够准」「判准率高即性能好」「ml_embedding 会翻转」三个假设被推翻；
    小预算下学习式反而略差），并汇总**四处评测口径缺陷**（预算不公平／trace 不代表／工况与标签标定不一致／
    标签口径是字节而非时延）与 simulator-实现代价不对应的限定。
  - **固化实验产物到 `docs/adaptive-range-reader/results/`**：基准报告原本位于 `services-custom/.../target/`
    （被 gitignore 且 `mvn clean` 即清空）、标签翻转日志原本在 `/tmp/`，均有丢失风险，已复制留存 13 份产物。
  - 下一步锚定：**动态 oracle 上界**（决定 bandit 方向是否值得走，是所有后续工作的天花板）与
    **预算压力/重用距离进入特征空间**（现有 10 维特征看不到决定标签的 ρ）。

- **2026-07-28（夜，续）**：**标签翻转检查 → 回答「64MiB 重训是否有意义」**（新增 `tools/label_flip_check.py`，见 §8.5）。
  - **反推出 §6.2 的标注口径 = 「256MiB 下最小远端字节」**（字节口径 argmin 5/6 精确复现标签；tpch 差 1.6% 属并列）。
    按时延取完全对不上——tpch/ml_epoch 会选 `s3a_prefetch`（时延最低但字节爆炸）。**此处“必须沿用字节口径”的
    当时结论已被 2026-07-29 的双目标分析修订**：重标定必须先声明 objective；RTT 主导时以 cost / 真实 wall-clock 为准。
  - **只有 lance 两类会随预算翻转**（6 个负载 4 个不变）：lance_small `s3a_prefetch`→`s3a_random`（64MiB 下沿用旧标签
    **多抓 111 倍字节、amp 228.6**）、lance_large `template_locality`→`template_multimodal`（+40%）。
  - **证伪了一个猜测**：原以为 ml_embedding（工作集 67MB）会在 64MiB 翻，实测没翻——其远端字节仅 0.9–1.3MiB
    （amp 0.014–0.020），工作集远小于估计。
  - **结论**：64MiB 重训**有实质但范围有限**；且模拟器 `s3a_prefetch`(8MiB×depth8) 与 SDK 执行器(1MiB×depth1) 代价不对应
    （SDK 在 64MiB 读放大仅 1.293，不会出现 228x），故重训收益无法照搬。**更根本的修法是用 SDK 自己的执行器在目标预算下
    重新标定标签**，而不是继续沿用模拟器执行器的标签。

- **2026-07-28（夜）**：**缓存预算扫描 → 谜团解开：策略选择的价值是工况函数，此前在错误工况下评测**（见 §8.4）。
  - **背景疑问**：判准率差 80 个百分点却只换来 4%，是否意味着项目「塌缩回缓存开/关」？
  - **实验**（零代码改动，只扫 `-Ds3arr.cacheBudgetMiB` = 8/32/64/256/1024，mixed_holdout，零-RTT 单次迭代）：
    读放大 S2 = 1.444 / 1.412 / 1.293 / **0.917** / **0.610**；`S2 vs template_auto` 远端字节
    **+1.0% / +1.3% / −5.3% / −16.2% / −14.8%**。
  - **结论**：(1) 256MiB 起读放大跌破 1.0，缓存才真正实现字节级复用（印证 §6.2 的 0.04x/0.16x 需要工作集<预算）；
    (2) 学习式相对手写的价值随预算**单调放大**，摆动达 17 个百分点；
    (3) **oracle 标签本就是在 256MiB 下标定的**（§6.2 成本模型），我们此前默认 64MiB = 在错误工况下给决策树打分——
    这才是「93% 判准率只换来 4%」的根因，不是模型选型问题（小预算下它甚至略差于手写，正是错配的直证）。
  - **对项目主张的修正**：**没有塌缩回「缓存开/关」**。四执行器差别是 over-fetch 形状（抓多大/怎么对齐/是否两段式），
    且「一律开缓存」并不免费——8–64MiB 下缓存要多抓 37–44% 字节，只赚 GET/RTT。真正的自适应问题是
    **「在给定工作集/预算比下，每次读该抓多大」**，而它的价值确实随预算进入复用区而显现。
  - **行动项**：评测默认预算应对齐 256MiB 或显式报告工作集/预算比；`-DargLine="-Xmx12g"`（大预算必须加堆，
    注意本机是 Java 8，不能传 JDK13+ 的 `-XX:+AllowRedefinitionToAddDeleteMethods`）。

- **2026-07-28（晚）**：**oracle 命中率验证 → 推翻「手写规则已够准」的假设，并定位真正瓶颈**。
  - **新增 `tools/policy_agreement.py`**（见 §8.3）：逐读比较 `ta(java)` / `ta(sim)` / `tree+hyst` / `tree(raw)` 的选择与
    §6.2 oracle 最优策略；in-sample 与 held-out 分开；mixed 按 `query_id` 的 source 标签逐行定 oracle（可区分 lance 类里
    fmnist=prefetch 与 sift=locality 两种不同最优）。**保真度交叉验证**：mixed 上算出的四策略分布与 Java 基准打印计数逐读全等。
  - **结论（held-out，读加权 / 按 trace 宏平均）**：`ta(java)` 命中 **12.4% / 23.8%**，`tree+hyst` 命中 **93.1% / 82.7%**。
    即手写规则**几乎从不收敛到最优策略**（tpch 上 0.3%、lance_small 0%、ml_lastfm_emb 0%——把 19988/20000 个读判成 locality），
    而决策树基本判对（弱点是 clickbench 仅 44.2%）。**假设被推翻：不是手写已够准，而是手写很不准。**
  - **真正的瓶颈由此定位**：策略判准率差了 **80 个百分点**，端到端只差 **~4%**（IO 时延 −4%、GET −5%）。
    说明在当前 SDK 接线下**「选哪个策略」这个杠杆很弱**——四个执行器一旦都坐在同一份 per-app 共享 cache + 预算后面，
    产生的 IO 形状高度趋同；收益几乎全部来自缓存本身（相对 passthrough −24%）。
    → 所以**换模型（GBDT/bandit）不是发力点**。
    **⚠ 后续修正（见下一条 §8.4）**：「策略选择是弱杠杆」被证实**只是 64MiB 预算这一工况下的结论**——
    把预算提到 oracle 标定所用的 256MiB，`S2 vs template_auto` 的远端字节增益从 −5.3% 扩大到 **−16.2%**。
    故此处「杠杆很弱」不可外推；真正的根因是**在错误工况下给决策树打分**。
  - **顺带回答"切换多有何劣势"**：当前实现里**没有直接代价**——`policySwitches` 只是 `MetricsRecorder` 的计数器，
    `AdaptivePolicySelector.reset()` 在主流程无任何调用者，四执行器共享 `AppCache` 故切换不失效缓存、不重取；
    切换的间接影响（块粒度变化导致的重叠抓取）已被 GET/读放大完整计入。滞后对命中率也几乎无影响（93.1% vs 93.1%）。
    结论：切换次数只是「规则对噪声敏感」的诊断信号，不是成本项，不应作为选型理由。

- **2026-07-28（下午）**：**RTT/带宽/think 成本模型注入 → 量清 S3 真实性能**。
  - **实现**：新增 `internal.io.SimLatency`（`fetchNanos = rtt + bytes/BW`、park-until-deadline 精确 sleep），
    `GeneratedObjectStore`（passthrough 同步路径）与 `InMemoryAsyncObjectStore`（异步 S2/S3 路径）都支持 `latency(rttNanos, bwMiBps)`。
    基准新增 `-Ds3arr.rttMs/bwMiBps/thinkMs`：延迟注入为**真实 sleep**，异步 GET 走共享线程池——demand 阻塞主线程 ~fetch 成本、
    预取在后台跑并可在 think 间隙完成使后续读命中缓存（这才可能掩盖 RTT）。报告新增 `IO latency sum (s)` 及"IO-latency deltas"段；
    开延迟时 `warmup/iters` 默认 `0/1`。四模式同注入，公平可比。留 0 保持原零-RTT 快速跑。
  - **结论（mixed_holdout, rtt=5ms/bw=200MiB·s⁻¹/think=5ms, apps=1）**：
    passthrough 75.96s → template_auto 60.46s → **S2(learn) 57.96s（相对 passthrough −23.7%）** → S3(learn,d1) 58.90s。
    即**缓存是 RTT 下的真正赢家（−24%）**；`S3 vs S2` 仅 **+1.6%**（打平略亏），`S2 vs template_auto` −4%（学习式略优）。
  - **预取为何仍无收益**：机制正常（929 次预取异步发出、不阻塞主线程，S3 IO 时延仅比 S2 高 1.6%），瓶颈是**推测命中率**——
    mixed 上预取 useful 仅 28MiB/697MiB≈**4%**，纯顺序的 `ml_taxi_epoch` 上更是 **0%**（该 trace 每读都是不同大区间、无块级复用）。
    depth-1「下一 1MiB 块」几乎不等于真正的下一次读；训练期预取"有用"是 simulator 用激进 8MiB×depth8 近似把小工作集整文件预热所致，
    Java 端保守参数不复现。→ 下一步方向：要么按工作集/预算比自适应放大预取块与深度，要么承认「RTT 下靠缓存」为主结论、预取仅在可验证顺序段开启。

- **2026-07-28**：**统一 baseline（新增 `template_auto` 手写规则）+ 公平预算 + 综合 holdout trace**（评测口径修正，非新功能）。
  - **公平预算**：基准的 S2 从"每对象独立 `PageCache`"改为**走 `AdaptiveReaderRuntime`（depth=0）**，与 S3 共用同一
    appID 组织 + 同一 work-conserving 全局预算（每 app 预留=全局/apps）。三模式差异收敛为"选择器大脑 + 预取深度"，
    `S3 vs S2` 成为干净的纯预取增量。`AdaptiveRangeReaderImpl`（旧每对象 S2）保留于生产代码，基准不再用它。
  - **新增手写规则基线 `template_auto`**：`internal.RuleBasedPolicySelector`（1:1 移植 `TemplateAutoPolicy.on_read`
    的 IO 形状派发，路由到同 4 执行器）；两处刻意调整——去掉 `query_id` 判断保持 label-free、页访问计数每读更新
    （修掉模拟器里 seen_pages 循环自增导致 locality 分支近乎不触发的瑕疵）。新增 public `SelectorMode{DECISION_TREE,
    TEMPLATE_AUTO}` + `AdaptiveReaderRuntime.builder().selectorMode(...)`，真实部署也可切换。
  - **基准四列化**：`passthrough / template_auto / S2(learn) / S3(learn)`，`render` 改为泛型多列，新增
    `S2(learn) vs template_auto` 等增量；`results.csv` 追加 `template_auto` 行。见 §8.1。
  - **综合 holdout trace**：新增 `tools/build_mixed_trace.py` → `traces/mixed_holdout.csv`（5 类均衡、保结构、分段拼接、
    源前缀命名空间，11189 reads）。见 §8.2。
  - **首个结果（mixed_holdout, apps=2）**：`S2(learn) vs template_auto` GET −3.7%、bytes −0.1% 且切换次数 73 vs 3400
    （学习式略优且远更稳）；两者相对 passthrough GET −41.6%。`S3 vs S2` 在本 trace 上 GET +14.2%（预取净亏——因零-RTT
    基准只量字节维度、且工作集>预算，见 §8.1 读法边界）。`mvn -pl … -Djapicmp.skip=true verify` 全绿。
- **2026-07-23**：完成 §7.3 **S3（异步预取 + work-conserving 统一预算 + appID 环境隔离）**。
  - **异步 IO SPI**（`internal.io`）：`AsyncObjectStore.head/getRange(CompletableFuture)`；`S3AsyncClientObjectStore`
    唯一接触 `S3AsyncClient`（`AsyncResponseTransformer.toBytes()` + 版本 `If-Match`，412/异常 → `ObjectChangedException`）；
    测试双件 `InMemoryAsyncObjectStore`（immediate/executor/manual 三种完成模式 + 可注入延迟/失败/对象变更 + 并发峰值计数）。
  - **策略脑抽出为 planner**（`internal.exec`）：`PolicyPlanner`（`plan()` 一次算 demand + prefetch）+ 4 个实现
    （random/multimodal 不预取；locality/prefetch 支持 depth≥1 的块 lookahead，默认 block 1MiB、depth 1）。**S2 四个执行器改为
    委托同一 planner，行为与字节保持不变**（S2 测试全绿）。
  - **预取**（`internal.prefetch`）：`Prefetcher`（per-reader）——in-flight 注册表去重（demand 命中在飞的预取则 join 复用，不重复
    GET）、`seek` 取消未用推测、异步失败回退精确 demand、完成后入 per-app cache 并记 useful 归属。
  - **work-conserving 统一预算**（`internal.budget`）：`GlobalBudget`（全局硬上限 + 每 app 预留 + 借用；回收时只淘汰**他 app 超出
    预留的借用块**，保证任一 app 不被他人挤到预留以下，且全局上限不破）、`AppBudgetLease`、全局 `ConcurrencyLimiter`（并发 GET
    信号量 + 峰值）、每 app `InflightLimiter`（推测预取 in-flight 字节上限）。in-flight 用硬上限而非纳入可回收池（POC 简化，已注明）。
  - **per-app 多对象缓存**（`internal.cache`）：新增 `AppCache`（块键含 `bucket/key + versionToken`，跨对象），预算由 lease 支撑并
    作为 `CacheEvictor` 暴露 LRU 回收；与全局预算共用一把锁避免死锁。S2 的 `PageCache`（单对象）保留不动。
  - **运行时与隔离**（public）：`AdaptiveReaderRuntime`（进程级，持全局预算/并发/共享决策树模型）→ `register(engineName)` 分配
    appID → `AppContext`（独占 cache/lease/in-flight/**per-app 跨对象共享且线程安全的 selector** + reader 工厂）。同一 JVM 内
    flink/spark 各拿独立环境；`RuntimeConfig` 提供全局/预留/并发/预取深度/块大小/单抓上限等可调项。
  - **采集口径修正**：selector 由 S2 的“每 reader/每对象”提升为 **per-app 跨对象共享**（`SharedPolicySelector`/`LockingPolicySelector`
    串行化 `onRead`），**消除 §7.2“窗口作用域”已知差异（仅 S3 路径）**；`currentPolicy`/hysteresis 随之为 per-app 语义。
  - **指标**：`ReaderMetrics` 增 prefetch GETs/bytes、useful/wasted、cancelled（改 builder 构造）；新增 `AppMetrics`（每 app 预留/
    用量/块数/in-flight 峰值）与 `RuntimeMetrics`（全局用量/并发峰值 + 每 app 明细，用于证明隔离）。
  - **验证（全离线）**：`PrefetcherTest`（demand 正确/入 flight 去重/取消释放/失败回退/超限跳过）、`GlobalBudgetTest`（预留保护、
    借用回收、硬上限、释放）、`PrefetchingReaderTest`（混合读字节正确/depth=0 退化/seek 取消/ObjectChanged 上抛）、`AppIsolationTest`
    （预留被尊重且无跨 app 淘汰、特征窗口/selector 分离、并发 GET 全局封顶）、`CrossObjectSelectorTest`（跨对象共享 selector + 并发
    线程安全）、`MultiAppTraceReplayTest`（双 app 回放 tpch）。`mvn -pl services-custom/s3-adaptive-range-reader
    -Djapicmp.skip=true verify` 全绿。
  - **基准**：`AdaptiveReaderSystemBenchmark` 增第三种模式 **S3（异步预取 + 多 app）**，新增 `-Ds3arr.apps` / `-Ds3arr.prefetchDepth`，
    打印 prefetch useful/wasted/cancel 与每 app `RuntimeMetrics` 隔离明细；`results.csv` 追加 `s3` 行及 prefetch 列（见 §8.1）。
- **2026-07-23**：§8 新增 **§8.1 S2/S3 系统基准**运行说明（手动 `-Dtest=AdaptiveReaderSystemBenchmark`）；
  明确 `passthrough(off)`=`PassthroughRangeReader`、`adaptive(on)`=`AdaptiveRangeReaderImpl`，以及 IO 形状指标有效、
  ns/op 不含网络、不宜与 simulator 读放大逐位对比等读法边界。
- **2026-07-22**：完成 §7.3 **S2（同步可 seek 自适应 Range Reader + IO 采集接入）**。
  - **IO 采集层**：明确落在 reader impl 的读入口 `AdaptiveRangeReaderImpl.readAt()`——每次逻辑 `read`
    恰好构造一个 `IoRequest(key, pos, len, size)` 并 `selector.onRead()` 一次（与 S1 golden “每逻辑读一次
    特征”对齐）；**不在 ObjectStore（物理抓取层）、不在执行器（策略相关）**。
  - **ObjectStore SPI**（`internal.io`）：`ObjectStore.head/getRange` 隔离 SDK 依赖。`S3ClientObjectStore`
    是唯一接触 `s3` 客户端的类，用 `GetObjectRequest.range("bytes=a-b")` + `ResponseTransformer.toBytes()`，
    以 head 返回的 eTag 作版本令牌，后续 GET 带 `If-Match`，412 → `ObjectChangedException`（版本隔离）。
    测试双件 `InMemoryObjectStore`（存真实字节）与 `GeneratedObjectStore`（按位置函数生成，免分配百 MiB 对象）。
  - **PageCache**（`internal.cache`）：单对象版本的字节预算 + LRU 块缓存（`LinkedHashMap` accessOrder），
    命中判定为“单块完全覆盖”（不跨块拼接，语义对齐 simulator `BlockCache.find_covering`）；超预算前端淘汰，
    超过整预算的块不缓存但仍返回字节。
    *PageCache日后可以更换为不同的缓存策略*
  - **四个同步执行器**（`internal.exec`，`AbstractPolicyExecutor` 统一“查缓存→按策略取范围→夹取覆盖请求+对象界
    →预算护栏→GET→填缓存→拷贝”）：`s3a_random`(64KiB readahead)、`template_locality`(256KiB 页复访≥2 才整页,
    否则 64KiB)、`template_multimodal`(≤16KiB 对齐 64KiB, 大读精确)、`s3a_prefetch`(**S2 降级：1MiB 块对齐 demand,
    无投机 lookahead**)。正确性由基类保证（返回字节必来自单一覆盖缓存块或一次抓取缓冲）。
  - **API/安全**：`AdaptiveRangeReader`（`SdkAutoCloseable`，`size/read(pos)/read/seek/position/metrics/currentPolicy`
    + builder：`s3Client/bucket/key/versionId/enabled/cacheBudgetBytes/maxSingleFetchBytes/appId`）。**flag 默认关**
    → `PassthroughRangeReader` 精确 demand，字节与原生一致；flag 开 → 未知标签/执行器异常回退精确 demand
    （`fallbackReads` 计数），`ObjectChangedException` 上抛；单次抓取超 `maxSingleFetchBytes` 退化为精确 demand。
    关于 `appId`：2.25.70 无原生应用标识，凭证身份不便获取/不稳定，故 `appId` 可选、per-App 先验持久化留待 S3+。
  - **观测**：`ReaderMetrics`（logical/remote bytes、GET 数、read amplification、cache 命中率、每策略读次数、
    策略切换次数、fallback 次数、每读时延 p50/p95/p99）。
  - **[已知差异] 特征窗口作用域**：reader 按单对象实现，`FeatureWindow` 只含同对象的读；而训练/S1 golden 用的是
    跨对象全局窗口。原始字段与特征公式仍 1:1 一致，但 `distinct_obj_ratio` 等多对象特征分布偏离训练分布，
    可能影响选策略（不影响正确性）。详见 §7.2 约束 2 的“已知差异”。**换模型前必须对齐此口径。**
  - **验证**：全离线。`AdaptiveRangeReaderFunctionalTest`（顺序/随机/重叠/后向 seek/跨页/EOF/有状态游标/预算不越界/
    对象中途变更抛错/passthrough 精确等价且放大=1.0）、`PageCacheTest`、`TraceReplayTest`（回放 tpch 400 行切片，
    逐读断言字节正确 + 指标 sane；缓存使放大可 <1.0）。`mvn -pl services-custom/s3-adaptive-range-reader
    -Djapicmp.skip=true verify` 全绿（checkstyle/spotbugs/javadoc/依赖分析 + 30 用例）。
- **2026-07-17**：完成 §7.3 **S1（模型导出 + Java 无依赖推理 + golden 一致性）**。
  - 新增 `tools/export_policy_tree.py`：把 `models/policy_selector.joblib` 导出为 versioned
    `models/policy_selector_v1.json`（111 节点决策树、特征 schema/常量、sklearn 分裂语义
    `x[feat] <= threshold` 走左、叶子 `argmax(value)`、sha256 校验和 + provenance）。
  - 新增 `tools/export_golden_vectors.py`：回放 `traces/` 6 条真实 trace + mixed 切换段 + 边界样本，
    产出 2361 条 golden（每读含 10 维特征 + 原始预测标签 + hysteresis 后当前策略；34 次切换）。
  - 新建 opt-in 模块 `services-custom/s3-adaptive-range-reader`（已在 `services-custom/pom.xml` 注册）：
    `FeatureWindow`（1:1 复刻 `extract_features`，含 numpy median、log2、边界、无同对象 pair 时
    `forward_ratio=1.0` 等；热路径仅分配结果数组）、`DecisionTreePolicySelector`（classpath 载入 JSON +
    schema/常量校验 + 树遍历）、`Hysteresis`（3 次一致）、`AdaptivePolicySelector`（在线“选策略大脑”，
    **仅选策略、不做任何 S3 IO**，执行层留待 S2/S3）。
  - 一致性：`GoldenConsistencyTest` 回放全部 2361 读，特征容差 1e-9 一致、**预测标签与 hysteresis 后
    策略 100% 一致**；另有 FeatureWindow/Hysteresis/DecisionTree 边界单测。`mvn verify`（checkstyle/
    spotbugs/javadoc/依赖分析）全过，20 用例通过；微基准 `onRead` ≈ 6.5 µs/op。
  - 工具链：Python 3.12 venv（sklearn 1.9 / numpy 2，模型以 numpy2 pickle）、本地 Maven 3.9.16 + JDK8；
    首次构建需 `mvn -pl build-tools install` 提供本地 `build-tools:1.0` 插件依赖。
- **2026-07-17**：新增 §7“Track 1 从离线 POC 迁移到 AWS SDK”交接计划。目标仓库为
AWS SDK for Java v2.25.70；明确采用 opt-in `AdaptiveRangeReader`、模型 JSON 导出、
Java 无依赖推理、同步 Range Reader→异步预取→统一预算→真实 AWS S3 验收的分阶段路线，
并给出新工作区首个提示词。
- **2026-07-17**：将训练修复版 v3 提升为默认模型（`models/policy_selector.joblib`；
`max_per_trace=15000, max_depth=12, min_samples_leaf=10`）。mixed 上相对 `template_auto`
**时延 −7.8%、远端字节 −3.5%**；切换尖峰 12.7x。Path 2 判定：决策树可优于手写自适应。
非标准测试集上的单负载回归（如 tpch）暂不处理。评测默认产物同步为 v3。
- **2026-07-16**：完成 Track 1 首版 POC（Path 2 离线）。新增 `stat_selector`、
`train_policy_selector.py`、`eval_track1.py`、`阶段汇报_Track1统计策略选择器.md`；
`BlockCache.find_covering` 改 O(log n)。首版 v1：mixed 时延 −5.8%，但远端字节 +77.8% +
切换点 291x（后由 v3 修复）。

