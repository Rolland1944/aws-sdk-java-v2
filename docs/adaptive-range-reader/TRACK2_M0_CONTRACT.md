# Track 2 M0 实验合同（冻结版）

> **定位**：本文件是 `TRACK2_PLAN.md` §6 第一步「冻结实验合同」的交付物，对应里程碑 **M0（8 月 5–7 日）**。
> 它冻结 Track 2 后续所有实验的技术栈、工作负载、数据契约、指标口径、候选档位、能力矩阵和实验清单。
>
> **文档优先级**：`PROJECT3.md`（总路线） > `TRACK2_PROJECT.md`（Track 2 执行与变更记录） > 本文件（M0 冻结项） > `TRACK2_PLAN.md`（计划）。
>
> **冻结日期**：2026-08-07。**当前修订 r3**：2026-08-07（r2 引擎改 Spark 4.1.x、Writer 改 parquet-mr；
> r3 动作改用引擎中立规范名、跨引擎适配定为备选项。见 §12 修订记录）。
>
> **修改规则**：本文件冻结后**不得为了让结果更好看而修改**。任何变更必须在 `TRACK2_PROJECT.md` 变更记录中说明
> 「改了什么、为什么改、改之前已经观测到什么」，并明确该变更是否影响已产出的结论。
> 尤其禁止在看到实验结果后更换 §4 的主指标。

---

## 0. 决策摘要

| # | 决策项 | 冻结结果 | 依据 |
| --- | --- | --- | --- |
| D-1 | 查询执行引擎 | **Spark 4.1.x** + S3A | `TRACK2_PROJECT.md` §7.7：SQL 语义 SDK 不可得，需 engine adapter；物理层需 JVM SDK 热路径 |
| D-2 | SDK 物理层采集方式 | AWS SDK v2 `ExecutionInterceptor`，经 `fs.s3a.audit.execution.interceptors` 注入 | 非侵入；无需自定义 FileSystem |
| D-3 | Hadoop 版本 | **3.4.2（Spark 4.1.x 自带）** | S3A 自 Hadoop 3.4.0 起用 AWS SDK v2；Spark 4.x 已自带，无需替换发行版 |
| D-4 | Parquet 元数据解析 | Python + PyArrow 离线解析 footer（**只读**） | `TRACK2_PROJECT.md` §7.7：不让 SDK 在热路径重复解析 footer |
| D-5 | SF100 数据生成 | DuckDB `dbgen` → 一份规范源数据 | 用户决策（2026-08-07） |
| D-6 | Parquet 布局写出 | **Spark / parquet-mr 1.15.x** | 与 Reader 同库，消除跨库能力错配；`parquet.block.size` 以字节计；可分布式写 SF100 |
| D-7 | 代码落点 | `tools/track2/`（Python）+ `s3-adaptive-range-reader` 内采集钩子 | 与现有 `tools/`、`traces/`、`results/` 归档规范同仓 |
| D-8 | 云环境 | AWS S3 `us-east-2` + EC2 `m5d.4xlarge`（同 region） | 用户决策（2026-08-07） |
| D-9 | 基线布局口径 | Spark 默认 `df.write.parquet(...)` | Writer 与 Reader 同库后，默认值天然自洽，无需人工对齐 |
| D-10 | 列顺序实验 | 不进入首轮门禁，但**保留为 M5 可行动作** | Spark 4.1 有 vectored IO，Range 合并可发生；见 §6.2 M-3 |
| D-11 | Reader Range 合并 | vectored IO **显式开启并全程固定**；开/关对比只作敏感性分析 | `TRACK2_PLAN.md` §4.2：不与 Writer 同时自适应 |
| D-12 | 跨引擎 Writer 可移植性 | 候选动作用**引擎中立的规范名**表达（对齐 Iceberg 属性词表）；Flink/Trino 适配器列为**备选项**，不进入 M1–M4 | 动作空间本质是格式级而非引擎级；见 §11 |

---

## 1. 技术栈与环境合同

### 1.1 引擎与版本

| 组件 | 冻结版本 | 说明 |
| --- | --- | --- |
| Spark | **4.1.x** | 见下方选型理由 |
| Hadoop | 3.4.2（Spark 4.1.x 自带） | S3A 已在 AWS SDK v2 上 |
| parquet-mr | 1.15.x（实际版本由 E0 记录） | 有 vectored IO（1.14.0 引入） |
| AWS SDK v2 bundle | 随 Hadoop 3.4.2 | 实际版本在 E0 记录归档 |
| JDK | **17**（Spark 4.x 要求 17 或 21） | 记录 vendor 与完整版本号 |
| Scala | 2.13（Spark 4.x 唯一选项） | 我们用 PySpark / SQL，无直接影响 |
| PyArrow | ≥ 19.0（实际版本由 E0 记录） | 仅用于 footer 解析与 §6.2 M-5 逃生舱 |
| DuckDB | ≥ 1.1（实际版本由 E0 记录） | 仅用于 dbgen |

**为什么是 Spark 4.1.x 而不是 3.5.x**：Spark 3.5 会同时引入两个版本问题——它自带 Hadoop 3.3.4（S3A 仍在
AWS SDK **v1** 上，`fs.s3a.audit.execution.interceptors` 会被静默忽略），且自带 parquet-mr 1.13.1（**无** vectored IO）。
绕开前者需要 `bin-without-hadoop` 发行版加手工拼 Hadoop 3.4.1，后者则根本无法绕开。
Spark 4.x 自带 Hadoop 3.4.1+ 与 parquet-mr 1.15.2，**一次解决两个问题**。

**为什么是 4.1 而不是 4.0 / 4.2**：Spark **3.6 不存在**——3.x 线到 3.5 终止（extended LTS 至 2027-11），
之后直接进入 4.x。4.0.x 于 2026-11-23 EOL（仅剩约 120 天），实验期内就会失去支持；
4.2.0 于 2026-07-14 才发布，过新。4.1.x（2025-12 发布，EOL 2027-06）成熟度与支持窗口都覆盖整个项目周期。

### 1.2 云环境

| 项 | 值 |
| --- | --- |
| 对象存储 | AWS S3，`us-east-2` |
| 客户端 | EC2 `m5d.4xlarge`，同 region（16 vCPU / 64 GiB / 2×300GB NVMe / 网络 "Up to 10 Gbps"） |
| 凭证 | instance profile（不落盘长期 key） |
| 本地 scratch | NVMe 实例存储，用于 SF100 生成与 shuffle |
| 对象版本 | 全程记录 `VersionId` 或 `ETag`；布局重写后视为新对象版本 |

> **⚠ 网络突发风险**：`m5d.4xlarge` 的 10 Gbps 是**突发**带宽，持续基线显著更低。
> 长时间大吞吐 scan 可能触发带宽信用耗尽，导致运行间方差上升，**直接威胁 §4.2 的 CV < 5% 要求**。
> E0 必须实测持续吞吐并记录；若 CV 不达标，优先考虑换用带宽有保证的实例类型，而不是放宽 CV 门槛。

### 1.3 Writer 工具链（D-5 / D-6）

```text
DuckDB（一次性）                    Spark / parquet-mr（每个布局候选）
  └─ CALL dbgen(sf=100)               ├─ parquet.block.size        (row group，字节)
     → 规范源数据                      ├─ parquet.page.size         (字节)
       （Parquet，仅作为源）           ├─ parquet.page.row.count.limit
                                      ├─ parquet.enable.dictionary[#col]
                                      ├─ parquet.bloom.filter.enabled[#col]
                                      ├─ parquet.bloom.filter.expected.ndv#col
                                      ├─ parquet.compression（全局，见 M-5）
                                      ├─ parquet.writer.version
                                      ├─ ORDER BY / sortWithinPartitions（真实排序）
                                      ├─ partitionBy（分区）
                                      └─ maxRecordsPerFile / repartition（target file size）
```

**分工理由**：DuckDB 只负责生成**一份**规范 SF100 源数据（dbgen 简单、确定性好）；
所有布局候选由 Spark 从这份源数据重写产生。

**为什么 Writer 换成 parquet-mr 而不是 PyArrow**（r2 修订的核心，见 §12）：

1. **消除跨库能力错配**。原方案 PyArrow 写 / parquet-mr 读，产生的 M-1（page index 默认值相反）、
   M-4（`sorting_columns` 只是声明）等错配**不是研究发现，而是自找的集成缺陷**。
   Writer 与 Reader 同库后，写侧默认值与读侧默认值天然自洽。
2. **`parquet.block.size` 以字节计**，直接消灭 PyArrow `row_group_size`（单位是**行数**）带来的
   字节↔行数换算问题，原 O-4 未决项随之取消。
3. **per-column 控制并未损失**：parquet-mr 支持 `parquet.enable.dictionary#column.path` 与
   `parquet.bloom.filter.enabled#column.path`、`parquet.bloom.filter.expected.ndv#column.path`。
4. **SF100 规模可行**。单机 PyArrow 逐个候选写约 30GB Parquet 是时间黑洞；Spark 可并行写。
5. **提升论文可比性**。PTO 的动作落点就是 Iceberg/Spark 表属性；Code Diff 目标为 Spark writer 配置时，
   与 PTO baseline 是同类对象。对 PyArrow 脚本的 Diff 说服力弱得多。
6. **基线更无可争议**：基线就是 `df.write.parquet(...)`，即真实用户所得，无需再为「基线是否够强」辩护。

**PyArrow 的保留用途**：footer 解析（D-4，只读，不受影响）；以及 §6.2 M-5 所述 per-column 压缩/编码
实验的逃生舱（仅 M5 需要时启用，且必须单独说明）。

**Code Diff 的目标**因此改为 `tools/track2/write_layout.py` 中的 **Spark writer 配置与 DataFrame 变换**
（排序、分区、文件切分）。

### 1.4 Reader 配置冻结

Writer 布局比较期间 Reader 配置**必须全程固定**（`TRACK2_PLAN.md` §4.2）。冻结如下：

| 配置 | 冻结值 | 说明 |
| --- | --- | --- |
| `fs.s3a.audit.enabled` | `true` | D-2 的前提 |
| `fs.s3a.audit.execution.interceptors` | Track 2 采集器类名 | 见 §8 |
| `fs.s3a.input.fadvise` | `random` | Parquet 列式随机读的标准配置 |
| `parquet.hadoop.vectored.io.enabled` | **`true`（显式设置）** | D-11；parquet 1.15 默认为 `false`，1.16+ 默认 `true`，故必须显式写死 |
| `fs.s3a.vectored.read.min.seek.size` | `128K`（默认） | Range 合并阈值，全程固定 |
| `fs.s3a.vectored.read.max.merged.size` | `2M`（默认） | 同上 |
| `fs.s3a.vectored.active.ranged.reads` | `4`（默认） | 同上 |
| `parquet.filter.stats.enabled` | `true`（默认） | row group min/max 裁剪 |
| `parquet.filter.dictionary.enabled` | `true`（默认） | |
| `parquet.filter.columnindex.enabled` | `true`（默认） | page 级裁剪 |
| `parquet.filter.bloom.enabled` | `true`（默认） | 写侧需显式开启，见 §6.2 M-2 |
| `fs.s3a.threads.max` / `connection.maximum` | E0 中标定后固定 | 全实验一致 |
| Spark 并行度 / executor 规格 | E0 中标定后固定 | 全实验一致 |
| Reader 侧五维自适应（D1–D5） | **全部关闭** | 走 passthrough，避免缓存变化掩盖布局因果效应 |

**缓存状态**：cold-cache 与 steady-state 分开报告。**主 profile 冻结为 cold-cache**
（每次运行前重建 Spark 会话、清页缓存），因为它与 Writer 布局的因果关系最直接。

---

## 2. 工作负载合同

### 2.1 TPC-H 查询集与频率 profile

- **查询集**：TPC-H 标准 22 条查询，全部纳入。使用 DuckDB `tpch` 扩展提供的标准查询文本，不做人工改写。
- **主 profile（用于门禁）**：**均匀频率**，每条查询权重 1。
  TPC-H 没有天然的查询频率分布，任何加权都是人为构造；均匀是唯一无需辩护的选择。
- **敏感性 profile（不用于门禁，预先声明）**：`skewed`，按各查询在基线上的 wall-clock 占比加权。
  它只用于检查建议是否对频率假设稳健，结论必须单独标注。

> 加权总 wall-clock 之外，**必须逐查询报告**（§4.1 guardrail 需要）。任一 profile 的结论都不得掩盖单查询回归。

### 2.2 SF100 生成规则与确定性

| 项 | 冻结值 |
| --- | --- |
| Scale factor | 100 |
| 生成器 | DuckDB `tpch` 扩展 `CALL dbgen(sf=100)` |
| 表 | 标准 8 表全量 |
| 确定性 | dbgen 对给定 SF 是确定性的；生成后记录每表行数与内容校验和 |
| 归档 | DuckDB 版本、扩展版本、生成命令、每表行数、校验和、生成耗时 |

**所有布局候选必须由 Spark 从同一份 dbgen 源数据重写产生**，只改变 §1.3 的写出参数与行序。
禁止为不同候选重新 dbgen——那会引入数据本身的差异，破坏因果归因。

### 2.3 默认（基线）布局 —— D-9

基线 = **Spark 默认 `df.write.parquet(...)`**，即真实用户不做任何调优时所得：

| 参数 | 基线值 | 来源 |
| --- | --- | --- |
| 分区 | 无 | |
| 排序 | 无（dbgen 原始行序） | |
| target file size | 由默认并行度决定，E0 记录实际分布 | |
| `parquet.block.size`（row group） | 128 MB | parquet-mr 默认 |
| `parquet.page.size` | 1 MB | parquet-mr 默认 |
| `parquet.page.row.count.limit` | 20000 | parquet-mr 默认 |
| ColumnIndex / OffsetIndex | 写入（**无法关闭**） | parquet-mr 自 1.11 起恒写，见 §6.2 M-1 |
| bloom filter | 关闭 | parquet-mr 默认 |
| `parquet.enable.dictionary` | `true` | parquet-mr 默认 |
| `parquet.compression` | `snappy` | Spark 默认 |
| `parquet.writer.version` | `PARQUET_1_0` | parquet-mr 默认 |

> **r2 修订说明**：r1 曾需要显式把 PyArrow 的 `write_page_index` 打开来「对齐 parquet-mr 默认行为」，
> 否则基线会是一个连主流 writer 默认能力都没有的弱布局（`TRACK2_PROJECT.md` §7.4 的
> 「避免在较差 baseline 上得到漂亮百分比」陷阱）。改用 parquet-mr 作为 Writer 后，
> **这个人工对齐动作不再需要**——基线定义为库默认值即可，这也是该项修订的直接收益之一。

---

## 3. 数据契约

### 3.1 关联键与关联机制

三层 telemetry 通过 `query/span id + object key + version + time window` 关联（`TRACK2_PROJECT.md` §7.7）。
在 Spark + S3A 栈上具体实现为：

- **主路径**：Spark executor 在任务开始时把 `sql_execution_id` 写入 S3A 的
  `CommonAuditContext`；`ExecutionInterceptor` 直接从审计上下文读出，物理请求天然带上查询身份。
- **兜底路径**：S3A `LoggingAuditor` 会把审计信息（含 `op`、路径、span id、task attempt id）写入 HTTP
  `Referer` 头。拦截器解析该头拿到 task attempt id，再用 Spark 事件日志做
  `task attempt → stage → job → sql_execution_id` 的连接。

两条路径都记录；E1 分别报告各自的覆盖率。**若主路径覆盖率已达标，兜底路径仅作为交叉校验。**

### 3.2 ObservationBundle

一条记录 = 一个物理 GET，已关联到查询语义与格式元数据。

```jsonc
{
  // ---- 关联键 ----
  "run_id": "string",              // 一次完整 workload 运行
  "sql_execution_id": "string",    // Spark SQL execution id
  "query_id": "string",            // "q01".."q22"
  "scan_fragment_id": "string",    // 一个 FileSourceScanExec 实例
  "attempt_id": "string",          // task attempt，兜底关联用
  "ts_start_ns": 0, "ts_end_ns": 0,

  // ---- 语义层（SemanticCollector）----
  "table": "lineitem",
  "filter_columns": ["l_shipdate"],
  "project_columns": ["l_orderkey", "l_extendedprice"],
  "predicates": [{"column": "l_shipdate", "op": "<=", "literal": "1998-09-02"}],
  "rows_in": 0, "rows_out": 0,
  "files_pruned": 0, "row_groups_pruned": 0,

  // ---- 格式层（FormatMetadataCollector）----
  "object_key": "s3a://bucket/prefix/part-00000.parquet",
  "object_version": "string",      // VersionId 或 ETag
  "row_group_ordinal": 0,          // 命中的 row group；-1 表示 footer/metadata 读
  "column_chunks": ["l_shipdate"], // 该 GET 覆盖的 column chunk 集合（合并后可能多个）
  "page_ordinals": [0, 1],         // 可用时；不可用时为 null，不得伪造
  "chunk_offsets": [[0, 0]],       // 与 column_chunks 一一对应的 [offset, length]

  // ---- 物理层（SdkIoCollector）----
  "range_offset": 0, "range_length": 0,
  "http_status": 200,
  "bytes_returned": 0,
  "bytes_wasted": 0,               // 取回但不属于任何被请求 chunk 的字节（Range 合并代价）
  "latency_ns": 0,
  "retry_count": 0,
  "concurrency_at_issue": 0,

  // ---- 归因质量 ----
  "attribution_level": "column_chunk"  // one of: none | object | row_group | column_chunk | page
}
```

**`attribution_level` 是必填字段**，E1 的覆盖率就按它统计。
`page_ordinals` 在无法确定时必须为 `null`——`TRACK2_PLAN.md` §6 明确要求「不支持时不得声称 page 级归因」。

> **r2 变更**：开启 vectored IO 后一次 GET 可覆盖**多个** column chunk，故 `column_chunk` 改为
> `column_chunks` 数组，并新增 `bytes_wasted`。归因仍然精确，只是从 1:1 变为 1:N；
> `bytes_wasted` 同时为 `PROJECT3.md` §3.4 的 `g* ≈ RTT × BW` 分析提供直接测量。

### 3.3 LayoutCandidate

沿用 `TRACK2_PLAN.md` §3.2 / `TRACK2_PROJECT.md` §7.8 的统一中间表示，字段具体化为：

```jsonc
{
  "candidate_id": "string",
  "scope": {"table": "lineitem", "partition": null},
  "actions": [
    // canonical 是引擎中立的规范名（§11），rendered 是当前 Writer 的具体配置
    {"family": "row_group_size",
     "canonical": "write.parquet.row-group-size-bytes", "value": 33554432,
     "rendered": {"writer": "parquet-mr", "parquet.block.size": "33554432"}},
    {"family": "sort",
     "canonical": "sort.columns", "value": ["l_shipdate"],
     "rendered": {"writer": "parquet-mr", "transform": "ORDER BY l_shipdate"}}
  ],
  "prerequisites": [],             // 例如 bloom 需要 Reader bloom filtering 能力
  "conflicts": [],
  "evidence": [                    // 必须可追溯到 ObservationBundle 聚合
    {"metric": "row_groups_touched_ratio", "value": 0.94,
     "source": "run_id=... query_id=q06", "n": 5}
  ],
  "predicted_metrics": {
    "scan_wallclock_ms": 0, "end_to_end_wallclock_ms": 0,
    "gets": 0, "remote_bytes": 0,
    "files_touched": 0, "row_groups_touched": 0
  },
  "uncertainty": {"scan_wallclock_ms": [0, 0]},   // 区间估计
  "rewrite_cost": {"bytes": 0, "seconds": 0, "usd": 0.0},
  "storage_delta_bytes": 0,
  "patch": {"file": "tools/track2/write_layout.py", "diff": "string"},
  "rollout": "string",
  "rollback": "string",
  "fidelity": "L1"                 // L0 | L1 | L2 | L3 —— 该预测来自哪一级
}
```

**`fidelity` 是必填字段**：任何一处引用 `predicted_metrics` 都必须能说清它来自哪一级保真度，
避免把 L1 解析模型的估计当成实测结论。

### 3.4 RecommendationPackage

```jsonc
{
  "package_id": "string",
  "workload_window": {"from": "ISO8601", "to": "ISO8601", "runs": 5},
  "data_snapshot": {"scale_factor": 100, "dbgen_checksum": "string"},
  "observation_coverage": {"attribution_ge_column_chunk": 0.0, "page_level": 0.0},
  "baseline": { /* LayoutCandidate，actions 为基线布局 */ },
  "recommended": { /* LayoutCandidate */ },
  "measured_metrics": { /* 与 predicted_metrics 同构，来自真实云 */ },
  "prediction_error": { /* measured vs predicted，逐指标 */ },
  "benefiting_queries": ["q06"],
  "regressing_queries": [{"query_id": "q01", "delta_pct": 3.2}],
  "guardrail_status": {"single_query_regression": "pass", "p95": "pass",
                       "p99": "pass", "peak_memory": "pass"},
  "rewrite_payback_periods": 0.0,
  "canary": "string",
  "validity": {"expires": "ISO8601", "reevaluate_if": "string"},
  "verdict": "accept | reject | inconclusive"
}
```

`verdict = inconclusive` 是**合法且必须保留**的结果。`TRACK2_PROJECT.md` §8 禁止只记录正面结果。

---

## 4. 指标口径

### 4.1 两级目标与 guardrail（冻结，禁止事后更换）

| 角色 | 指标 | 说明 |
| --- | --- | --- |
| **候选排序主指标** | scan-stage / IO critical-path wall-clock 的 **median** | 与布局因果关系最直接 |
| **最终验收主指标** | 完整 workload end-to-end wall-clock 的 **median** | 门禁用；≥10% 改善 |
| Guardrail | 单查询 median 回归 ≤ 10% | `TRACK2_PLAN.md` §1.2 |
| Guardrail | workload P95 / P99 回归 ≤ 5% | |
| Guardrail | 峰值内存增长 ≤ 10% | |
| Guardrail | rewrite payback 周期 | `rewrite_cost / saved_query_cost_per_period` |
| 代价指标（必须同时报告） | GET 数、remote bytes、读放大、重试次数、**浪费字节** | 浪费字节来自 Range 合并 |
| 代价指标 | CPU、解压/解码时间、rewrite bytes、rewrite time、storage delta | |
| 解释指标（**不得作为目标**） | skipped rows、files / row groups / pages touched、min/max / page / Bloom / dictionary pruning | |
| **禁止作为优化目标** | cache hit rate、单独的 remote bytes、单独的 skipped rows | `PROJECT3.md` §3.1、`TRACK2_PROJECT.md` §1.3 |

### 4.2 重复与统计口径

- 每个配置**至少 5 次独立运行**，报告 **median 与分布**，不取最快一轮。
- 基线重复运行的**变异系数 CV < 5%**，否则先修环境再继续实验（见 §1.2 带宽风险）。
- 报告改善时同时给出**绝对 wall-clock**，不只给相对百分比（`TRACK2_PROJECT.md` §7.4）。
- 归档：原始 CSV/JSON + 环境元数据 + 数据生成参数 + Writer 代码版本（git commit）。

---

## 5. 候选档位

所有档位以 parquet-mr 配置项表达（D-6）。

### 5.1 PTO 兼容动作（首轮）

| 动作族 | 配置项 | 档位 | 备注 |
| --- | --- | --- | --- |
| partition | `partitionBy` | `{none, l_shipdate:year, l_shipdate:month, o_orderdate:year}` | 含 `none` 保守候选（`TRACK2_PROJECT.md` §7.6） |
| target file size | `maxRecordsPerFile` / `repartition` | `{128MB, 256MB, 512MB, 1GB}` | 实际大小分布由 footer 回读校验 |
| row-group size | `parquet.block.size` | `{16MB, 32MB, 64MB, 128MB, 256MB}` | **字节**，128MB 为基线，无需换算 |
| sort | `ORDER BY` / `sortWithinPartitions` | `{none, [l_shipdate], [l_shipdate, l_suppkey], [l_orderkey]}` | 真实排序；按谓词频率从 E1 语义层导出 |

### 5.2 扩展动作（M5 消融）

| 动作族 | 配置项 | 档位 | 备注 |
| --- | --- | --- | --- |
| page size | `parquet.page.size` | `{256KB, 1MB, 8MB}` | 1MB 为基线 |
| page 行数上限 | `parquet.page.row.count.limit` | `{5000, 20000, 100000}` | 20000 为基线；与 page size 共同决定裁剪粒度 |
| bloom filter | `parquet.bloom.filter.enabled#col` + `expected.ndv#col` | `{off, on@等值谓词列}` | off 为基线；`ndv` 由实测 NDV 设定 |
| dictionary | `parquet.enable.dictionary#col` | per-column `{on, off}` | on 为基线；仅对高 NDV 长字符串列试 off |
| compression | `parquet.compression` | `{snappy, zstd, none}` | **全局**，见 §6.2 M-5 |
| writer version | `parquet.writer.version` | `{PARQUET_1_0, PARQUET_2_0}` | 影响 page header 与 level 编码 |
| 列顺序 | `select(...)` 列序 | 待宽表专项定义 | 不进入首轮门禁，但本栈可表达（§6.2 M-3） |

> **page index 不是候选动作**：parquet-mr 恒写 ColumnIndex/OffsetIndex 且无关闭开关（§6.2 M-1）。
> 页级裁剪的可调量是 **page size 与 page 行数上限**，即裁剪**粒度**，而非索引的有无。

### 5.3 档位合法性约束（L0 静态检查）

候选在进入 What-if 之前必须通过：

1. **Writer 能力**：目标参数在冻结的 parquet-mr 版本上可用（由 §6.3 探针确认，不靠文档假定）。
2. **Reader 能力**：产生的结构会被 §1.4 冻结的 Reader 实际消费（见 §6.2 的错配清单）。
3. **结构保真**：候选产生的文件数 ≥ 2 且 row group 数 ≥ 4，否则该档位在 SF100 上无意义
   （`TRACK2_PLAN.md` §5.3：样本不能简单按采样率同比缩小）。
4. **预算**：额外存储、rewrite bytes、实验机时在预算内。
5. **单调性检查**：`parquet.block.size ≤ target_file_size`；`parquet.page.size ≤ parquet.block.size`。

---

## 6. Writer × Reader 能力矩阵

### 6.1 矩阵

栈：**Spark 4.1.x / parquet-mr 1.15.x（Writer 与 Reader 同库）+ S3A 3.4.2**。

| 能力 | Writer 配置 | Writer 默认 | Reader 配置 | Reader 默认 | 结论 |
| --- | --- | --- | --- | --- | --- |
| row group min/max | `parquet.column.statistics.enabled[#col]` | 开 | `parquet.filter.stats.enabled` | 开 | ✅ 默认自洽 |
| ColumnIndex / OffsetIndex | **无开关，恒写** | 开 | `parquet.filter.columnindex.enabled` | 开 | ✅ 默认自洽（M-1：不是可调动作） |
| Bloom filter | `parquet.bloom.filter.enabled[#col]` | **关** | `parquet.filter.bloom.enabled` | 开 | ⚠ **M-2**：需显式开启写侧 |
| Dictionary + filtering | `parquet.enable.dictionary[#col]` | 开 | `parquet.filter.dictionary.enabled` | 开 | ✅ 默认自洽 |
| 排序 | `ORDER BY` / `sortWithinPartitions` | 无 | 经 min/max 生效 | — | ✅ 真实排序（M-4 消失） |
| Vectored IO / Range 合并 | 不适用 | — | `parquet.hadoop.vectored.io.enabled` | 1.15=**关**，1.16+=开 | ⚠ **M-3**：必须显式冻结 |
| per-column compression | ❌ `parquet.compression` 仅全局 | snappy | — | — | ⚠ **M-5**：不可表达 |
| per-column encoding | ❌ 无直接开关 | — | — | — | ⚠ **M-5** |
| row group 尺寸单位 | `parquet.block.size` = **字节** | 128MB | — | — | ✅ 无需换算 |
| 文件级裁剪（分区） | `partitionBy` | — | Spark 分区发现 | — | ✅ 可用 |

### 6.2 已识别的能力错配

改用同库 Writer 后，r1 中的 **M-1（page index 默认值相反）** 与 **M-4（`sorting_columns` 只是声明）
两项跨库错配已消失**。剩余项如下：

**M-1（性质改变）—— ColumnIndex/OffsetIndex 恒写，不可关闭。**
parquet-mr 自 1.11 起始终写页索引，README 中没有任何禁用开关（`parquet.columnindex.truncate.length`
只控制截断长度）。因此在本栈中 **page index 不是 Writer 动作，而是固定能力**。
→ 处置：§5.2 已把可调量改为 page size 与 page 行数上限（裁剪粒度）。
若确需「无页索引」对照，只能通过 `parquet.column.statistics.enabled=false` 间接实现，
但它会同时抹掉 row-group min/max，**不是干净的页级消融**，只能作为上界参考并明确标注。

**M-2 —— Bloom filter 写侧默认关、读侧默认开。**
读侧 `parquet.filter.bloom.enabled` 默认 `true`，但写侧 `parquet.bloom.filter.enabled` 默认 `false`。
这是**库内既定设计**（bloom 需要预先知道 NDV 才能定尺寸），不是跨库错配，但后果相同：
不显式开启则 bloom 跳过永不发生，实验会呈现为「bloom 无收益」这一错误的负结果。
→ 处置：bloom 作为显式扩展动作，`expected.ndv#col` 由实测 NDV 设定；
启用后必须从 footer 确认 `bloom_filter_offset` 非空，并在 trace 中确认确实发生了 row group 跳过
（`TRACK2_PROJECT.md` §7.9 要求「用 trace 证明发生了跳过」）。

**M-3 —— vectored IO 的默认值随 parquet 版本翻转，必须显式冻结。**
`parquet.hadoop.vectored.io.enabled` 于 parquet-mr **1.14.0** 引入，**1.15.x 默认 `false`，1.16.0 起默认 `true`**。
若不显式设置，同一份实验代码在不同 parquet 小版本上会得到**不同的 Reader Range 合并行为**，实验不可比。
→ 处置：§1.4 已冻结为**显式 `true`**，并固定 S3A 的三个合并参数。理由：

- 这是现代 Reader 的真实行为，也让本栈与 PTO 的对照更贴近实际部署；
- 它使**列顺序成为可表达的动作**（Range 合并会发生，列的物理邻近性才有意义），
  恢复了 Spark 3.5 栈下先验无效的一整类动作——这正是升级到 Spark 4.x 的附带收益。

  代价是归因从 1:1 变为 1:N（一次 GET 覆盖多个 column chunk）。归因仍然精确，
  §3.2 已相应改为 `column_chunks` 数组并新增 `bytes_wasted`。
  E1 的覆盖率目标据此表述为「≥95% 的 GET 可映射到一个**已知的** column chunk 集合」。

  按 `TRACK2_PLAN.md` §4.2，vectored IO 的开/关对比只作为**敏感性分析**单独进行，
  绝不与 Writer 布局同时变化。

**M-5 —— parquet-mr 不支持 per-column 压缩与编码。**
`parquet.compression` 是全局设置；per-column codec 无法表达。
→ 处置：per-column 压缩/编码属于 §5.2 的 M5 扩展动作，不影响首轮。
若 M5 确需该动作，用 PyArrow（`compression` / `column_encoding` 支持 per-column dict）作为逃生舱写出对照数据，
**并必须明确标注该组数据的 writer 与主线不同**，其结论不得与 parquet-mr 写出的候选直接混合排名。

### 6.3 能力探针

上述矩阵**不得仅凭文档采信**。E0 必须运行 `tools/track2/probe_capability.py`，
用实际安装的 Spark / parquet-mr / Hadoop / PyArrow 版本逐项验证，输出机器可读结果并归档。
探针失败的能力，其对应候选动作在 §5.3 的 L0 检查中直接判为不可行。

### 6.4 能力矩阵的 writer 维度

§6.1 是 **parquet-mr 这一个 writer** 的矩阵。真实湖仓里 Writer 未必是 Spark，
因此能力矩阵是 `能力 × writer` 的二维表，而不是全局常量。当前已知的三个 writer 对照如下
（**仅 parquet-mr 一列在 M1–M4 中实际使用**，其余两列为 §11 的备选项做准备）：

| 能力 | parquet-mr（Spark / Flink） | Trino 原生 writer | PyArrow |
| --- | --- | --- | --- |
| row group 尺寸 | `parquet.block.size`（字节） | `parquet.writer.block-size` | `row_group_size`（**行数**） |
| page 尺寸 | `parquet.page.size` | `parquet.writer.page-size` | `data_page_size` |
| page 行数上限 | `parquet.page.row.count.limit` | `parquet.writer.page-value-count`（默认 80000） | `max_rows_per_page` |
| **ColumnIndex / OffsetIndex** | **恒写，无开关** | **不写**（trinodb/trino#9359 未完成） | `write_page_index`，默认**关** |
| Bloom filter | `parquet.bloom.filter.enabled#col` | 支持，属性名与 parquet-mr 一致 | `bloom_filter_options` |
| per-column dictionary | `parquet.enable.dictionary#col` | 无直接开关 | `use_dictionary`（per-column） |
| per-column 压缩 | ❌ 仅全局 | ❌ 仅全局 | ✅ per-column |
| per-column 统计 | `parquet.column.statistics.enabled#col` | 无直接开关 | `write_statistics` |

**这张表本身就是一个值得写进论文的发现**：三个主流 Parquet writer 在**页级索引**这一项上行为完全不同
（恒写 / 不写 / 默认不写）。这说明 `TRACK2_PROJECT.md` §7.9 提出的
「必须建立 Writer × Reader capability matrix」不是工程上的繁文缛节——
同一条布局建议在不同 writer 上可能根本无法生效，而失效方式是**静默的**。

> **⚠ Trino 的页索引缺口是硬约束**：Trino 原生 writer 不写 ColumnIndex/OffsetIndex，
> 因此任何依赖页级裁剪的建议在 Trino 写出的数据上**先验无效**。
> 若将来接入 Trino，L0 检查必须据此直接否决该类候选，而不是让实验跑出一个无法解释的负结果。

---

## 7. 实验清单

| ID | 实验 | 里程碑 | 目的 | 通过判据 |
| --- | --- | --- | --- | --- |
| **E0** | 环境与能力验收 | M1 | 版本归档、拦截器装载验证、能力探针、vectored IO 生效确认、持续带宽实测 | 拦截器采到数据；能力矩阵逐项实测；带宽与方差已记录 |
| **E1** | 三层观测关联覆盖率 | M1 | 验证 GET → query → object → row group / column chunk 集合 的映射 | `attribution_level ≥ column_chunk` 占比 **≥95%**；page 级覆盖率单独报告 |
| **E2** | 默认布局 SF100 基线 | M1 | 建立基线 | 5 次运行 **CV < 5%**；逐查询 median 与分布归档 |
| **E3** | 单旋钮布局矩阵 | M2 | partition / TFS / RGS / sort 各自单独变化 | 每档位 5 次运行；结果可复现 |
| **E4** | 目标相关性 | M2 | skipped rows、bytes、GET、scan wall-clock、end-to-end wall-clock 的 rank correlation | 判定哪些指标可作代理；结论写入 `TRACK2_PROJECT.md` |
| **E5** | L0/L1 What-if 校准 | M2 | 解析模型的候选排序能力 | top-3 含实测最佳，或 selection regret **≤5%**；否则进 L2 残差校准 |
| **E6** | PTO 四参数联合候选 | M3 | 联合布局 | 相对单旋钮最优有增量；PTO 作为 baseline |
| **E7** | 建议包 + Code Diff 闭环 | M3 | 采集→分析→建议→Diff→重写→复测全链路 | 全链路可重复；产出一次真实 Diff |
| **E8** | **SF100 门禁** | M4 | 最终验收 | end-to-end wall-clock median 改善 **≥10%** 且全部 guardrail 通过 |
| **E9** | 扩展动作消融 | M5 | Default → PTO 四参数 → +page 粒度/Bloom/dictionary 的递增消融 | 证明扩展动作的增量价值 |
| **E10** | ATUN binary vs empirical branch | M5 | 成本分支细化是否值得 | 仅当稳定降低 selection regret 才保留；否则记录为负结果 |
| **E11** | vectored IO 敏感性 | M5 | Range 合并开/关对布局收益排序的影响 | 单独进行，不与 Writer 布局同时变化 |

**E8 未通过即止损**：Track 2 降为论文 discussion，保留 Advisor 架构、成本模型误差和负结果，
不再扩张布局动作空间（`TRACK2_PLAN.md` §7.1）。

---

## 8. 代码落点（D-7）

```text
tools/track2/
├── probe_capability.py      # §6.3 能力探针，E0 必跑
├── gen_tpch.py              # DuckDB dbgen SF100 → 规范源数据
├── write_layout.py          # Spark writer 配置与 DataFrame 变换；★ Code Diff 的目标文件
├── parse_footer.py          # FormatMetadataCollector：footer → RG/chunk/page 映射（PyArrow，只读）
├── collect_semantic.py      # SemanticCollector：Spark 事件日志/plan → scan fragment
├── correlate.py             # 三层关联 → ObservationBundle
├── analyze_layout.py        # 共访问矩阵、切碎率、冷字段、候选生成
└── whatif.py                # L0 静态检查 + L1 解析模型

services-custom/s3-adaptive-range-reader/src/main/java/software/amazon/awssdk/s3/adaptive/telemetry/
├── Track2IoCollectorInterceptor.java   # SdkIoCollector；仅实现 ExecutionInterceptor
└── Track2CollectorSetting.java         # 采集器配置（系统属性或环境变量）

docs/adaptive-range-reader/results/track2/<cloud>_<date>/   # 结果归档
```

**依赖约定（r4 修订）**：拦截器**不依赖 Hadoop**。S3A 只要求实现 `ExecutionInterceptor`，
`org.apache.hadoop.conf.Configurable` 是可选的；采集器只需要自己的输出配置，
故改由 `SystemSetting` 读取（同时支持系统属性与 `spark.executorEnv.*` 环境变量）。
收益是模块 pom 无需改动、与 Hadoop 版本完全解耦、同一个 jar 可用于任意 S3A 发行版。
运行时它只引用 `software.amazon.awssdk.core.interceptor.*` 与 `utils`，
这些类由 Hadoop 捆绑的 AWS SDK v2 bundle 提供（bundle 不重命名自身包名），因此**无需 shading**。
拦截器以 Java 8 为目标编译，在 Spark 4.x 的 JDK 17 上运行无碍；E0 中记录 SDK 与 bundle 两者版本号。

**两点实现约束**（由 Hadoop 审计文档确认，影响 §3.1 与 §3.2）：

1. **审计头的 `ta` / `ji` 在读路径上不可用**。Hadoop 文档明确说明 Task Attempt / Job ID
   **仅在 S3A committer 参与的操作中设置**，即写路径。E1 全为读，故关联必须依赖
   查询引擎自行注入 `CommonAuditContext` 的键（默认 `sqlid`），S3A 会像序列化 `ji` / `ta`
   一样把它带入 Referer 头。**注入侧尚未实现**，是 E1 的前置阻塞项。
2. **`latency_ns` 的语义是 TTFB，不是完整传输时间**。拦截器在响应头到达时被回调，
   而 `GetObject` 的响应体由调用方随后流式消费。该值恰好是成本模型所需的 RTT 分量，
   但**不得作为端到端传输时间报告**。

---

## 9. 未决项与风险

M0 门禁要求「主目标与边界无未决项」。以下均**不影响主目标与边界**，属于执行期标定项：

| # | 未决项 | 处置 | 期限 |
| --- | --- | --- | --- |
| O-1 | Spark 4.1.x 具体小版本、parquet-mr / Hadoop / AWS SDK bundle 实际版本号 | E0 记录后回填 §1.1 | M1 |
| O-2 | vectored IO 是否在 Spark 向量化 Parquet 读路径上真正生效 | E0 用 GET 合并证据确认；未生效则回退 M-3 处置并记录 | M1 |
| O-3 | Spark 并行度、executor 规格、S3A 线程数的标定值 | E0 标定后回填 §1.4 并全程固定 | M1 |
| O-4 | 默认写出的实际文件大小分布 | E0 从 footer 回读记录 | M1 |
| O-5 | `m5d.4xlarge` 突发带宽是否满足 CV<5% | E0 实测；不达标则换实例类型，**不放宽 CV 门槛** | M1 |
| O-6 | 主路径（`CommonAuditContext` 注入）的实际覆盖率 | E1 与兜底路径分别报告 | M1 |

> r1 的 O-4（PyArrow `row_group_size` 行数↔字节换算）已因 D-6 改用 `parquet.block.size`（字节）而**取消**。

**已知风险**：

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| 突发带宽耗尽导致方差超标 | CV 门禁不过，M1 延期 | E0 前置实测；必要时换实例 |
| M-2 未被发现（bloom 写侧没开） | 产出错误的负结果 | §6.3 探针 + footer `bloom_filter_offset` 回读校验 |
| vectored IO 未真正生效 | Range 合并不发生，列顺序动作再次失效 | O-2 在 E0 用实际 GET 合并证据确认 |
| Spark 4.x 与既有工具链的兼容性（Scala 2.13 / JDK 17） | P0 延误 | 周末小规模验证时一并确认 |
| SF100 生成与上传耗时超预期 | 挤压 M2–M4 | 生成与上传在 M1 并行启动 |

---

## 10. M0 门禁自检

| 冻结项 | 状态 |
| --- | --- |
| 主目标（两级 wall-clock）已冻结且不可事后更换 | ✅ §4.1 |
| 边界（非侵入 / 建议式 / 可验证 / 不以 AI 代替性能判断） | ✅ 继承 `TRACK2_PROJECT.md` §1.3 |
| TPC-H 查询集、频率 profile、SF100 生成规则 | ✅ §2 |
| Reader 版本与配置冻结 | ✅ §1.4 |
| `ObservationBundle` / `LayoutCandidate` / `RecommendationPackage` | ✅ §3 |
| Writer × Reader capability matrix | ✅ §6（实测由 E0 完成） |
| 默认布局、候选参数档位、主 profile、指标口径 | ✅ §2.3、§5、§1.4、§4 |
| 实验清单 | ✅ §7 |

---

## 11. 跨引擎 Writer 可移植性（D-12）

### 11.1 结论：现在不实现，但现在就把成本锁低

实际湖仓的 Writer 可能是 Spark、Flink 或 Trino。**Flink/Trino 适配器不进入 M1–M4，列为备选项**：
它不在关键路径上（M4 门禁在 8 月 28 日），而且没有它并不影响 §4 的主目标与 §7 的实验清单。

但有一个观察让这件事比看上去便宜得多：

> **布局动作空间是格式级的，不是引擎级的。**
> row group 尺寸、page 尺寸、排序、分区、bloom、dictionary、压缩都是 **Parquet 文件的属性**，
> 不是 Spark 的属性。换引擎不会改变动作空间，只会改变**同一个动作怎么写出来**。

因此三层 collector 中只有 `SemanticCollector` 是引擎相关的；
`SdkIoCollector`（物理 IO）与 `FormatMetadataCollector`（footer）天然引擎无关，
`WhatIfEvaluator`、`ConstraintChecker`、`CandidateSelector` 也全部作用在格式与物理层。

### 11.2 现在就做的两个零成本决策

**(1) 候选动作用引擎中立的规范名表达。**
不自创词表，直接对齐 **Iceberg 表属性**——它已经是经过设计的引擎中立词汇，
且 Spark / Flink / Trino 写 Iceberg 表时都遵守它。规范名到各 writer 的映射：

| canonical（Iceberg 词表） | parquet-mr（当前使用） | Trino | 备注 |
| --- | --- | --- | --- |
| `write.parquet.row-group-size-bytes` | `parquet.block.size` | `parquet.writer.block-size` | 默认 128MB |
| `write.parquet.page-size-bytes` | `parquet.page.size` | `parquet.writer.page-size` | 默认 1MB |
| `write.parquet.page-row-limit` | `parquet.page.row.count.limit` | `parquet.writer.page-value-count` | 默认 20000 / 80000 |
| `write.parquet.compression-codec` | `parquet.compression` | 会话属性 | Iceberg 默认 zstd |
| `write.parquet.bloom-filter-enabled.column.X` | `parquet.bloom.filter.enabled#X` | 同名支持 | |
| `write.parquet.bloom-filter-ndv.column.X` | `parquet.bloom.filter.expected.ndv#X` | 同名支持 | |
| `write.parquet.bloom-filter-fpp.column.X` | `parquet.bloom.filter.fpp#X` | 同名支持 | |
| `write.parquet.dict-encoding-enabled.column.X` | `parquet.enable.dictionary#X` | 无 | |
| `write.parquet.stats-enabled.column.X` | `parquet.column.statistics.enabled#X` | 无 | |
| `write.target-file-size-bytes` | `maxRecordsPerFile` / `repartition` | `target_max_file_size` | |
| `sort.columns` | `ORDER BY` / `sortWithinPartitions` | `ORDER BY` | 非属性，是变换 |
| `partition.spec` | `partitionBy` | `partitioning` | 非属性，是变换 |

`LayoutCandidate.actions[]` 因此同时携带 `canonical` 与 `rendered`（§3.3）。
**分析、What-if、约束检查、候选选择一律只看 `canonical`**；只有 `RecommendationEmitter`
在生成 Code Diff 的最后一步才做 rendering。

**(2) 能力矩阵增加 writer 维度**（§6.4 已完成）。

这两项现在做几乎不花时间；等分析器、What-if 和 Emitter 全部写成 Spark 字符串之后再回头改，
就是一次真正的重构。这是典型的低成本保留选项。

### 11.3 备选项的具体范围（不排期）

若将来决定接入，需要且仅需要：

| 工作项 | 规模 | 说明 |
| --- | --- | --- |
| Flink `SemanticCollector` | 中 | Flink 用 parquet-mr，**writer 侧无需改动**；只缺 plan/event adapter |
| Trino `SemanticCollector` | 中 | Trino 有 event listener，可拿到 scan fragment |
| Trino patch renderer | 小 | 会话属性 / 表属性映射，见 §11.2 |
| Trino 能力约束 | 小 | L0 需否决页级裁剪类候选（§6.4） |
| Iceberg 表属性 renderer | 小 | 一次写好即同时覆盖三个引擎 |

**最省力的路径是走 Iceberg 表属性**：写一个 renderer 就同时对 Spark / Flink / Trino 生效，
而且与 PTO 的动作落点完全一致。

**为什么 MVP 仍用裸 Parquet 而不是 Iceberg**：Iceberg 会引入 manifest / metadata 的额外 GET，
这些请求也会进入 SDK telemetry 并需要归因，直接抬高 E1 的 ≥95% 覆盖率难度；
而动作空间与裸 Parquet 完全相同。因此 MVP 保持裸 Parquet 以保证因果归因最干净，
Iceberg 留给「需要实证跨引擎可移植性」的时候。

---

## 12. 修订记录

### r3（2026-08-07）—— 跨引擎 Writer 可移植性定为备选项，动作改用引擎中立规范名

**触发**：实际湖仓的 Writer 未必是 Spark，也可能是 Flink 或 Trino，是否需要扩展能力。

**结论**：Flink/Trino 适配器**列为备选项，不进入 M1–M4**（不在关键路径上）。
但同时做两个零成本决策把将来的成本锁低：候选动作改用对齐 Iceberg 属性词表的**引擎中立规范名**
（`LayoutCandidate.actions[]` 增加 `canonical` / `rendered`），能力矩阵增加 **writer 维度**（§6.4）。
详见 §11。

**关键技术发现**：三个主流 Parquet writer 在**页级索引**上的行为完全不同——
parquet-mr 恒写、Trino 原生 writer **不写**（trinodb/trino#9359 未完成）、PyArrow 默认不写。
这使「Writer × Reader capability matrix」从工程规范升级为**有实证支撑的必要概念**。

### r2（2026-08-07）—— 引擎升至 Spark 4.1.x，Writer 改为 parquet-mr

**触发**：审阅 r1 时提出两个问题——(a) 既然 Spark 3.5 引入两个版本问题，为何不升版本；
(b) 既然 PyArrow 默认布局弱于 parquet-mr，为何仍用 PyArrow 作 Writer。

**变更**：

| 项 | r1 | r2 |
| --- | --- | --- |
| 引擎 | Spark 3.5.x + 手工拼 Hadoop 3.4.1 | **Spark 4.1.x**（自带 Hadoop 3.4.2 + parquet-mr 1.15.x） |
| Writer | PyArrow | **Spark / parquet-mr** |
| PyArrow 角色 | Writer + footer 解析 | 仅 footer 解析（只读）+ M-5 逃生舱 |
| 基线 | 人工对齐 parquet-mr 默认值 | 库默认值 `df.write.parquet(...)` |
| Range 合并 | 不可能（parquet 1.13.1 无 vectored IO） | 显式开启并冻结 |
| 列顺序 | 本栈先验无效 | 可表达，保留为 M5 动作 |

**消除的问题**：r1 的 M-1（page index 写读默认值相反）与 M-4（`sorting_columns` 只是声明）
两项**跨库错配随同库 Writer 一并消失**；r1 的 O-4（行数↔字节换算）因 `parquet.block.size` 以字节计而取消；
r1 的首要风险「误用 Spark 默认发行版导致采集静默失效」因 Spark 4.x 自带 Hadoop 3.4.2 而消失。

**新增的约束**：vectored IO 的默认值在 parquet 1.15/1.16 之间翻转，必须显式冻结（M-3）；
parquet-mr 无法表达 per-column 压缩与编码（M-5）。

**Spark 3.6 不存在**：3.x 线到 3.5 终止（extended LTS 至 2027-11），之后直接进入 4.x。
4.0.x 于 2026-11-23 EOL，实验期内失去支持；4.2.0 过新；故选 4.1.x。
