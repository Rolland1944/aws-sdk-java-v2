# Track 2 v2：面向文件格式层的 Parquet 布局计划生成

状态：现行方案（取代 v1 的四维布局建议）
上游文档：[TRACK2_M0_CONTRACT.md](TRACK2_M0_CONTRACT.md) r5、[TRACK2_PROJECT.md](TRACK2_PROJECT.md)
被取代：[track2_report_new.md](track2_report_new.md) 描述的 v1 架构（保留为历史记录）

---

## 1. 范围变更

v1 的形态是：Semantic(Spark eventlog) + Format(footer) + Physical(SDK) 三层采集，
动作空间 `partition × file size × row group size × sort`，全部经 Spark DataFrameWriter 落地。

v2 做四处收敛：

1. **采集降为两层**：只用 SDK 字节访问 + Parquet footer。Semantic 层从计划生成链路中彻底移除。
   理由是它绑定 SQL 引擎，且与 SDK 能看到的字节信息关联度低；移除之后同一套方法对非 SQL 负载
   （向量检索、ML data loading）也成立。
2. **动作空间换成文件格式层六维**：列顺序、行组大小、目标文件大小、压缩开关、页大小/页行数上限、
   encoding 族。`sort.columns` 与 `partition.spec` 移出动作空间——流式到达的数据无法全局排序，
   分区是表级高层概念，两者都偏离 SDK+footer 主线。
3. **产出拆成两层**：第一层产出引擎无关的文件级中间优化计划（研究重点）；第二层按 use case
   把计划翻译成落地改动（不追求精益求精）。
4. **先确定性算法，后 LLM**：只有确定性算法先跑通并证明有效，LLM 才有上的必要。

硬约束：产出仍是标准 Parquet，能被 parquet-mr 和 PyArrow 正常读取，不更换底层格式库，
不重写或重编译 parquet-mr / Arrow / Spark。

---

## 2. 两层架构

```text
SdkIoCollector NDJSON ─┐
                       ├─→ correlate（GET ∩ column chunk）
Parquet footer ────────┘        ↓
                            access_profile
                                ↓
                       第一层：计划生成器
                       （确定性算法；LLM 为后续对照）
                                ↓
                        中间优化计划 JSON
                       （引擎无关 canonical actions）
                                ↓
                          L0 可行性校验
                                ↓
                          L1 代价模型估价
                                ↓
                ┌───────────────┴───────────────┐
        第二层 UC1                        第二层 UC2
   手写调用 PyArrow writer            Spark SQL + 参数
                └───────────────┬───────────────┘
                                ↓
                          重写 + 复测
```

第一层与查询引擎无关，是论文主线。第二层是工程翻译，允许人工或 LLM 辅助。

---

## 3. 采集层

### 3.1 保留不动

- **Physical**：`Track2IoCollectorInterceptor.java` 经 `fs.s3a.audit.execution.interceptors` 注入，
  记录 `range_offset / range_length / ts_wall_ms / ts_start_ns / thread / method`。
  注入方式不需要重编译 Spark，天然符合 v2 的约束。
- **Format**：`parse_footer.py` 的 per-column-chunk 记录
  （`byte_start / byte_end / compressed_bytes / encodings / compression / has_*_index`）。

### 3.2 correlate：去掉 Semantic 依赖

`--semantic` 从必填改为可选。关联退化为纯几何：

```text
GET [offset, offset+length)  ∩  column chunk [byte_start, byte_end)
        → (file, row_group, column, overlap_bytes)
```

查询身份用 **access episode** 替代。一个 episode 是同一 `(thread, object)` 上时间间隔不超过
`--episode-gap-ms` 的连续请求序列。它不依赖任何引擎语义，因此对 SQL 与非 SQL 负载一致成立。
episode 就是 v2 里"一次逻辑读取"的单位，取代了 v1 的 `execution_id`。

覆盖率门禁相应改为 **chunk 归属覆盖率**：落进任一 column chunk 的 GET 字节占比。
footer/metadata 尾部读取单独计数，不算作未归属。

### 3.3 access_profile：新的证据层

`access_profile.py` 取代 `workload_snapshot.py`，从 correlate 的 observation 产出：

- 每列读取频次、远端字节、被跳过比例；
- **列-列共访问矩阵**：同一 episode 内共同出现的列对，按 episode 权重累加；
- 请求尺寸直方图、每文件 HEAD/footer 开销、每 row group 平均触达列数；
- 复用 `sysconst.py` 拟合的 `(RTT, BW, K)`。

共访问矩阵是列顺序动作的唯一证据来源。

---

## 4. 六维动作空间

六个动作都是现有公开 API 的构造参数或写入前的 schema/文件旋转。

| 维 | Canonical | 改什么 | UC1 (PyArrow) | UC2 (Spark) |
| --- | --- | --- | --- | --- |
| 1 列顺序 | `write.parquet.column-order` | schema 字段顺序 | 重排 `pa.schema` | `df.select(*order)` |
| 2 行组大小 | `write.parquet.row-group-size-bytes` | `row_group_size` / `parquet.block.size` | 是 | 是 |
| 3 文件大小 | `write.target-file-size-bytes` | 何时 `close()` 再开新文件 | 是 | `repartition(n)` |
| 4 压缩 | `write.parquet.compression-codec`(+`.column.X`) | `compression=` / `parquet.compression` | 全局 + per-column | 仅全局 |
| 5 页几何 | `write.parquet.page-size-bytes`、`write.parquet.page-row-limit` | `data_page_size` / `parquet.page.size` | 是 | 是 |
| 6 encoding 族 | `write.parquet.encoding.column.X` | `PLAIN` / `RLE_DICTIONARY` / `DELTA_BINARY_PACKED` / `DELTA_BYTE_ARRAY` / `BYTE_STREAM_SPLIT` | `column_encoding` | 仅字典开/关 |

### 4.1 页级索引不是第 7 维

UC1 固定 `write_page_index=True`，parquet-mr 本身恒写。`--verify` 检查 footer 里
`has_column_index` / `has_offset_index` 为真，缺失判写出失败。这是**校验项，不是搜索维**。

### 4.2 固定或排除

- 列统计：默认全开；
- Data page v2 / writer version：固定默认；
- 字典页大小上限：并入 encoding 维的约束（字典失效退回 PLAIN），不单独成维；
- Bloom filter：能写但不搜——决策依赖等值谓词，偏离 SDK+footer 主线；
- 表分区、全局排序、单批次内存排序：不是文件内部格式，或已被明确否决。

### 4.3 中间计划 schema

```json
{
  "plan_id": "hits-v2-001",
  "target": {"table": "hits"},
  "generator": "deterministic",
  "evidence": {"source": ["sdk_io", "parquet_footer"], "episodes": 1043},
  "actions": [
    {"canonical": "write.parquet.column-order", "value": ["EventDate", "CounterID"]},
    {"canonical": "write.parquet.row-group-size-bytes", "value": 33554432},
    {"canonical": "write.target-file-size-bytes", "value": 268435456},
    {"canonical": "write.parquet.compression-codec", "value": "zstd"},
    {"canonical": "write.parquet.page-size-bytes", "value": 1048576},
    {"canonical": "write.parquet.page-row-limit", "value": 20000},
    {"canonical": "write.parquet.encoding.column.EventDate", "value": "DELTA_BINARY_PACKED"}
  ],
  "constraints": {
    "format": "parquet",
    "page_index": "required_on",
    "readable_by": ["parquet-mr", "pyarrow"]
  }
}
```

所有动作都可带 `"table"` 字段做 per-table scope，与 v1 一致。

---

## 5. 列顺序：核心动作

### 5.1 为什么可行

文件里 schema 的列顺序**无需**与建表 metadata 的顺序一致。查询时引擎按列名匹配，
不校验顺序。因此列顺序调整对上层计算完全透明，是"只改一小段代码"就能拿到的存储优化。

### 5.2 收益机制

`virtual_footer.merge_gets()` 已经按 schema order 走列、按 vectored-IO 规则
（`min_seek=131072`、`max_merged=2097152`）合并相邻 chunk。v2 把它改成接受**候选顺序**参数。

优化目标：让同一 episode 内共读的列相邻，被 vectored read 合并成更少的 range；
同时冷列聚成大段连续空洞被整体跳过。

### 5.3 前提

栈为 parquet-mr 1.15.x + 已开启 vectored read，满足旧文档记录的
"列顺序需 parquet-mr ≥ 1.14 + vectored IO"门槛。

### 5.4 负载选择

主验证负载是 **ClickBench `hits`**（约 105 列）。TPC-H lineitem 只有 16 列，
按 `TRACK2_PROJECT.md` 的记录不适合作为列顺序主负载，退为 rg/file/压缩维度的验证集。

---

## 6. L0 / L1 改造

### 6.1 L0

删除（依赖已移除的谓词/排序语义）：

- Gate A（基线聚簇度余量）
- Gate C（剪枝后并行度）
- Gate D（分区有效性）
- 排序 NDV 文件数上界

保留：writer/reader 能力校验、Row Group ≤ 128 MiB 可读性上界、文件数/RG 数几何约束。

新增：

- 列顺序必须是原 schema 的一个排列（不增删列）；
- 压缩 codec 与 encoding 必须在目标 writer/reader 能力矩阵内，且与物理类型匹配
  （`BYTE_STREAM_SPLIT` 仅浮点，`DELTA_BINARY_PACKED` 仅整数，`DELTA_BYTE_ARRAY` 仅变长字节）；
- `page_size ≤ row_group_size`，`page_row_limit` 为正。

### 6.2 L1

删除：剪枝选择率模型、`partitionBy` 的 F×P 几何、排序 NDV 文件数上界。

保留并改造：

- `merge_gets` 改为 order-aware，是列顺序收益的主要来源；
- 页几何进入模型：更小的 page 提高 OffsetIndex 可跳过比例，但增加 GET 数；
- 压缩比 + encoding：对每列采样后用各 codec/encoding 组合做小文件实测，再外推全表字节；
- I/O 公式 `(request_count × RTT + bytes / BW) / k` 不变。

---

## 7. 第二层：两个 use case

### UC1 — 手写调用 PyArrow writer

`write_layout_pyarrow.py`。不 fork Arrow，只用公开 `pq.ParquetWriter`：
重排后的 `schema`、`row_group_size`、按目标大小旋转文件、`compression`、
`data_page_size`、`column_encoding`。`write_page_index=True` 写死。

这条路径能完整表达 per-column 压缩与 encoding 族，是两个 use case 的关键差异点。

### UC2 — Spark SQL + 参数

`write_layout.py`。列顺序 → `df.select(*order)` 或显式列清单的 `SELECT`；
行组/页大小/压缩 → `.option()`；文件大小 → `repartition(n)`；
encoding 族若不是"字典开/关"则跳过并记 warning（UC1-only）。
parquet-mr 恒写页索引，无需 option。全程不触碰 Spark 源码。

两者共用同一份中间计划，`--verify` 回读 footer 确认实际生效。

---

## 8. 第一层：先确定性算法

### 8.1 `plan_deterministic.py`（主生成器）

输入：access_profile + footer 摘要 + 动作词表。输出：中间计划 JSON，过 L0 / L1。

规则（可解释、可复现）：

1. **列顺序**：共访问矩阵层次聚类，簇内按字节占比贪心 seriation；热列相邻，冷列沉到文件尾。
2. **文件大小 / RG 大小**：实测几何 + 请求尺寸直方图生成有限梯子（复用
   `adaptive_physical_options.py`），order-aware `merge_gets` + `(RTT, BW, K)` 选 L1 最优，
   RG ≤ 128 MiB。
3. **压缩开关**：高 NDV / 宽字节列试 zstd，其余保持 snappy。
4. **页大小 / 页行数上限**：tiny GET 占比高则增大 page；同一 column chunk 被多次短 range
   切开则减小 page；并满足 `page_size ≤ row_group_size`。
5. **encoding 族**：按物理类型选候选，用 footer 已有 encodings 与列 NDV 剪枝。

这一层独立构成论文主线的可行性证明：SDK+footer → 计划 → 写出仍是 Parquet → 查询变快。

### 8.2 `plan_llm.py`（只有确定性有效之后才加）

同一输入 packet，LLM 产出计划，schema + L0 校验失败则带错误回灌重试。

上 LLM 的必要条件（缺一不可，否则 LLM 不进论文主线）：

- 确定性计划已先通过 E-0，并在 E-B 上相对 baseline 有稳定收益；
- LLM 在 L1 或实测上相对确定性算法仍有增量；
- 或确定性算法在宽表排列上算不动，LLM 用来缩小搜索空间。

不能反过来"先上 LLM 再补确定性"，否则无法说明大模型的必要。

---

## 9. 实验

单一主线：**仅凭对象存储 SDK 的字节访问信息与 Parquet footer，不依赖查询引擎语义，
自动生成可跨引擎落地的文件级 Parquet 写入布局计划**，其中列顺序是核心新动作。

实验分两段，**先冒烟、后消融**。E-A–E-E 在 E-0 确认墙钟下降之前不启动。

### 9.1 E-0：ClickBench SF1 墙钟冒烟（当前唯一先跑的实验）

数据：官方 `hits.parquet` 一份（`gen_clickbench.py --copies 1`，约 14 GiB）。

1. 在 SF1 基线布局上跑 ClickBench 查询，采 SDK IO + footer，生成 access_profile；
2. `plan_deterministic.py` 产出一份六维中间计划；
3. 用 UC1 或 UC2 写出新布局（页索引恒开）；
4. 同一套查询、同一 Reader 配置复测。

通过标准（只看方向，不设 10% 门禁）：

- 主指标：workload **墙钟时间** median 相对同环境 baseline **下降**；
- 辅助记录 GET 数、远端字节，用于解释，不作为本关否决项；
- 新文件必须仍是可被 parquet-mr / PyArrow 读取的 Parquet。

失败则停在确定性算法/采集/落地，不进入消融，也不上 LLM。

### 9.2 E-0 通过之后

| 实验 | 内容 |
| --- | --- |
| E-A | 列顺序透明性：文件列序 ≠ 表列序时 Spark 查询结果一致 |
| E-B | 更大负载上确定性计划的六维单因素消融 + 组合 |
| E-C | UC1/UC2 同计划双落地 |
| E-D | 跨 reader：parquet-mr 与 PyArrow 都能读、都能获益 |
| E-F | TPC-H 窄表对照（预期列顺序收益有限） |
| E-E | 最后：LLM vs 确定性 vs baseline |

`results/track2/` 下已有的 sort/partition 结果保留为历史对照，不再扩展。

---

## 10. 模块清单

| 模块 | v2 角色 |
| --- | --- |
| `Track2IoCollectorInterceptor.java` | Physical 采集，不变 |
| `parse_footer.py` | Format 采集，不变（新增 pyarrow 版本兼容） |
| `correlate.py` | 改造：去 Semantic，加 access episode |
| `access_profile.py` | 新增：列热度 + 共访问矩阵 + 访问模式 |
| `dataset_snapshot.py` | 改造：footer 里补采 `physical_type`（encoding 合法性依赖它） |
| `compression_probe.py` | 新增：按列采样实测压缩比与 encoding 收益 |
| `layout_actions.py` | 新增：六维动作词表 + render + L0，两个 renderer 共用 |
| `plan_deterministic.py` | 新增：第一层主生成器（含 `--ablation`） |
| `plan_llm.py` | 新增：第一层 LLM 对照（含前置条件检查） |
| `virtual_footer.py` | 改造：order-aware `merge_gets`，删除剪枝模型 |
| `whatif.py` | 改造：L0 去 Gate A/C/D，L1 按 access pattern 计价 |
| `advisor_catalog.py` | 改造：`QUERIES` → `PATTERNS` |
| `advisor_policy.py` | 改造：删除排序/分区门槛，新增页几何与压缩策略 |
| `write_layout.py` | 改造：UC2 渲染（`df.select` 列序、`--emit-sql`），去 sort/partition |
| `write_layout_pyarrow.py` | 新增：UC1 落地，六维全表达 + 回读校验 |
| `run_e0_smoke.py` | 新增：E-0 八阶段端到端编排 |
| `run_e1_coverage.py` | 改造：去 Semantic，覆盖率门禁改为 chunk 归属 |
| `legacy/collect_semantic.py` | 退役 |
| `legacy/predicates_from_runtime.py` | 退役 |
| `legacy/workload_snapshot.py` | 退役（被 `access_profile.py` 取代） |
| `legacy/analyze_layout.py` | 退役（候选生成移到 `plan_deterministic.py`） |
| `legacy/hand_catalog_*.py` | 退役 |
