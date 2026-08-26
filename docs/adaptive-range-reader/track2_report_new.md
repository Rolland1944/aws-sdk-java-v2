# Track 2 阶段进展汇报：面向对象存储的 Parquet 布局优化

## 1. 研究目的

在不修改 Parquet 格式和查询语义的前提下，通过改变 Parquet 文件的物理布局，减少对象存储上的文件打开、Range GET 和远端读取字节，最终缩短查询时间。

目前关注四类布局动作：

1. 分区方式（partition）；
2. 目标文件大小（target file size）；
3. Row Group 大小（row group size）；
4. 排序键（sort key）。

系统实现一个建议式的 Writer Layout Advisor：先从真实负载中收集证据，再生成和估价候选布局，输出建议 JSON，最后由 Writer 按建议重写一份新布局并复测。旧布局始终保留，是否采用建议由人决定。

目前本阶段尚未修改 writer 代码，仅通过 parquet-mr/spark 参数调节四类布局动作，也没有自动生成可提交的 code diff。

系统整体流程如下：

```text
运行时观测
  → 生成候选布局
  → 静态约束过滤
  → 不物化的成本估计
  → 推荐候选
  → Writer 重写
  → 用真实云环境复测
```

---

## 2. 三层观测：系统如何知道“慢在哪里”

AWS SDK 的 GET 日志只能看到读取了哪些字节，却不知道这些字节属于哪条查询、哪个 Row Group 或哪一列。因此系统把观测分为 Semantic、Format 和 Physical 三层。

### 2.1 Semantic：查询语义层

Semantic 层通过 Spark eventlog 和物理计划收集实际执行计划中的扫描行为：

- 查询或 SQL execution；
- `Scan parquet` 节点；
- 扫描位置 `Location`；
- 下推谓词 `PushedFilters`；
- 实际投影列 `ReadSchema`；
- 一条查询包含的多个 scan fragment；
- execution 的开始、结束时间。

### 2.2 Format：Parquet 格式层

Format 层由 PyArrow 离线解析 Parquet footer，得到静态的文件内部地图：

```text
文件
  → Row Group
      → Column Chunk
          → byte_start / byte_end
          → compressed bytes
          → min / max
          → ColumnIndex / OffsetIndex 是否存在
```

Format 解析位于离线分析路径，不在查询热路径中执行。作用是和Physical结合获得每次 GET 覆盖了哪些 Row Group 和 Column Chunk，并提供给 Advisor 计算基线聚簇度，模拟候选排序后的 Row Group 剪枝

### 2.3 Physical：对象存储请求层

Physical 层通过 AWS SDK v2 `ExecutionInterceptor` 收集真实请求：

- object key 和 version；
- GET / HEAD；
- Range offset 和 length；
- 远端字节数；
- 请求开始、结束时间和时延；
- 请求结果或异常。

Spark 通过 S3A 访问 S3，因此拦截器经由 S3A 审计机制注入。采集器不依赖 Hadoop 内部实现。

### 2.4 三层如何关联

三层使用以下信息关联：

```text
query / execution / span id
  + object key
  + object version
  + time window
  + GET 字节区间与 Column Chunk 区间重叠
```

最终可以把“Q6 很慢”逐步解释为：

```text
Q6
  → 扫描 lineitem
  → 下推了 l_shipdate / l_discount / l_quantity 谓词
  → 实际读取若干 Row Group 的相关 Column Chunk
  → 产生多少 GET、多少远端字节和多少 RTT 开销
```

对于 Lance、向量检索或部分 ML 数据加载负载，如果没有 SQL 计划和可恢复的谓词语义，就没有完整的 Semantic 优化。仍然可以利用 Format 和 Physical 层做文件大小、分块、请求合并和并行度方面的优化，但不能声称已经知道其查询谓词或能做 SQL 式 zone-map 优化。这部分适配目前尚未完成。

---

## 3. Writer 能力并不统一

三个常见 Writer 对 Parquet 页级索引的默认行为不同：

- parquet-mr：写出 ColumnIndex / OffsetIndex；
- Trino 原生 Writer：不写；
- PyArrow：默认不写。

当前基线和候选统一由 Spark 4.1.x + parquet-mr 写出。Advisor 中保留 Writer/Reader capability check，避免推荐目标 Writer 根本无法实现或 Reader 无法利用的动作。

---

## 4. 建议式优化流程

### 4.1 第一步：从运行时数据生成候选

候选布局包含四个维度：

```text
partition × file size × row group size × sort
```

#### 排序键

`predicates_from_runtime.py` 从 Spark eventlog 的：

- `Scan parquet`；
- `Location`；
- `PushedFilters`；
- `ReadSchema`

提取真实的运行时谓词。范围谓词列进入排序候选，并按对应 execution 的运行时间加权。多个列经常共同出现时，还可以形成复合排序键候选。

#### Identity 分区键

低 NDV、经常出现等值谓词的列进入 identity 分区候选。目录数来自列 NDV，后续再由 L0 Gate D 检查是否会产生过多目录或过小分区。

#### 文件大小和 Row Group 大小

系统先从对象列表、footer 和 column stats 中读取当前布局，再在“计数空间”生成候选。文件大小候选的核心是先决定文件数：

```text
floor = min(运行并行度, 基线文件数)
n_files ∈ {floor, 2 × floor, 4 × floor, ...}
```

文件数不超过基线文件数，最后才根据总压缩字节换算成目标文件大小。

Row Group 候选也根据当前文件数、Row Group 数和未压缩字节生成，并受 Reader 可读性上界约束。

#### Per-table 设计

由于重写是文件级别的，对于每个表可以进行不同的参数配置。目前的per-table设计如下：

- 哪些表进入搜索，由实测表大小决定；
- 每张表的排序和分区键，由运行时谓词决定；
- 每张表的文件和 Row Group 候选，由该表的实测几何决定。

### 4.2 第二步：L0 硬约束过滤

生成候选后先做 L0 静态检查，明显不可写、不可读或不可能稳定执行的候选不进入解析成本模型。

当前检查包括：

#### Writer/Reader 能力

- canonical action 能否翻译成当前 Writer 参数；
- Reader 是否具备建议依赖的 index、filter 和 vectored read 能力；
- 拒绝spark无法读取的派生变换分区。

#### 基本几何约束

目前仍保留：

- 主表文件数至少为 2；
- 主表文件数不超过 20,000；
- Row Group 总数至少为 4。

这三条最初为 TPCH SF100 设计，通用性不足。后续应改成相对于表大小、基线几何和运行并行度的自适应约束，而不是固定常数。

#### Row Group 可读性

当前请求的 Row Group 大小不能超过 128 MiB。更大的 Row Group 曾触发 parquet-hadoop vectored read 的 300 秒等待超时，因此上界设定为 128 MiB.

这条约束仍有盲区：排序可能因为低 NDV 把文件数压得很低，从而间接产生更大的实际 Row Group，即使候选没有显式请求大 Row Group。后续需要检查预测的实际 RG 大小，而不只是 Writer option。

#### 大表并行度

对于压缩大小不低于 2 GiB 的表，候选文件数不能低于不能低于并行度下界。动作空间生成阶段则通常不生成比基线更多的文件，避免只增加对象打开成本。

#### Gate A：基线聚簇度余量

这一阈值的作用是比较“当前跨度”和“排序后理论可达到的跨度”。只有当前布局与理想排序结果相比仍有足够改进空间，才允许进入排序候选。否则跳过候选。

#### Gate C：剪枝后并行度

排序可能让谓词剪枝更强，但也可能把目标行集中到极少数文件和 Row Group 中，导致查询从并行扫描退化成一两个 task。

如果：

```text
存活 Row Group 数 < 4
并且
存活文件数 < 4
```

则候选被硬拒绝。两项必须同时低于门槛，因为一个大文件中仍可能包含多个可并行读取的 Row Group。

#### Gate D：分区有效性

Gate D 检查：

- 分区是否为可直接被查询谓词利用的 identity 分区；
- 目录数是否超过 64；
- 每个分区平均字节是否低于 128 MiB。

这可以避免高 NDV 分区把一个大表切成几千个小目录和小文件。

### 4.3 第三步：L1 解析模型估价并排名

所有 L0 合法候选都会调用 `evaluate_workload()`，对 workload snapshot 中每条查询的每个 scan 估价。

模型输出：

```text
t_e2e = t_io + t_exec_residual
```

`t_io` 是 Virtual Footer 预测出的对象存储请求时间（RTT × 请求数 + 字节 / 带宽，再除以有效并行度）。`t_exec_residual` 是基线实测时间减去预测 I/O 后剩下的部分，再按候选的扫描行数外推。Advisor 比较和推荐用的是相加后的 `t_e2e`。模型还输出每条查询的 GET、字节、文件数、Row Group 数和剪枝比例，用来解释 `t_io` 从哪里来。

候选相对同一套 L1 基线，如果任何单条查询预测回归超过 10%，就会被 guardrail 拒绝。

最终推荐是：

```text
L0 合法
  ∧ 单查询预测回归不超过 10%
  ∧ t_e2e 最小
```

当前搜索空间规模仍然允许穷举，因此主要采用暴力枚举加硬规则剪枝，也保留了按坐标逐步选择的迭代搜索用于比较。**现阶段没有使用机器学习模型来搜索动作空间**。

---

## 5. L1 解析成本模型

### 5.1 Virtual Footer：不重写文件，先预测重写后的几何

Virtual Footer 是这套 Advisor 的核心。它不实际写出每个候选，而是预测候选布局可能拥有的 Parquet 元数据。

基线几何包括：

- 文件数；
- Row Group 总数和每文件平均 RG 数；
- Row Group 未压缩平均大小；
- 总压缩字节；
- 列顺序和 Column Chunk 字节占比；
- 列 NDV、CDF 和 `rg_span`。

#### 文件数预测

如果候选指定目标文件大小：

```text
n_files = ceil(table_compressed_bytes / target_file_bytes)
```

排序键有额外的 NDV 上界。`repartitionByRange(F, key)` 不可能产生多于排序键不同值个数的非空区间：

```text
n_files = min(n_files, sort_key_ndv)
```

#### 分区几何

Identity 分区不仅产生目录，也会影响文件数。当前 Writer 先按排序键 shuffle，再执行 `partitionBy`。如果排序键与分区键无关，每个写 task 可能为每个分区值写一个文件，因此近似为：

```text
n_files_candidate ≈ n_files_before_partition × n_partitions
```

#### Row Group 数预测

Row Group 数按候选 RG 大小相对基线缩放，同时保持：

```text
n_rg ≥ n_files
```

即每个非空文件至少包含一个 Row Group。

### 5.2 剪枝比例预测

给定一个 scan 和候选排序键，模型预测重写后会保留多少比例的 Row Group。

主要步骤如下。

#### 1. 解析候选布局

确定当前表的：

- 排序列前缀；
- identity 分区列；
- 文件数和 Row Group 数；
- 对应列统计。

#### 2. 折叠谓词

同一列上的多个范围谓词折叠成一个区间。例如：

```text
l_shipdate >= 1994-01-01
AND l_shipdate < 1995-01-01
```

会合并成一个起止范围。等值和 IN 谓词根据 NDV 单独估计。

#### 3. 匹配排序前缀

只有命中排序键前缀的谓词才能直接形成 zone-map / Row Group 剪枝。不相关谓词不会因为表被排序就自动得到剪枝。

如果没有任何谓词命中排序前缀，模型按“无法通过该排序剪枝”处理。

#### 4. 相关列映射

少量已知强相关列可以借用排序前缀的 CDF。例如 TPC-H 中 `l_commitdate`、`l_receiptdate` 与 `l_shipdate` 强相关。

**这一点目前是显式模型假设，可以通过参数关闭**。后续应从运行时统计学习相关性，而不是长期手写。

#### 5. CDF 和 NDV 估计

范围谓词通过列 CDF 估计选择率，等值类谓词通过 NDV 估计。模型再加入边界 Row Group，避免把连续范围理想化成完全精确的 RG 切分。

#### 6. Identity 分区剪枝

如果 scan 的谓词直接命中 identity 分区列，Spark 可以在打开 Parquet 文件前过滤目录。预测得到的剪枝比例最终转换成：

- 存活 Row Group 数；
- 存活文件数；
- 后续可用的并行度。

### 5.3 I/O 成本

根据 Virtual Footer 的预测，每个 scan 继续估算：

- 存活文件和 Row Group；
- 数据 Range GET；
- footer / page-index 等 meta GET；
- HEAD；
- 远端字节。

I/O 时间公式为：

```text
k = max(1, min(K_busy, n_files_opened, PARALLELISM))

t_io +=
    (request_count × RTT + remote_bytes / bandwidth)
    / k
```

其中：

- `RTT`：一次对象存储请求的固定往返成本；
- `bandwidth`：持续传输带宽；
- `K_busy`：实测有效并发，不直接等于 Spark 的 `local[16]`；
- `n_files_opened`：当前 scan 实际能提供的文件并行度；
- `PARALLELISM`：执行环境最大并行槽位。

这个公式体现了两类不同收益：

1. 通过剪枝减少 Row Group 和远端字节；
2. 通过减少文件数降低 HEAD、footer GET 和 RTT 开销。

### 5.4 `execution_residual`

端到端开销中除去IO开销剩余的部分：

```text
residual_base(q) =
    max(0, measured_median(q) - predicted_baseline_io(q))
```

目前 `execution_residual` 的主要缺陷是把固定开销、随行数变化的开销、随文件数变化的开销和 shuffle/join 开销混在一起。后续应至少拆成：

```text
固定查询开销
  + 扫描/解码开销
  + 调度与文件并行度开销
  + join / aggregation / shuffle 开销
```

---

## 6. Writer 如何采用建议

### 6.1 建议 JSON

Advisor 输出 engine-neutral 的 canonical action，例如：

```json
{
  "canonical": "sort.columns",
  "value": ["l_shipdate"],
  "table": "lineitem"
}
```

分析、约束和成本模型只依赖 canonical action，不直接依赖 Spark 参数。后续可以为其他 Writer 增加 renderer，而不必重写 Advisor。

### 6.2 当前 Spark/parquet-mr renderer

`write_layout.py` 读取建议 JSON，并执行：

#### Sort

```text
repartitionByRange(file_count, sort_columns)
  → sortWithinPartitions(sort_columns)
```

这提供跨文件的全局 range partitioning，以及文件内部排序。

#### File size

先根据表压缩字节和目标文件大小计算文件数，再通过 DataFrame `repartition(file_count)` 或 `repartitionByRange` 控制写 task 数。

#### Partition

Identity 分区通过 DataFrame 和 `partitionBy(column)` 实现。派生变换分区虽然也能被 Writer 写出来，但裸 Parquet 查询无法自动将原列谓词改写到派生列，因此已经从推荐空间移除。真正的 hidden partitioning 需要 Iceberg 一类表格式和查询引擎配合，暂时未实现。

#### Row Group size

通过 parquet-mr Writer option设置。

---

## 7. 尚未完成的工作

尚未完成：

1. Lance、向量检索和 ML 数据加载等无 SQL 谓词负载的布局适配；
2. 大动作空间的学习型搜索或 surrogate；
3. `execution_residual` 的结构化分解；
4. 自动生成生产 Writer 源码 diff；
5. partition、bloom、dictionary、page size、列顺序等扩展动作的完整 Virtual Footer 模型；
6. 固定文件数、RG 数约束的进一步自适应；
7. 排序间接产生超大实际 Row Group 的可读性检查；
8. AWS EC2 同区正式 E2/E8 实验。

---

## 10. 正式实验结果

实验在两种网络环境下运行：

- **跨云（腾讯云 VM → AWS S3 us-east-2）**：RTT ≈ 228 ms，BW ≈ 226 MiB/s，K\_busy ≈ 6.77。
- **AWS 同区（EC2 m5d.4xlarge → S3 us-east-2）**：RTT ≈ 25 ms，BW ≈ 103 MiB/s，K\_busy ≈ 6.77。

两者使用相同的布局候选、相同的查询集和相同的 Spark 4.1 / parquet-mr reader 配置，唯一的变量是网络。

### 10.0 核心发现：改善率随 RTT 衰减，但衰减方式因负载而异

同一布局在两种 RTT 下的端到端改善：

| 负载 | 跨云 228 ms | 同区 25 ms | 衰减倍数 |
|---|---|---|---|
| TPC-H SF100 | −37.0% | **−6.0%** | 6.2× |
| ClickBench SF1 | −51.3% | **−3.8%** | 13.5× |

布局在低 RTT 下仍然执行剪枝，GET 削减率几乎不变（TPC-H 44%、ClickBench 26%）。但由于RTT减小，剪枝的收益在端到端 wall clock 中也随之变小。用 L1 成本公式拆解：

| | 跨云 228 ms | 同区 25 ms |
|---|---|---|
| RTT 项占 IO 成本 | 95.6% | 52.0% |
| 带宽项占 IO 成本 | 4.4% | 48.0% |
| IO 占墙钟 | 70.5% | 34.0% |
| 省 44% GET 值多少（TPC-H） | 955 s | 104 s |

三个乘数同时缩小：每个 GET 的价值缩 9.1 倍（RTT 下降），IO 在墙钟里的份额缩 2.1 倍，RTT 在 IO 里的份额缩 1.8 倍。乘起来就是 6% 而不是 37%。

### 10.1 跨负载对比与结论

| 维度 | TPC-H SF100 | ClickBench SF1 |
|---|---|---|
| 负载特征 | 多表 join，22 条查询 | 单表扫描，43 条查询 |
| 跨云改善（228 ms） | −37.0% | −51.3% |
| 同区改善（25 ms） | −6.0% | −3.8% |
| 衰减方式 | 受益面缩小（join 查询无法从布局优化中受益） | 单条收益塌陷（CPU 成主导） |
| GET 削减 | −44% | −26% |
| 字节变化 | −23% | +5% |
| Amdahl 上限（同区） | 28.4% | 38.8% |
| within-winner 节省 | 40% | 13% |
| 主要回退查询 | Q18（join 局部性） | Q37/Q40（并行度）+ Q24（列局部性） |
