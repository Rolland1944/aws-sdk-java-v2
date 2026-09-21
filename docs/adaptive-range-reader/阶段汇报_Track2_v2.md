# Track2 v2 阶段汇报：面向 Parquet 文件格式层的自适应布局优化

## 1. 当前阶段结论

Track2 v2 当前已经完成一条可运行的端到端链路：

```text
SDK 字节访问轨迹 + Parquet footer
→ 构造 access profile
→ 生成文件级布局优化计划
→ 用 pyarrow.parquet.ParquetWriter 重写 Parquet
→ 使用同一套 ClickBench 查询复测
```

当前已完成的是 **UC1：用户直接调用底层 Parquet Writer 的场景**。使用的 writer 是：

```python
pyarrow.parquet.ParquetWriter
```

在 ClickBench SF1 `hits` 表上，完成了同时间窗口下 baseline 与 candidate 的 5 组交替重复实验。最终结果：


| 指标           | Baseline  | Candidate | 变化          |
| ------------ | --------- | --------- | ----------- |
| 端到端墙钟时间中位数   | 317.694 s | 266.077 s | **-16.25%** |
| Ranged GET 数 | 33,013    | 23,406    | -29.1%      |
| 远端读取字节       | 50.04 GB  | 33.95 GB  | -32.1%      |


因此，目前的阶段性成果是：**在不修改 Spark、Arrow、parquet-mr 底层代码、不改变 Parquet 文件格式的前提下，仅通过 SDK 访问轨迹与 Parquet footer 自动生成文件级布局计划，并在 UC1 中获得 16.2% 的端到端性能提升。**

## 2. 与上次讨论的对应关系

当前完成情况：


| 会议目标                    | 当前状态                        |
| ----------------------- | --------------------------- |
| 使用 SDK + footer 作为证据    | 已完成                         |
| 生成文件级中间优化计划             | 已完成                         |
| 聚焦 Parquet 文件格式层        | 已完成                         |
| 不修改底层格式库或 Spark 源码      | 已满足                         |
| UC1：直接调用 Parquet writer | 已完成端到端测试                    |
| UC2：Spark SQL / 参数落地    | 已实现基础 renderer，但尚未作为最终实验主结果 |
| LLM 自动生成计划              | 尚未进入当前阶段主线                  |
| 更大规模消融与跨 reader 验证      | 尚未作为当前阶段结论                  |




## 3. Track2 v2 的六维动作空间

Track2 v2 将原先 v1 中偏高层的 `partition`、`sort` 等动作移出主线，改为 Parquet 文件格式内部可控的六类动作。


| 维度                | 含义                          | Canonical action                                                 | UC1 中的落地方式                                           |
| ----------------- | --------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------- |
| 1. Column order   | 调整 Parquet 文件内部 schema 字段顺序 | `write.parquet.column-order`                                     | 重排 `pa.schema` / 写入列顺序                               |
| 2. Row-group size | 调整 row group 大小             | `write.parquet.row-group-size-bytes`                             | `ParquetWriter.write_table(..., row_group_size=...)` |
| 3. File size      | 调整目标文件大小                    | `write.target-file-size-bytes`                                   | 达到目标大小后关闭当前 writer 并开启新文件                            |
| 4. Codec          | 调整压缩算法                      | `write.parquet.compression-codec` / `.column.X`                  | `compression=`，支持 per-column compression             |
| 5. Page geometry  | 调整 page 大小 / page 行数上限      | `write.parquet.page-size-bytes` / `write.parquet.page-row-limit` | `data_page_size` / `max_rows_per_page`               |
| 6. Encoding       | 调整列编码族                      | `write.parquet.encoding.column.X`                                | `column_encoding=`                                   |


当前实现里 `codec` 和 `encoding` 不是两个相互独立相乘的估价轴，而是作为 **联合** `(codec, encoding)` **元组** 进入 L1。原因是二者对字节数的影响不是独立的：例如 dictionary encoding 已经把列压缩得很小后，codec 还能继续压缩的空间会变小。所以目前对于这一维的估价是`ratio(codec, encoding)`

## 4. 六维动作如何纳入 L1 估价



### 4.1 Column order

列顺序是 v2 的核心动作。Parquet 文件中的列顺序可以与上层表定义顺序不同，查询时按列名匹配，因此该动作对上层透明。

L1 中的估价方式：

1. 从 SDK trace + footer 构造 access episode。
2. 统计同一 episode 中共同读取的列，得到 co-access matrix。
3. 生成一个候选列顺序，使经常共同读取的列尽量相邻。
4. 在 `virtual_footer.merge_gets()` 中模拟 parquet-mr vectored read 的 range 合并。
5. 如果列相邻后多个 range 可以合并，L1 预测 GET 数下降。



### 4.2 Row-group size

Row group 大小影响：

- row group 数量；
- 每个 row group 的 column chunk 大小；
- 每次扫描需要打开的 row group 数；
- Spark input split / scan unit 数。

L1 中的估价方式：

```text
row_group_size
→ 预测 n_rg
→ 影响 data GET 数、metadata GET 数、scan unit 数
→ 进入 t_io
```

当前最终计划中，row group 选择保持 baseline，因为 L1 比较后没有选择改变该轴。

### 4.3 File size

目标文件大小影响：

- 文件数；
- 每文件 metadata / footer / HEAD 开销；
- Spark scan unit 数；
- 并行度和文件打开成本。

L1 中的估价方式：

```text
target_file_size
→ 预测 n_files
→ 预测 file-level metadata 请求
→ 预测 scan unit
→ 进入 t_io
```

当前最终计划中，L1 选择了约 32 个目标文件。

### 4.4 Codec

Codec 影响远端读取字节数。当前 L1 正式纳入的是 codec 对 **compressed bytes / wire bytes** 的影响，而不是解码时 CPU 的开销。

L1 中的估价方式：

```text
codec
→ compression_probe 实测每列压缩后字节比例
→ virtual_footer 缩放 column chunk 字节
→ 改变读取字节和可能的 range 合并
→ 进入 t_io
```



### 4.5 Page geometry

Page geometry 包括 page size 和 page row limit。当前 L1 对这一维采取保守估价：只估价 page index metadata 字节变化，不估价 page-level predicate skipping。因为 page skipping 需要知道谓词会过滤哪些 page，而 SDK 无法读取谓词。

L1 中的估价方式：

```text
page_size
→ compression_probe 实测 page index 字节 / page
→ 预测 OffsetIndex / ColumnIndex 字节变化
→ 计入 scan metadata bytes
→ 进入 t_io
```

当前最终计划中，page geometry 保持 baseline，因为该轴在 ClickBench 上的可见收益很小。

## 5. L1 估价模型

L1 的目标不是复刻整个查询执行时间，而是对不同文件布局候选进行排序。当前正式参与排序的是 I/O 成本：

```text
t_io = (request_count × RTT + bytes / BW) / K_eff
```

其中：

- `request_count`：预测的 ranged GET 数、metadata GET 数、HEAD 请求数；
- `RTT`：单次请求固定延迟，由 `sysconst.py` 从 SDK trace 拟合；
- `bytes`：预测读取的远端字节数，包括 data bytes 和 metadata bytes；
- `BW`：有效带宽，由 `sysconst.py` 拟合；
- `K_eff`：有效并发度，取决于 `K_busy`、scan unit 数和本地并行度上限。

## 6. 下一步计划：接入 LLM

### 6.1 把 canonical plan 翻译成 Spark options、SQL 或用户应用代码

引擎中立的文件级计划已经由确定性算法生成，下一阶段用 LLM 把它翻译成具体落地形式，例如 Spark SQL / DataFrame writer 的 options、改写后的 SQL，或用户应用中创建 `ParquetWriter` 的代码改动。

验证方式保持现有路径：写完后读回 footer，对照计划核对列顺序、codec、encoding、文件几何是否落地。

### 6.2 压缩轴的探针预算分配

压缩 / encoding 轴进入 L1 的前提是 `compression_probe` 对 `(codec, encoding)` 联合点做了实测。当前 ClickBench `hits` 上跑满了联合网格；换一张宽表或新数据集时，不能对每一列、每一个元组都测一遍。

下一阶段用 LLM 根据列名、物理类型、基数等线索排出测量顺序（例如高基数字符串优先试 dictionary / `DELTA_BYTE_ARRAY`），把有限探针预算花在更可能改变 L1 排序的列和元组上。未测到的元组仍按现有规则标为 unpriced，回退到 baseline。

