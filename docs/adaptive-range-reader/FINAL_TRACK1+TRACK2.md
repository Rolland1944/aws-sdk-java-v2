# Track1 + Track2 最终设计（终稿）

> **定位**：本文件是 Track1（SDK / S3A 读路径）与 Track2（Parquet 写侧布局）的**最终设计与结果**，
> 取代各工作文档中的阶段性结论。工作文档保留为过程记录与证据库，索引见 §10。
> 定稿：2026-09-24。最终实验：SF1 / SF8 ClickBench，Track1 × Track2 2×2 网格，每格 5 轮交替。

---

## 0. 结论

在不修改 Spark、Hadoop、parquet-mr、Arrow 源码，也不改变 Parquet 格式的前提下，两条路线各自有效，并且可以叠加：

- **Track2** 只凭对象存储 SDK 看到的字节访问轨迹与 Parquet footer，生成文件级布局计划 `joint2`，并重写数据；
- **Track1** 通过 Hadoop 的 S3 client factory 扩展点，在 S3A 的同步 `getObject` 上加一层透明的缓存：
  固定 4 GiB 容量（此处可按照内存大小变化，由用户设定），采用代价感知的 W-TinyLFU 准入与淘汰，命中路径不复制数据。

| 墙钟中位数（n=5） | SF1 | vs 基线 | SF8 | vs 基线 |
| --- | ---: | ---: | ---: | ---: |
| 原始布局，无 Track1（基线） | 337.2 s | — | 2309.5 s | — |
| 原始布局 + Track1 | 287.1 s | −14.9% | 2038.1 s | −11.8% |
| joint2，无 Track1 | 288.2 s | −14.5% | 1896.2 s | −17.9% |
| **joint2 + Track1（最终）** | **264.0 s** | **−21.7%** | **1711.8 s** | **−25.9%** |

最终方案把远端 GET 减少 88%（SF1）/ 83%（SF8），远端字节减少 53% / 43%。两个规模上，
43 条查询没有一条比基线慢。所有格的 CV 都在 0.45%–1.8% 之间；每一项配对比较，5 轮全部同向。

代价是 Spark driver 运行时的 Java heap 已用空间峰值：从约 8 GiB 升到 21–25 GiB（driver 最大堆预算为 32 GiB），GC 时间不增加。

在 CVM → COS 上按同一协议完成的 SF1 2×2 网格进一步验证了环境效应：组合方案从
482.63 s 降到 217.16 s（**−55.0%**）。GET 与远端字节的降幅和 AWS 几乎相同，但 COS
更高的单 GET 固定成本把同一组请求削减转化成了更大的墙钟收益。COS SF8 尚未完成。

---

## 1. 问题与评测口径

### 1.1 问题

Spark 通过 S3A 读取对象存储上的 Parquet。每次 Range GET 都要付一笔固定成本：同区 S3 上约 24–26 ms，
其中网络往返只有约 2 ms，其余是服务端首字节时间。另外还有按字节计的传输成本。
查询引擎看不到这两项成本，文件布局也不是为对象存储设计的。

两条路线分别从两端下手：

| | Track2（写侧） | Track1（读侧） |
| --- | --- | --- |
| 改什么 | Parquet 文件内部的布局 | SDK 客户端的读取行为 |
| 减少什么 | GET 数与远端字节（列相邻合并、压缩、文件几何） | 重复 GET（缓存复用） |
| 何时生效 | 数据重写一次，之后所有 reader 受益 | 每个进程在线生效，对数据无要求 |
| 侵入面 | 写入参数 / 写入代码 | 一个 Hadoop 配置项 |

### 1.2 负载与环境

| 项 | 值 |
| --- | --- |
| 负载 | ClickBench `hits`，43 条查询 |
| SF1 | 约 1 亿行；原始布局 110 文件 / 169 RG / 14.1 GiB |
| SF8 | 8 亿行；原始布局 880 文件 / 1352 RG / 112.0 GiB |
| 引擎 | Spark 4.1、Hadoop 3.4.2 S3A、parquet-mr 1.15（vectored IO 开） |
| 客户端 | EC2 `us-east-2`，`local[16]`，driver 32 GiB |
| 存储 | S3 `us-east-2`（同区），bucket `home-haoyue` |
| Reader 冻结配置 | `fadvise=random`，vectored `min.seek=128K` / `max.merged=2M` / `active.ranged.reads=4` |

### 1.3 实验协议

- 只按**墙钟中位数**排名。GET、远端字节、命中率、堆和 GC 只用于解释，不改变排名。
- 每个 `(格, 轮)` 独立拉起一个 Spark 进程，Track1 缓存每次冷启动，43 条查询在一个进程内共享缓存。
- 同一轮内各格交替运行，每轮轮换起始格，把 S3 时延漂移平均到所有格。
- **不接受跨时段比较**：同一代码的 SF8 单轮在不同时段可以相差 3%，原因是单个 GET 的 p50 从 79 ms 漂到 100 ms。
  所有结论只来自同一时段的配对或交替实验。
- 不用 GET 轨迹重放替代真实运行：重放会丢掉并发、缓存反馈和时序。

---

## 2. 总体架构

```text
                ┌──────────────────────── Track2（离线，一次） ────────────────────────┐
 SDK 字节轨迹 ──┤ correlate(GET ∩ column chunk) → access profile → plan_deterministic │
 Parquet footer ┤        → L0 可行性 → L1 定价 → 中间计划 JSON → UC1 writer 重写   │
                └──────────────────────────────────┬──────────────────────────────────┘
                                                   ▼
                                        joint2 布局（标准 Parquet）
                                                   │
 Spark / S3A vectored reader ──► sync getObject ───┤
                                                   ▼
                ┌──────────────────────── Track1（在线，每进程） ───────────────────────┐
                │ Track1S3ClientFactory → Track1GetPipeline                            │
                │   D1 命中？── 是 → pinned zero-copy 流（带回 ETag）                   │
                │     └ 否 → 准入过滤 → tee 流边读边收 → 读满后按 W-TinyLFU 准入      │
                │   任何内部异常 → 原始 exact-range GET；S3 4xx/5xx 原样抛出          │
                └──────────────────────────────────────────────────────────────────────┘
```

两条路线互不依赖：Track2 的产物是一份普通的 Parquet，Track1 对任意布局都生效。

---

## 3. Track2 最终设计：SDK + footer 驱动的文件级布局计划

### 3.1 证据层：只用 SDK 字节与 footer

采集只有两层，不读查询计划、SQL 或谓词：

- **Physical**：`Track2IoCollectorInterceptor` 经 `fs.s3a.audit.execution.interceptors` 注入，
  记录每次请求的 `range_offset / range_length / 起止时间 / 线程`，不需要重编译 Spark。
- **Format**：`parse_footer.py` 解析每个 column chunk 的字节区间、编码、压缩、索引。

`correlate.py` 按纯几何关联：`GET [offset, offset+len) ∩ column chunk`：同一 `(thread, object)` 上相邻请求间隔不超过阈值的连续序列，视为一次逻辑读取。
这一抽象不依赖引擎语义，对 SQL 与非 SQL 负载一致。得到的结果是`某次 GET 与哪些 column chunk 重叠、各重叠多少字节`

`access_profile.py` 聚合 correlate 的结果，产出每列读取频次与字节、**列-列共访问矩阵**（按 episode 累加）、请求尺寸直方图，
以及 `sysconst.py` 拟合的 `(RTT, BW, K)`。

### 3.2 六维动作空间

所有动作都是公开 writer API 的参数，或写入前的 schema 重排、文件轮换：

| 维 | Canonical action | 说明 |
| --- | --- | --- |
| 列顺序 | `write.parquet.column-order` | 核心新动作；引擎按列名匹配，对上层透明 |
| 行组大小 | `write.parquet.row-group-size-bytes` | 影响 RG 数、chunk 大小、scan unit |
| 目标文件大小 | `write.target-file-size-bytes` | 影响文件数、footer 开销与并行度 |
| 压缩 + 编码 | `...compression-codec.column.X` + `...encoding.column.X` | **联合元组**实测定价，不做两个比例相乘 |
| 页几何 | `page-size-bytes` / `page-row-limit` | 只按实测 page index 字节定价 |
| （约束）页索引 | — | 恒开，是校验项不是搜索维 |

### 3.3 计划生成：先确定性算法，由 L1 在候选中择优

`plan_deterministic.py` 为每一维生成有限候选，每一维都把 baseline 作为候选之一，然后由 L1 联合定价、择优：

1. **列顺序**：在共访问矩阵上做贪心 seriation。热列相邻，好让 vectored read 把相邻 chunk 合并成一个 range；
   冷列沉到文件尾。
2. **文件 / 行组**：从实测几何生成一组梯度有限候选，用 order-aware 的 `virtual_footer.merge_gets` 预测 GET 数。
3. **压缩 + 编码**：`compression_probe.py` 按列实测 `(codec, encoding)` 联合字节比。
   没测到的元组记为 unpriced 并退回 baseline。
4. **页几何**：只定价 OffsetIndex / ColumnIndex 字节，不给更细的 page 记跳过收益，因为那需要谓词。
   变化小于 2% t_io 时不动。

**L0**：列顺序必须是原 schema 的排列；codec / encoding 必须与物理类型和目标 writer 能力匹配；
`page ≤ row group`；RG ≤ 128 MiB；scan task 数不低于基线。能力检查按目标 renderer 做，并且在排名之前。

**L1**：`t_io = (request_count × RTT + bytes / BW) / K_eff`。L1 只用来给候选排序，不预测墙钟。

### 3.4 最终计划 `joint2`

L1 在 312 个合法点（另有 72 个被 L0 拒绝）中选出：

| 维 | 取值 |
| --- | --- |
| 列顺序 | seriation：105 个被读列按共访问（400 个列对）重排 |
| 压缩 + 编码 | 70 列 zstd，35 列保留 snappy（zstd 在这些列上省不到 8%）；18 列 PLAIN，`Title` / `URL` 用 DELTA_BYTE_ARRAY，`WatchID` 用 DELTA_BINARY_PACKED |
| 文件大小 | 目标 469,735,470 B（约 448 MiB） |
| 行组 | 保持 baseline（591,702 行 / RG，RG 数不变） |
| 页几何 | 保持 baseline |

重排后的数据集和原始数据集的对比如下。文件大小变化来自压缩/编码的改变。

| | 原始布局 | joint2 | 变化 |
| --- | --- | --- | --- |
| SF1 | 110 文件 / 169 RG / 14.1 GiB | 22 文件 / 169 RG / 10.24 GiB | 压缩字节 −27% |
| SF8 | 880 文件 / 1352 RG / 112.0 GiB | 174 文件 / 1352 RG / 81.9 GiB | 压缩字节 −27% |

### 3.5 落地（parquet writer）与校验

`write_layout_pyarrow.py` 只使用公开的 `pyarrow.parquet.ParquetWriter`：重排后的 `schema`、`row_group_size`、
按目标大小轮换文件、per-column `compression` 与 `column_encoding`，并写死 `write_page_index=True`。

`--verify` 回读 footer，校验列顺序、codec / encoding、页索引以及 page no-op，任何一项不符都判为写出失败。
SF8 写出耗时约 64 分钟，校验通过（174 文件，8 亿行；抽样的 20 个文件中全部 chunk 带 OffsetIndex）。

UC2（Spark SQL + writer 参数，`write_layout.py`）能表达列顺序、行组、文件大小、全局压缩，
但表达不了 per-column 编码。同一份证据下它只能拿到全局 zstd。UC2 保留为工程通道，不是最终主结果。

### 3.6 已测试并否决的分支

| 分支 | 结果 | 处置 |
| --- | --- | --- |
| 解码感知重排（`decode_rerank`） | 模型预测再省 7.8%；受控 A/B 显示解码只占 task CPU 2.4%（约 1.5% 墙钟） | `DECODE_MODELLED=False`，不进搜索 |
| `decode_veto`（否决 3 个更慢的编码） | −1.14%（n=3），不稳定 | 不采用 |
| `order_refined`（局部搜索精化列序） | −0.89%（n=5），但 GET +2.4% | 不采用 |
| 以上两者叠加 | −0.18%，处在噪声里 | 不采用；默认候选仍是 `joint2` |
| 排序 / 分区 | 超出 SDK + footer 主线 | 移出动作空间 |
| LLM 生成计划 | 前置条件：确定性算法有效之后，LLM 仍要有增量 | 未进入主线 |

---

## 4. Track1 最终设计：透明缓存

### 4.1 路径入口

```text
fs.s3a.s3.client.factory.impl = software.amazon.awssdk.s3.adaptive.s3a.Track1S3ClientFactory
```

`Track1S3ClientFactory` 委托 `DefaultS3ClientFactory` 建客户端，再用 JDK proxy 包住 `S3Client`，从而能够拦截`getObject`。`Track1S3ClientFactory`把 Range GET 交给 `Track1GetPipeline`,其他请求原样转发

### 4.2 GET 管线（`Track1GetPipeline`）

1. 解析 GET 请求中的 HTTP Range 头，得到 `[start, end)`。缓存对象标识为 `bucket + S3 object key + VersionId / If-Match / ETag`，不同版本缓存天然隔离。
2. **命中**：`pinCoveringSpan` 在一次加锁里解析并 pin 住覆盖该区间的全部块，可以是单块覆盖，也可以是相邻块拼接。
   返回一个直接读块内存的 composite 流，关闭时解除 pin。合成的响应必须带回 ETag，否则 S3A change detection 会报错。
3. **未命中**：只有 ≤ 8 MiB（单次 fetch 上限）的请求才可能进入缓存，更大的直接 passthrough，不复制。
   可缓存的请求走 **tee 流**：调用方直接读 S3 响应流，同时把字节收进一个 buffer；读满之后，再把整个请求交给 W-TinyLFU 做准入。
4. **异常语义**：缓存内部的任何异常都回退到原始的 exact-range GET；S3 的 4xx/5xx 原样抛出。
   性能决策上的错误不能变成查询失败。

### 4.3 容量、准入与淘汰：固定 4 GiB 的代价感知 W-TinyLFU

| 项 | 取值 |
| --- | --- |
| 容量 | 软目标固定为 hard cap（`fixed.capacity`），4096 MiB；由 `GlobalBudget` 强制执行 |
| 块大小 | 1 MiB（一个请求按块边界切片，但这些切片共享同一个 backing 数组） |
| 分段 | window（约 1% 容量，观察新缓存条目是否会被命中）→ probation（待观察条目） → protected（主区的 80%，条目再次命中后进入），SLRU 晋升 |
| 频率 | 按块 key 计数，上限 15，表规模超过 262,144 时全体减半（老化） |
| 收益 | `benefit = 频率 × 远端代价`；远端代价由 `RemoteCostEstimator` 按请求尺寸桶在线拟合，取置信下界；样本不足时退回字节数 |
| 准入 | **请求级原子**：新请求的 benefit 必须大于它要挤掉的 victim 集合的 benefit 之和，否则整个请求拒收 |
| 淘汰 | 先淘汰 probation，再 window；被 pin 的块不做 victim |
| ghost | 拒收 / 淘汰的 key 各保留最多 65,536 条，只用于遥测（ghost hit） |

这个设计要解决的是早期几版暴露的问题：

- LRU + 256 KiB 准入 + 256 MiB（`100`）在两个规模上都很稳，但它是按字节大小一刀切：
  SF1 上 256 KiB–4 MiB 的请求重用率在 70%–87%，也被一起拒掉了。
- 把准入上限放宽的同时加大预算（`4m1g` / `8m2g`）：在 SF8 上大扫描挤掉小热点，比 `100` 慢约 7%。
- 以请求为单位、按“频率 × 远端代价”做准入，就不需要尺寸阈值：大扫描只访问一次，频率低，拿不到准入；
  重复读的中大 range 可以进入缓存。SF8 上，这使 GET 比 `100` 少 34%。

### 4.4 热路径优化：拒收零复制、命中零复制

| 机制 | 做法 | 状态 |
| --- | --- | --- |
| descriptor-first 准入 | 先用请求描述符做准入比较和 victim 规划，通过之后才动 payload；被拒请求零复制 | 保留 |
| shared backing | 准入成功时转移 tee buffer 的所有权，多个 1 MiB 块直接引用同一个数组的切片，不再复制一份 | 保留（`shared.backing=true`） |
| pinned zero-copy hit | 命中时不在锁内拼接出新数组，由流直接读块；pin 住的块不会被淘汰，因此不会产生 hard cap 之外的 payload | 保留（`zero.copy=true`） |
| `read()` 单字节路径 | 不再分配 `byte[1]` | 保留 |
| doorkeeper（首次访问不 tee） | 首次访问只计频率、直接透传，第二次访问才参与准入 | **否决**（`doorkeeper=false`） |

doorkeeper 让每段数据都要到第二次访问才开始缓存，第二次访问因此仍是远端 GET，多出约 2.2 万次。
每个 GET 约 90 ms，它省下的一次本地复制远抵不上这部分成本。
zero-copy + shared backing 在这组数据里只体现为约 2 s 的 GC 下降；它们对墙钟的净贡献没有被单独证明。
保留它们是因为语义上更干净（拒收零复制、hit 不进锁内 materialize），而且没有观测到代价。

### 4.5 不变量

- 响应字节与原始 GET 逐字节一致；不同版本 / ETag 之间不共享缓存。
- 请求级原子准入：一个请求的所有切片要么全部进入缓存，要么全部不进。
- hard cap 始终成立：被 pin 的块不做 victim；插入前必须向 `GlobalBudget` 申请到字节，申请失败即拒收。
- 探针、遥测、控制器的任何失败都不影响 GET 本身。

### 4.6 已移除或未采用的维度

| 维度 | 证据 | 处置 |
| --- | --- | --- |
| D2 同步转异步 | 单独开启时是净成本（+1.7%），只作为 D4 的依赖存在 | 代码保留，默认关，不进最终方案 |
| D4 请求合并 | 合并率只有提交量的 2.9%；大读平均连续段长 1.07；能合并的小读正好是缓存吃掉的那部分 | 同上 |
| D3 预取 / D5 对冲 | 同区不可隐藏 I/O 为 0，没有可攻击的空间 | 未实现 |
| 在线自适应容量（soft controller） | 最后仍会走到容量上限，且效果差于固定容量 | 最终方案使用 `fixed.capacity` |
| `profile` 诊断计数 | 在热路径上增加 `nanoTime` 调用和原子写 | 正式测量关闭；只在单独的诊断运行中打开 |

### 4.7 最终配置

```text
-Dtrack1.d1=true
-Dtrack1.d1.adaptive=true            # 启用 W-TinyLFU 与请求级准入
-Dtrack1.d1.fixed.capacity=true      # 软目标钉在 hard cap
-Dtrack1.d1.cache.mib=4096
-Dtrack1.d1.hard.mib=4096
-Dtrack1.d1.admit.bytes=0            # 不设尺寸阈值；实际上限为单次 fetch 8 MiB
-Dtrack1.d1.zero.copy=true           # 默认值
-Dtrack1.d1.shared.backing=true      # 默认值
-Dtrack1.d1.doorkeeper=false         # 默认值
-Dtrack1.d1.profile=false            # 默认值
# D2/D4 保持默认 false；块大小保持默认 1 MiB
```

对应 `run_benchmark.py` 参数：
`--track1-s3a --track1-d1 --track1-d1-admit-bytes 0 --track1-d1-cache-mib 4096 --track1-d1-adaptive
--track1-d1-fixed-capacity --track1-d1-hard-mib 4096 --track1-d1-coverage 1.0`。
doorkeeper 默认关闭；对照实验用 `--track1-d1-doorkeeper` 打开。

---

## 5. 最终实验：Track1 × Track2 2×2 网格

报告：`results/track2/sf1_joint2_track1_2x2_n5/`（2026-09-23）与 `results/track2/sf8_joint2_track1_2x2_n5/`（2026-09-24）。
每格 5 轮，共 20 个独立进程，按轮交替并轮换顺序。

### 5.1 SF1

| 格 | 五轮（s） | 中位数 | CV | GET | 远端 | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 原始，无 Track1 | 342.8 / 343.1 / 336.8 / 336.4 / 337.2 | 337.21 | 1.00% | 33,038 | 46.6 GiB | 7.6 GiB |
| 原始 + Track1 | 286.1 / 290.1 / 283.2 / 287.1 / 288.4 | 287.08 | 0.90% | 4,798 | 36.2 GiB | 20.8 GiB |
| joint2，无 Track1 | 285.1 / 291.3 / 285.6 / 290.5 / 288.2 | 288.19 | 0.96% | 23,421 | 31.6 GiB | 11.1 GiB |
| **joint2 + Track1** | 265.8 / 263.6 / 264.0 / 264.2 / 262.5 | **264.04** | 0.45% | **4,034** | **22.0 GiB** | 21.7 GiB |

### 5.2 SF8

| 格 | 五轮（s） | 中位数 | CV | GET | 远端 | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 原始，无 Track1 | 2289.8 / 2275.9 / 2309.5 / 2339.5 / 2333.9 | 2309.48 | 1.19% | 262,309 | 371.9 GiB | 8.1 GiB |
| 原始 + Track1 | 2000.9 / 2012.3 / 2052.9 / 2038.1 / 2094.2 | 2038.11 | 1.80% | 75,616 | 333.8 GiB | 21.7 GiB |
| joint2，无 Track1 | 1852.3 / 1908.5 / 1901.8 / 1896.2 / 1866.1 | 1896.24 | 1.30% | 183,675 | 252.1 GiB | 8.9 GiB |
| **joint2 + Track1** | 1679.8 / 1711.8 / 1675.1 / 1714.6 / 1727.7 | **1711.79** | 1.36% | **43,762** | **210.7 GiB** | 25.0 GiB |

### 5.3 分解

| 效应 | SF1 | SF8 |
| --- | ---: | ---: |
| Track1，原始布局上 | −14.9% | −11.8% |
| Track1，joint2 上 | −8.4% | −9.7% |
| Track2，无 Track1 | −14.5% | −17.9% |
| Track2，有 Track1 | −8.0% | −16.0% |
| 两者简单相加的预期 | −99.2 s | −684.6 s |
| 组合实际 | −73.2 s（−21.7%） | −597.7 s（−25.9%） |

同一轮内的配对差值（单位 s，5 轮全部同向）：

| 配对 | SF1 | SF8 |
| --- | --- | --- |
| 原始 → 原始 + Track1 | −56.7 / −53.0 / −53.6 / −49.3 / −48.8 | −288.9 / −263.6 / −256.6 / −301.4 / −239.7 |
| joint2 → joint2 + Track1 | −19.3 / −27.7 / −21.6 / −26.3 / −25.7 | −172.4 / −196.7 / −226.7 / −181.7 / −138.4 |
| 原始 → joint2 | −57.7 / −51.8 / −51.1 / −45.8 / −49.0 | −437.5 / −367.4 / −407.7 / −443.3 / −467.8 |

逐查询：组合方案相对基线没有一条查询变慢。收益最大的是 Q24（SF1 −11.6 s，SF8 −104.6 s）。

### 5.4 CVM → COS：SF1 跨环境验证

COS 使用同一组 43 条查询和固定 4 GiB Track1。每格 5 轮交替运行；五轮列为 `query_sum`，GET 为总请求数，
峰值堆为五轮 `peak_heap_bytes` 的中位数。

| 格 | 五轮（s） | 中位数 | CV | GET | 远端 | 峰值堆 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 原始，无 Track1 | 479.0 / 482.3 / 498.4 / 482.6 / 501.4 | 482.63 | 2.11% | 33,038 | 46.6 GiB | 7.3 GiB |
| 原始 + Track1 | 281.4 / 258.9 / 262.0 / 257.0 / 297.9 | 261.99 | 6.53% | 4,798 | 36.2 GiB | 20.6 GiB |
| joint2，无 Track1 | 344.0 / 389.4 / 338.5 / 370.5 / 350.0 | 349.96 | 5.90% | 23,588 | 31.6 GiB | 10.7 GiB |
| **joint2 + Track1** | 211.7 / 271.3 / 220.8 / 213.3 / 217.2 | **217.16** | 11.07% | **4,087** | **22.0 GiB** | 21.0 GiB |

`joint2 + Track1` 五轮 GET 为 4,079 / 4,084 / 4,087 / 4,088 / 4,088，说明请求行为稳定；
墙钟抖动主要来自运行环境。无 Track1 两格的峰值堆采样波动较大：原始布局为 6.8–14.5 GiB，
joint2 为 6.7–14.9 GiB，因此不对这两格约 3.4 GiB 的堆中位数差作机制解释。

| 效应 | COS SF1 |
| --- | ---: |
| Track1，原始布局上 | −45.7% |
| Track1，joint2 上 | −37.9% |
| Track2，无 Track1 | −27.5% |
| Track2，有 Track1 | −17.1% |
| 两者简单相加的预期 | −353.3 s |
| **组合实际** | **−265.5 s（−55.0%）** |

同一轮内的配对差值（单位 s，三项 5 轮全部同向）：

| 配对 | COS SF1 |
| --- | --- |
| 原始 → 原始 + Track1 | −197.6 / −223.4 / −236.5 / −225.6 / −203.5 |
| joint2 → joint2 + Track1 | −132.3 / −118.1 / −117.7 / −157.1 / −132.8 |
| 原始 → joint2 | −135.1 / −92.9 / −160.0 / −112.1 / −151.4 |

组合方案兑现了简单相加秒数的 75.1%，与 AWS SF1 的 74% 接近：两条路线的重叠主要是结构性的——
Track2 已合并的 GET 不再能被 Track1 重复消除，而不是某一家对象存储独有的现象。尽管 CV 高于
AWS，最小的单轮配对收益仍为 92.9 s，远大于轮间抖动，方向性结论明确；精确百分比仍以中位数表达。

---

## 6. 机制解释

1. **Track1 是请求数优化，Track2 同时减请求和字节。** Track1 在原始布局上砍掉 85%（SF1）/ 71%（SF8）的 GET，
   远端字节只少 22% / 10%。Track2 靠列相邻合并和压缩，GET 少 29% / 30%，字节少 32% / 32%。
2. **规模变大时，Track2 的比重上升。** 缓存固定为 4 GiB，数据量增大 8 倍后，Track1 能覆盖的重复读比例下降；
   布局收益则随数据量线性放大。所以在 SF8 上 Track2 单独的贡献（−17.9%）超过了 Track1 单独的贡献（−11.8%）。
3. **两者有重叠，但不是替代。** joint2 已经把相邻小读合并成更少的 range，Track1 能消除的重复 GET 随之变少，
   因此组合收益小于两者相加（SF1 实现了相加预期的 74%，SF8 实现了 87%）。
4. **同区环境下，I/O 大部分藏在计算后面。** 早期测量（SF1 joint2 布局，无缓存）：至少有一个 GET 在途的时间只占墙钟的 53%；
   计算地板（task CPU / 16 = 181 s）已经高于 I/O 覆盖窗口（152 s），不可隐藏的 I/O 为 0。所以 GET 减少 88% 只换来约 20% 的墙钟。
   这说明两条路线的收益在同区是被压低的，而不是被夸大的。
5. **解码不是剩余墙钟的来源。** 受控 A/B 把解码定在 task CPU 的 2.4%。剩余墙钟主要是过滤、聚合、shuffle 和调度，
   不属于存储布局或读路径的范围。

---

## 7. 运行环境：为什么结论在高时延环境更强

### 7.1 单 GET 成本对比

| 测量 | EC2 → S3（us-east-2） | CVM → COS（ap-guangzhou） |
| --- | ---: | ---: |
| TCP 握手 p50（网络 RTT） | 1.17 ms | 0.71 ms |
| TCP + TLS 新建 p50 | 30.9 ms | 73.2 ms |
| 复用连接的签名 64 B Range GET p50 | 35.7 ms | 67.6 ms |
| SF1 基线：<64 KiB GET p50 | 23.6 ms | 81 ms |
| SF1 基线：≥8 MiB GET p50 | 48.3 ms | 112 ms |
| SF1 基线 43 查询墙钟 | 321.7–337.2 s | 473.2 s |

COS 的网络往返并不比 S3 慢，但**每个请求的固定成本约为 S3 的 3.4 倍**，差额主要是服务端处理时间。
两边 GET 次数与远端字节完全一致（33,034 vs 33,038 次，都是 46.6 GiB），因为这两个量由布局和查询决定。

### 7.2 COS SF1 实测

§5.4 的同时段 2×2 网格确认了方向，而且放大幅度明显：

| 相对各自原始基线的收益 | AWS SF1 | COS SF1 |
| --- | ---: | ---: |
| Track1，原始布局上 | −14.9% | **−45.7%** |
| Track2，无 Track1 | −14.5% | **−27.5%** |
| Track1 + Track2 | −21.7% | **−55.0%** |

两边的物理削减几乎相同：组合方案均把约 33,038 个 GET 降到约 4,000（−88%），远端字节均从
46.6 GiB 降到 22.0 GiB（−53%）。变化的是每个被消除 GET 的时间价值：COS 小 GET 固定成本约为
S3 的 3.4 倍，因此同一组结构性削减兑现为更大的墙钟收益。Track1 主要减少 GET，放大最明显；
Track2 同时减少 GET 和字节，但 COS 的有效带宽约 589 MiB/s，字节项相对便宜，所以放大较小。

这组跨主机比较不能把全部差值严格归因于对象存储时延，CVM 与 EC2 的 CPU、调度和运行噪声也不同；
因果证据来自各环境内部的交替配对。COS 四格 CV 为 2.1%–11.1%，高于 AWS，但所有 15 个相关配对
仍全部同向，且 GET 极稳定。COS SF8 仍在运行，当前结论只覆盖 SF1。

---

## 8. 复现

### 8.1 构建（注意必须打包）

```bash
mvn -pl services-custom/s3-adaptive-range-reader \
    -Dcheckstyle.skip=true -Dspotbugs.skip=true -Djapicmp.skip=true -DskipTests package
# 确认 Spark 加载的是新 JAR
unzip -l services-custom/s3-adaptive-range-reader/target/aws-sdk-java-s3-adaptive-range-reader-2.25.70.jar \
    | grep PinnedSliceInputStream
```

`mvn test` 只更新 `target/classes`，不会重新打 JAR。Spark 从 JAR 加载代码，而新的系统属性在旧 JAR 上会被静默忽略。
曾因此导致一轮“优化版”实际运行的是旧代码。

必须使用仓库的 `.venv/bin/python`（PySpark 4.1.0）；系统 `python3` 没有 PySpark。

### 8.2 Track2：生成与写出布局

```bash
# 已有计划直接写出（SF8 示例）
.venv/bin/python tools/track2/write_layout_pyarrow.py \
    --source s3a://home-haoyue/track2/clickbench_sf8 \
    --out    s3a://home-haoyue/track2/clickbench_sf8_e0_uc1_joint2 \
    --plan   docs/adaptive-range-reader/results/track2/e0_smoke_uc1/plan_deterministic.json \
    --tables hits --verify
# 从轨迹端到端重新规划：tools/track2/run_e0_smoke.py（八阶段编排）
```

### 8.3 单格运行

```bash
.venv/bin/python tools/track2/run_benchmark.py \
    --data s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2 \
    --out <dir> --queries-file tools/track2/clickbench_queries.json --tables hits --runs 1 \
    --track1-s3a --track1-d1 --track1-d1-admit-bytes 0 --track1-d1-cache-mib 4096 \
    --track1-d1-adaptive --track1-d1-fixed-capacity --track1-d1-hard-mib 4096 \
    --track1-d1-coverage 1.0
```

### 8.4 数据位置

| 布局 | 路径 |
| --- | --- |
| SF1 原始 | `s3a://home-haoyue/track2/clickbench_sf1` |
| SF1 joint2 | `s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2` |
| SF8 原始 | `s3a://home-haoyue/track2/clickbench_sf8` |
| SF8 joint2 | `s3a://home-haoyue/track2/clickbench_sf8_e0_uc1_joint2` |

长时间实验用 `nohup` / tmux 在服务器上后台运行，不依赖本地终端连接。

---

## 9. 局限与后续

- **COS 当前只完成 SF1。** 高固定时延环境中的放大效应已由 5 轮 2×2 网格确认，但 COS SF8
  尚未完成；而且该 SF1 使用的是不带 scan-task 数量下限的既有 joint2 物理布局。
- **最终 2×2 网格的 Track1 使用固定 4 GiB。** 容量不随数据规模在线调整；SF8 上 Track1 份额下降，部分原因就在这里。
  是否需要更大的容量，取决于内存预算：8 GiB 只能在更大的 Xmx 下作为独立的成本实验，不与 32 GiB 的结果直接排名。
- **堆代价。** driver 峰值堆 21–25 GiB，占 32 GiB 的 66%–78%，对更小的 driver 需要相应缩小容量。
- **内存复制优化的墙钟收益未被单独证明。** 同时段单轮只看到约 2 s 的 GC 下降；最终保留是基于语义和零代价。
- **LLM 计划生成、UC2 作为主结果、跨 reader（PyArrow 读）验证**均未完成。

