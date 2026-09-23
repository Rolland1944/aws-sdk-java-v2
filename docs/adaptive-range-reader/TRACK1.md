# Track1：Spark/S3A 直连的读路径优化（D1 / D2 / D4）

> **定位**：本文件是 Track1 的工作文档。Track2 负责改 Parquet 布局；Track1 负责在 SDK / S3A
> 读路径上透明地做缓存、同步转异步和请求合并。二者共用同一套 ClickBench + 真实 S3 评测口径。
> 创建：2026-09-14。对应实现批次：P0 接入 → P1 三维基础版 → P2 固定组合联合 oracle。

---

## 0. 一句话结论

Track1 已经接到 Spark/S3A 的真实 `getObject` 路径上，**联合 oracle 收敛在 D1-only
的 −7.2% ~ −7.8%，离 16.4% 的目标差一个数量级，且原因是结构性的**：这条负载上
S3A + `local[16]` 已经把 I/O 藏在了解码后面。从 `000` 到最激进的缓存配置，GET 的
TTFB 总量从 669s 砍到 155s（−77%）、网络字节 −28.6%，墙钟只从 284s 到 257s
（−9%）——去掉 1 秒 I/O 只换回约 0.05 秒墙钟。

三个维度的剩余空间都已量化到底：

- **D1 是唯一的正收益维度**，但把准入从 256KiB 抬到 8MiB、预算从 256MiB 抬到 2GiB，
  命中率 64.3% → 81.5%、字节 −2.9% → −28.6%，墙钟只多赚 **0.57 个百分点**，
  峰值堆却从 15.0GiB 涨到 21.4GiB。**推荐 oracle 仍取 `100`（256KiB / 256MiB）**。
- **D4 没有矿**：>256KiB 的 2520 个 distinct range 里只有 169 个严格相邻，平均连续段
  长 1.07；它唯一的合并对象是 64–256KiB 的小读，而那批正好被 D1 以 64% 命中率吃掉。
- **D2 是净税**（+1.71%），只作为 D4 的依赖存在。

**但「结构性」指的是这个运行环境的结构，不是机制的结构（§6.7）。** 拟合出的 per-GET
成本只有 26.1ms，其中网络仅 2.2ms、其余约 24ms 是 S3 服务端首字节地板；机器是
`m5dn.4xlarge`（网络优化变体，16 vCPU / 25 Gbps）且与 bucket 同区。在这个配置下
`不可隐藏 I/O = max(0, 152s − 181s) = 0`，墙钟里根本没有留给读路径优化的份额。而
D1 砍掉的是 **81.5% 的请求**（字节只有 −28.6%），请求数恰好是唯一随 per-GET 成本
线性放大的量。把 bucket 换到 `eu-west-1`（per-GET 102ms，纯 AWS 内部、无需改代码），
在两种 K 假设下单 Track1 都外推到 **≥29%**。

原先写的「两条路」中有一条已被排除：Track2 解码侧的空间实测只有 ~1.5% 墙钟
（见 `DECODE_AXIS.md`）。现在的三条路是——换运行 regime（推荐，§9）、接受 ~7% 并
重定 30% 的口径、或改评价指标（不推荐）。

---

## 1. 目标与评测口径

Track2 UC1 已在同一 ClickBench 上拿到墙钟 **−16.25%**（317.7s → 266.1s）。相对原始
baseline 的 30% 目标，Track1 需要在 **Track2 candidate 布局**上再压约

```text
1 − 0.70 / 0.8375 ≈ 16.4%
```

本轮约束：

- **不做** E0 SDK GET 轨迹重放。重放会丢掉并发、排队、缓存反馈和合并后的时序，不能当正式结果。
- 所有数字来自真实 Spark/S3A、真实 S3、同一 43 条 ClickBench。
- 排序只认 **墙钟 median**。GET、bytes、cache hit、merged GET、peak heap 只解释、不改排名。
- 「联合 oracle」= 预先登记的有限动作空间里，墙钟 median 最小的那个固定组合；不声称连续参数空间的全局最优。

数据与环境（P2 正式矩阵）：

| 项 | 值 |
| --- | --- |
| 数据 | `s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2` |
| 查询 | `tools/track2/clickbench_queries.json`（Q1–Q43） |
| Spark | `local[16]`，driver 32g |
| 重复 | 每格 5 个独立 Spark 进程（D1 冷启动） |
| 调度 | 交错 + 每轮旋转配置顺序 |
| 报告 | `docs/adaptive-range-reader/results/track2/track1_p2_matrix/p2_report.json` |

---

## 2. 接入面（P0）

Hadoop 3.4.2 提供 `fs.s3a.s3.client.factory.impl`。Track1 不改、不重编 Spark / Hadoop，
只换这个 factory：

```text
fs.s3a.s3.client.factory.impl =
  software.amazon.awssdk.s3.adaptive.s3a.Track1S3ClientFactory
```

`Track1S3ClientFactory` 委托 `DefaultS3ClientFactory` 建客户端，再用 JDK proxy 包住
`S3Client` / `S3AsyncClient`。Transfer manager 拿到的是 unwrap 后的 raw async client，
避免 CRT / transfer 内部绑到 invocation handler。

P0 只做 passthrough：不改 GET 请求或响应对象。`Track1S3aProbe` 记 sync/async、range、
version、status、latency、exception；`-Dtrack1.s3a.probe.dir` 写 NDJSON。探针失败不得
影响 GET。

接线开关在 `tools/track2/s3a_session.py`：`apply_frozen_reader(..., track1_s3a=False)`
默认关，`run_benchmark.py --track1-s3a` 显式开。既有 Track2 E0 路径不变。

**P0 冒烟（Q1–Q7，n=1，candidate 布局）**

| 项 | 结果 |
| --- | --- |
| 行数 | 一致 |
| interceptor GET | 2741 = 2741 |
| bytes | +0.015% |
| 墙钟 | 22.67s vs 22.13s（+2.5%，单轮噪声） |
| probe | 2752 条全部是 **sync** `getObject`，全部 ok |

结论：此栈的真实读取入口是同步 `S3Client`，不是 async/CRT。D2 因此有明确对象。
若包装破坏 range / cancel / close，停止在此接入面上做维度实现。

编译注意：`s3-adaptive-range-reader` 的 `pom.xml` 必须显式 `provided` 依赖
`hadoop-common` 3.4.2，否则 factory 看不到 `Configurable` / `Configuration`。
Spark 侧 JAR 必须带上更新后的 `RuntimeConfig$Builder`，否则会 `NoSuchMethodError`。

---

## 3. 三个维度（P1）

维度默认全关。`-Dtrack1.d1/d2/d4` 独立开关。旧四策略仍是 baseline，不承载新架构。

```text
S3A  sync getObject
        │
        ▼
Track1GetPipeline
        │
   ┌────┴────┐
   │ D1 hit? │── yes → 合成 GetObjectResponse（必须带回 ETag）
   └────┬────┘
        │ no
        ▼
   D2 开？── yes → Track1GetQueue（单 dispatcher）
        │              │
        │              └─ D4 开？── yes → RangeMerger + g*=RTT×BW
        │
        └── no → 原始 sync exact-range GET
```

异常路径：缓存 / 队列 / 合并失败回退 exact-range GET；S3 4xx/5xx 原样抛出。性能决策
错误不得变成查询失败。

### 3.1 D1 缓存

`Track1RangeCache` 复用已有的 `AppCache` + `GlobalBudget`。

| 项 | 基础版取值 |
| --- | --- |
| 容量 | 256 MiB（`-Dtrack1.d1.cache.mib`） |
| 块大小 | 1 MiB（`-Dtrack1.d1.block.bytes`） |
| 淘汰 / 准入 | LRU / always-admit |
| key | bucket + key + VersionId / If-Match；无版本时 `*` |
| 命中 | 单块 covering，或相邻块拼接 |
| 生命周期 | 每个 Spark 进程冷启动；同一轮 43 条查询之间共享 |

**必须带回 ETag**。S3A change detection 要求 ETag；只回 bytes 会抛
`NoVersionAttributeException`（P2 接线时修过）。

禁止把 `cacheHitRate` 当优化目标或验收指标。

### 3.2 D2 同步转异步

同步 `getObject` 进 `Track1GetQueue`，dispatcher 用 unwrap 后的 `S3AsyncClient` 发出，
调用线程只等自己的结果。D2 开、D4 关时仍逐请求发出，用来单独量排队和线程切换成本。

**单 dispatcher**。多 worker 会拆散可合并批次，D4 测不到合并。实际并发交给
`ConcurrencyLimiter`（`-Dtrack1.d2.workers`，默认 16），不是多个队列线程。

等待窗口 `-Dtrack1.d2.wait.us`：`010`/`110` 用 0μs；`011`/`111` 用 50μs。

### 3.3 D4 请求合并

同对象邻近 range 按 `g* = RTT × BW`（`LinkEstimator` EWMA）合并，再受

- `maxSingleFetchBytes`（默认 8 MiB）
- 浪费字节比例（`-Dtrack1.d4.max.waste`）
- 并发 GET、in-flight bytes

硬约束。合并响应按原 range 切片分发。D4 无 D2 时忽略——合并依赖队列里同时看到多条请求。

---

## 4. P2 固定组合矩阵

动作空间（第一版）：

| cell | D1 | D2 | D4 | wait | 用意 |
| --- | --- | --- | --- | --- | --- |
| `000` | off | off | off | 0 | factory passthrough |
| `100` | on | off | off | 0 | 纯缓存 |
| `010` | off | on | off | 0 | 纯排队成本 |
| `011` | off | on | on | 200μs | 实测等待 / 合并拐点 |
| `110` | on | on | off | 0 | 缓存 + 排队 |
| `111` | on | on | on | 200μs | 全开 |

Runner：`tools/track2/run_track1_matrix.py`。每个 `(cell, run)` 独立拉起
`run_benchmark.py`，避免 factory 单例和 D1 热缓存漏到下一格。`--resume` 默认开。
`spark.stop()` 前从 `Track1S3aRuntime.snapshotJson()` 写入 cache hits / useful
bytes / merged GET / waste / queue wait / peak heap / g*。

排序规则：只按墙钟 median。同时要求 CV < 5%、读放大 / 堆内存有界、没有严重逐查询回退；
这些是约束与解释，不改排名。

---

## 5. P2 正式结果（2026-09-14）

6 格全部 STABLE，CV 0.39%–1.09%，query regression = 0，fallback = 0。
bytes / heap 边界全部 `ok`。

| cell | median | mean | CV | vs `000` | GET | 远端 GiB | cache hit | useful GiB | merged | peak heap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **100** | **276.17s** | 276.38s | 0.58% | **−2.39%** | **15287** | 30.58 | **8130** | 1.03 | 0 | 20.3 GiB |
| 111 | 276.35s | 277.74s | 1.09% | −2.33% | 15306 | 30.59 | 8115 | 1.03 | 0 | 20.6 GiB |
| 110 | 276.83s | 276.57s | 0.43% | −2.16% | 15288 | 30.59 | 8129 | 1.03 | 0 | 17.3 GiB |
| 000 | 282.95s | 282.74s | 0.39% | — | 23411 | 31.62 | 0 | 0 | 0 | 12.6 GiB |
| 011 | 286.78s | 286.21s | 0.53% | +1.35% | 23411 | 31.62 | 0 | 0 | 0 | 17.5 GiB |
| 010 | 288.01s | 287.96s | 0.41% | +1.79% | 23411 | 31.62 | 0 | 0 | 0 | 17.3 GiB |

排名：`100` > `111` > `110` > `000` > `011` > `010`。

**稳定 winner：`100`。**

各格墙钟（秒）：

```text
000  [283.72, 282.95, 281.63, 283.87, 281.54]
100  [278.18, 274.07, 277.62, 276.17, 275.87]
010  [289.71, 287.57, 288.10, 286.43, 288.02]
011  [286.78, 287.26, 284.49, 287.84, 284.68]
110  [275.57, 276.84, 278.19, 276.97, 275.27]
111  [276.35, 275.95, 282.84, 278.15, 275.40]
```

---

## 5.1 修正版 6×5（v2，2026-09-14）

含 async bootstrap 的 JAR，`011/111` wait = 200μs，每格 5 轮。
报告：`docs/adaptive-range-reader/results/track2/track1_p2_matrix_v2/p2_report.json`。

| cell | median | vs `000` | GET | 远端 GiB | cache hit | merged | queue submit | 单例 batch | peak heap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **100** | **278.5s** | **−2.10%** | 15298 | 30.59 | 8123 | 0 | 0 | 0 | 17.2 GiB |
| 110 | 279.0s | −1.89% | 15309 | 30.59 | 8112 | 0 | 15305 | 12693 | 18.9 GiB |
| 111 | 279.0s | −1.89% | 15104 | 30.59 | 8123 | 169 | 15294 | 10538 | 19.4 GiB |
| 000 | 284.4s | — | 23421 | 31.62 | 0 | 0 | 0 | 0 | 13.6 GiB |
| 011 | 290.7s | +2.20% | 22639 | 31.59 | 0 | 669 | 23417 | 15050 | 17.7 GiB |
| 010 | 289.3s | +1.71% | 23421 | 31.62 | 0 | 0 | 23417 | 18686 | 17.7 GiB |

D4 的接线确实修好了（`011` 合并 669 次、`111` 合并 169 次），但**合并率只有提交量的
2.9%**，batch 有 82%–91% 是单例。D2 的排队税（+1.71%）稳定大于 D4 的合并收益。
结论：`100` 仍是 winner，D2/D4 不进 oracle。

---

## 5.2 D1 准入 / 预算扫描（2026-09-15）

先补两项 D1 实现（见 §6.5）：**准入上限**（大于 `d1AdmitMaxBytes` 的 range 不进缓存）
和**边读边缓存**（命中不了的可缓存 range 用 tee 流，调用方直接读 S3 流，读满后再发布
到缓存，不再先整段 `byte[]`）。然后按 trace 重放挑出的两个候选做 3 轮扫描。

报告：`docs/adaptive-range-reader/results/track2/track1_d1_budget_sweep/p2_report.json`

| cell | 准入 / 预算 | median | vs `000` | CV | GET | 远端 GiB | 字节变化 | 命中率 | peak heap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `000` | — | 280.84s | — | 0.63% | 23421 | 31.62 | — | — | 12.4 GiB |
| **`100`** | 256KiB / 256MiB | **260.56s** | **−7.22%** | 0.10% | 8370 | 30.69 | −2.9% | 64.3% | **15.0 GiB** |
| `100-4m1g` | 4MiB / 1GiB | 259.76s | −7.51% | 0.95% | 4855 | 25.73 | −18.6% | 79.3% | 17.9 GiB |
| `100-8m2g` | 8MiB / 2GiB | 258.95s | −7.79% | 0.86% | 4334 | 22.57 | −28.6% | 81.5% | 21.4 GiB |

3 轮不满足 `stable`（该标记要求 ≥5 轮），但 CV 0.1%–0.95%，远低于 5% 阈值；
`query_regressions = 0`。

放大预算确实兑现了所有中间指标，甚至超出 trace 重放的预测（重放预测 8MiB/2GiB 命中
72.9%、字节 −24.5%，实测 81.5% / −28.6%，因为真实缓存能跨拼接块做覆盖命中）。
但**墙钟只从 −7.22% 走到 −7.79%**，而且逐查询是对冲的：23 条查询合计快 3.9s，
20 条合计慢 2.6s，净赚 1.3s。代价是峰值堆 +6.4GiB——大块命中时 `tryHit` 每次都要新分配
一个整段 `byte[]`，分配压力随块大小线性放大。

**取 `100` 作为推荐 oracle**：它用 15.0GiB 的堆拿到 7.79 个百分点里的 7.22 个。

---

## 6. 机制诊断

### 6.1 墙钟杠杆小的真正原因：I/O 早就被解码盖住了

这是本轮最重要的发现，它把「缓存收益为什么这么小」从猜测变成了测量。

用 `Track2IoCollectorInterceptor` 的 GET 起止时间戳，算三个不同的量：所有 GET 区间的
**并集**（墙钟里「至少有一个 GET 在飞」的时间）、**时长之和**（TTFB 总量）、以及
**零 GET 在飞的时间**：

| cell | 墙钟 | GET 区间并集 | 占墙钟 | TTFB 之和 | 零 GET 时间 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `000` | 284.0s | 151.5s | 53% | 669.4s | 132.5s |
| `100` | 261.6s | 85.4s | 33% | 261.7s | 176.2s |
| `100-4m1g` | 259.5s | 66.1s | 25% | 167.2s | 193.4s |
| `100-8m2g` | 257.5s | 62.1s | 24% | 154.6s | 195.4s |

三条读法：

1. **I/O 的绝对上限只有约 1/4 墙钟**。`8m2g` 下只有 62s 的墙钟里有任何 GET 在飞，
   剩下 195s 一个请求都没有，全是解码 / shuffle / 聚合。即便把 I/O 变成零成本，
   Track1 的天花板也就 24%。
2. **消掉的 I/O 几乎全是重叠的那部分**。`100 → 8m2g` 砍掉 107s 的 TTFB、23s 的 I/O
   覆盖窗口，墙钟只动 1.6s（median）。换算下来 **1 秒 I/O ≈ 0.05 秒墙钟**。
3. **零 GET 时间随 I/O 下降反而单调上升**（132.5s → 195.4s），墙钟基本不动。这是
   计算受限负载的典型特征：I/O 本来就藏在 16 个并行 task 的解码间隙里。

同一份 trace 还可以反过来验证「不是带宽/服务端排队打满」：GET 延迟对 `inflight_at_issue`
在 ≤18 并发前完全平坦（p50 恒定 24ms，到 20 才升到 32ms）。也就是说链路还有余量，
串行化来自 reader 自己——但因为 I/O 本来就被盖住，预取能抢回的也只有那 62s 里没被
重叠的一小块。

副作用：D1 把 peak heap 从 12.4GiB 抬到 15.0GiB（`100`）/ 21.4GiB（`8m2g`）。
driver 是 32g，`8m2g` 已经占到 67%，这是不推荐它的第二个理由。

### 6.2 零合并的根因：D2/D4 根本没有入队

`Track1GetPipeline.fetch()` 原来只有在 `runtime.asyncClient() != null` 时才 submit；否则直接
`fetchSync()`。但是这条 Spark/S3A 路径只调用 factory 的 `createS3Client()`，没有调用
`createS3AsyncClient()`。所以首轮矩阵虽然 `d2=true/d4=true`，运行时 async 一直是 null。

证据链：

- 首轮所有 D2 cell：`queue_wait_ns = 0`、`merged_gets = 0`；
- 加入队列诊断后、修复前的真实 Q1：`remote_gets = 186`，但 `queue_submissions = 0`；
- `g*` 仍有值，是 `fetchSync()` 在记 link sample，不能证明 D4 执行过；
- 首轮所谓 D2 回退是 sync response 被 `IoUtils.toByteArray()` 整段缓冲、再 synthetic stream
  的成本，不是排队 / async 的成本。

修复：

1. `Track1S3ClientFactory.createS3Client()` 在 D2 开启时额外创建一个 async client；
2. `Track1S3aRuntime.registerOwnedAsync()` 注册并在 runtime 关闭时负责 close；
3. bootstrap 失败且 D1 关闭时 pipeline 返回 `null`，保留原始 streaming sync passthrough，
   不再无意义地整段缓冲；
4. 增加 queue submissions / batches / singleton batches / same-object groups / mergeable groups /
   max batch size / queue wait 等指标，避免再次把“开关为真”误判成“机制已执行”。

### 6.3 修复后 D4 已能合并

真实 ClickBench Q1（单轮）：

| 配置 | 墙钟 | GET | queue submit | merged groups | merged members | 累计 queue wait |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| passthrough | 2.874s | 190 | 0 | 0 | 0 | 0 |
| D2 only | 2.928s | 190 | 186 | 0 | 0 | 687ms |
| D2+D4 50μs | 2.888s | 142 | 186 | 32 | 80 | 505ms |
| D2+D4 1ms | 2.920s | 145 | 186 | 34 | 79 | 571ms |
| D2+D4 5ms | 2.856s | 126 | 186 | 43 | 107 | 1454ms |

Q1–Q7（单轮窗口扫描）：

| wait | 墙钟 | vs passthrough 22.019s | GET | GET 变化 | merged groups | members | waste |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50μs | 22.228s | +0.95% | 2647 | −5.5% | 125 | 278 | 160KB |
| **200μs** | **21.800s** | **−0.99%** | **2607** | **−6.9%** | **136** | **329** | 435KB |
| 500μs | 23.282s | +5.74% | 2595 | −7.3% | 156 | 361 | 406KB |
| 1ms | 22.383s | +1.65% | 2565 | −8.4% | 179 | 414 | 489KB |
| 5ms | 23.881s | +8.46% | 2415 | −13.8% | 257 | 642 | 744KB |

结论：D4 的合并执行已恢复；**200μs 是当前等待 / 合并的单轮拐点**。更长窗口继续降低
GET，但等待成本迅速超过收益。以上只用于机制验证与挑候选，n=1 不能替代正式 5 轮结论。

此外，Track1 位于 SDK factory，看到的是 S3A vectored reader 已按 128KiB gap / 2MiB max
预合并后的物理 GET。D4 能做的是跨并发请求的第二级合并，而不是拿到原始 Parquet ranges；
这限制了它的剩余收益空间。

### 6.4 相对 16.4% 目标

| 台阶 | 墙钟 | 相对原始 baseline（Track2 汇报口径） |
| --- | ---: | ---: |
| 原始布局 + 原生 reader | 317.69s | — |
| Track2 candidate + 原生 reader | 266.08s | −16.25% |
| Track2 candidate + Track1 `100` | 276.17s | 见下 |

注意：276s 是 P2 矩阵里相对 **本矩阵 `000`（282.95s）** 的数，不能和 266s 那次
Track2 E0 直接加减——两次实验不在同一时间窗口。本轮能下的结论只有：

> 首轮同一矩阵里 D1 相对 passthrough 快 **2.4%**，这个单维结论保留。D2/D4 接线无效，
> 所以尚不能计算修复后的联合收益；等待修正版 6×5。

补 2026-09-15：修正版 6×5 与准入扫描跑完后，这一栏的结论是 Track1 在 Track2 candidate
上的最好成绩为 **−7.8%**（`100-8m2g`）/ **−7.2%**（推荐的 `100`），只有 16.4% 目标的
不到一半，且 §6.1 说明差距是结构性的而非调参问题。

### 6.5 D4 的剩余空间已被量化到零

两条独立证据指向同一结论。

**一、合并率**。v2 矩阵里 `011` 向队列提交 23417 次，只合并出 669 次（2.9%），
batch 有 82%–91% 是单例；D1 一开，合并数直接掉到 169。

**二、相邻性**。用 `track1-s3a-probe` 的全量 range 轨迹（23369 条）算：

| 请求大小 | 条数 | 占请求数 | 占字节 |
| --- | ---: | ---: | ---: |
| < 4KiB | 4230 | 18.1% | 0.0% |
| 4–64KiB | 909 | 3.9% | 0.1% |
| 64–256KiB | 11894 | 50.9% | 3.3% |
| 256KiB–1MiB | 1906 | 8.2% | 3.5% |
| 1–4MiB | 2586 | 11.1% | 16.4% |
| 4–16MiB | 1503 | 6.4% | 32.2% |
| > 16MiB | 341 | 1.5% | 44.6% |

`>256KiB` 的请求共 6336 条、占 96.7% 的字节，但只有 **2520 个 distinct range**，
其中只有 **169 个与前一个严格相邻**，平均连续段长度 1.07。也就是说大读之间基本不挨着，
没有可合并的结构。

而 D4 唯一真正能合并的是那 11894 条 64–256KiB 的小读（占请求数 50.9%）——这批恰好就是
D1 以 64% 命中率吃掉的那批。**D1 和 D4 抢同一块矿，D1 赢得更彻底**，所以 `111` 不可能
显著超过 `100`。

另外 Track1 位于 SDK factory，看到的已经是 S3A vectored reader 按 128KiB gap / 2MiB max
预合并后的物理 GET；D4 做的是第二级合并，先天空间就小。

### 6.6 本轮的 D1 实现改动

1. **准入上限** `track1.d1.admit.bytes`（默认 256KiB）。超限的 range 直接 passthrough，
   既不占预算也不被复制。加这条之前「无限准入 + 256MiB 预算」只有 2% 墙钟，因为大扫描
   会把提供绝大多数命中的小读挤出去。**准入上限和预算是一个旋钮不是两个**：trace 重放
   显示 2GiB 预算下 16MiB 准入反而比 8MiB 差（命中 68.0% vs 72.9%）。
2. **tee 流**（`Track1GetPipeline.CachingTeeStream`）。可缓存的 miss 不再先整段
   `byte[]` 再造 synthetic stream，而是让调用方直接读 S3 流、读满后发布进缓存。
3. **`AppCache.findCovering` 改为按对象的有序索引**。原实现在 exact-key 不中时会线性
   扫描全部 block，而整个方法持 `GlobalBudget` 的全局锁——256MiB（约 256 块）还能忍，
   放到 2GiB 就会变成 16 个 task 线程的串行点。改成 `Map<objectId, TreeMap<start>>` 后
   只需在 `[start-maxBlockLen, start]` 这个窗口里回退查找。补了 `AppCacheTest`（5 条），
   此前这个类没有直属单测。
4. 缓存块大小跟随准入上限（`block_bytes = max(1MiB, admit)`），让被准入的 range 尽量
   留在一个块里，重复读走 exact-key 快路径而不是逐块拼接。

---

### 6.7 再往下一层：7.9% 是运行环境决定的，不是机制决定的

§6.1 给出「I/O 被解码盖住」，本节回答「为什么这条负载上的 I/O 这么便宜」。结论是
**7.9% 由 per-GET 成本和 reader 并发两个环境量共同锁死，与数据集无关**。

#### 从扫描数据自己反解 per-GET 成本

`ttfb_sum = n_get × RTT + GiB × (1/BW)`，用 `000` 和 `100-8m2g` 两端联立（§5.2 的表）：

```text
23421·r + 31.62·y = 669.4        r = 26.1 ms          （sysconst SAME_REGION_RTT_S = 25ms，自洽）
 4334·r + 22.57·y = 154.6        1/y = 557 MiB/s
```

| cell | GET | 延迟分量 | 延迟占比 | 传输分量 | K_busy |
| --- | ---: | ---: | ---: | ---: | ---: |
| `000` | 23421 | 611.3s | **91.3%** | 58.1s | 4.42 |
| `100` | 8370 | 218.5s | 79.5% | 56.4s | 3.06 |
| `100-4m1g` | 4855 | 126.7s | 72.8% | 47.3s | 2.53 |
| `100-8m2g` | 4334 | 113.1s | 73.2% | 41.5s | 2.49 |

**D1 砍掉 81.5% 的请求，只砍掉 28.6% 的字节**——它本质是个请求数优化。这有一个直接
推论：**降带宽不会让 Track1 好看**（传输只占 58s/669s），降带宽是 Track2 的杠杆。
唯一能奖励 D1 的环境变量是 per-GET 成本。

#### 26ms 里只有 2.2ms 是网络

实测本机到各区 S3 endpoint 的 TCP connect（≈1 RTT）：

| region | TCP connect | region | TCP connect |
| --- | ---: | --- | ---: |
| us-east-2（本区） | **2.2ms** | eu-central-1 | 99.5ms |
| us-east-1 | 12.6ms | sa-east-1 | 122.0ms |
| us-west-2 | 52.0ms | ap-northeast-1 | 139.5ms |
| eu-west-1 | 78.4ms | ap-southeast-1 | 217.8ms |

同区网络 RTT 只有 2.2ms，而拟合出的 per-GET 是 26.1ms，**差出来的约 24ms 是 S3 服务端
的首字节地板**。这个地板调网络、加带宽都消不掉，只有「不发这个请求」能消掉——这正是
缓存这个机制的立论基础，比单纯谈 RTT 更有说服力。

机器本身也是最不利于暴露 I/O 的一端：`m5dn.4xlarge`，`dn` 是网络优化变体，16 vCPU 配
25 Gbps，bucket 与实例同区。

#### 不可隐藏的 I/O：不需要任何重叠假设

定义 `不可隐藏 I/O = max(0, GET区间并集 − 计算地板)`，计算地板取 `task CPU / 16` = 181s：

```text
000:   max(0, 152s − 181s) = 0s
```

**同区环境下不可隐藏的 I/O 恰好是零。** 这一行就是 7.9% 的全部解释：不是缓存不行，是
墙钟里没有留给它的份额。缓存该做的事全做到了（命中率 81.5%、请求 −81.5%、字节 −28.6%）。
反过来说，模型算出空间为零而实测仍赚了 7.9%，说明单 task 内部的读仍在阻塞自己的计算，
所以下面的外推是偏保守的。

#### K 不随延迟上升（这是外推能成立的关键）

外推唯一的不确定量是 `K_busy = ttfb_sum / union`。已有两个实测点：

| | per-GET | K_busy |
| --- | ---: | ---: |
| 本机同区 ClickBench（§5.2） | 26ms | 4.42 |
| 腾讯云 E2 baseline（`sysconst.json` 种子值） | 228ms | **6.77** |

延迟 8.7 倍，在飞并发只涨 1.5 倍。机制上成立：vectored read 对每个 row group 发一批
有界的 range（`active.ranged.reads = 4`）然后阻塞等这批完成，在飞数由批大小决定，
不由延迟决定。所以「K 随 RTT 等比上升、把 I/O 重新藏回去」这个分支可以排除。

保留意见：6.77 跑的是 TPC-H 而非 ClickBench，机器也不同，因此它能排除悲观分支，
不能当精确值用。跨云那次测量作为**部署场景**已被否决（见 §9），此处只把它当作
「同一套冻结 reader 在高延迟下的并发行为」的一次观测来使用，不进任何上报结果。

#### 外推

同一份代码 / 同一套 ClickBench / 同一批布局，只改 bucket 所在区域：

| region | per-GET | `000` 并集 | `8m2g` 并集 | Track1 收益（K=同区实测） | （K=6.77） |
| --- | ---: | ---: | ---: | ---: | ---: |
| us-east-2（现状） | 26ms | 152s | 62s | −38%..44% | −16%..33% |
| us-east-1 | 37ms | 207s | 80s | −30%..52% | −20%..41% |
| us-west-2 | 76ms | 415s | 149s | 19%..69% | 11%..59% |
| **eu-west-1** | **102ms** | 555s | 195s | **31%..74%** | **29%..65%** |
| eu-central-1 | 123ms | 667s | 231s | 37%..73% | 37%..70% |
| ap-southeast-1 | 242ms | 1294s | 437s | 52%..70% | 59%..82% |

区间下界取的是最悲观配对（基线按纯 I/O 计、候选按 I/O 与计算完全串行计）。26ms 那一行
跨过零，说明这个模型在同区本来就「说不准」，与实测 7.9% 一致——不是事后凑的。

模型假设跨区带宽不变，实际跨区单连接吞吐受 BDP 限制会更差；而基线读 31.6GiB、
`8m2g` 只读 22.6GiB，带宽变差对基线伤害更大，所以这一项只会让 Track1 的真实收益
高于预测。

复算：`python3 tools/track2/rtt_regime_project.py`。输入全部来自 §5.2 与 §6.1 的表
加上一次 endpoint 延迟测量，脚本本身只做算术，不碰网络也不碰 S3。

---

## 7. 实现与资产

### 7.1 Java（`services-custom/s3-adaptive-range-reader`）

| 类 | 职责 |
| --- | --- |
| `s3a.Track1S3ClientFactory` | Hadoop 扩展点；包装 sync/async client |
| `s3a.PassthroughS3Clients` | JDK proxy；sync `getObject` 进 pipeline |
| `s3a.Track1S3aRuntime` | 进程单例：cache / queue / estimator / stats |
| `s3a.Track1GetPipeline` | D1 → D2/D4 → fallback |
| `s3a.Track1RangeCache` | D1 covering / 拼接 / ETag |
| `s3a.Track1GetQueue` | D2 单 dispatcher 队列 |
| `s3a.RangeMerger` | D4 贪心合并 |
| `s3a.LinkEstimator` | RTT / BW EWMA，输出 `g*` |
| `s3a.Track1S3aProbe` | P0 调用探针 |
| `s3a.Track1S3aStats` | hits / merged / waste / heap |
| `internal.RuntimeConfig` | `-Dtrack1.*` 解析 |

### 7.2 工具

| 文件 | 职责 |
| --- | --- |
| `tools/track2/s3a_session.py` | `--track1-s3a` 与 `-Dtrack1.d1/d2/d4` |
| `tools/track2/run_benchmark.py` | 单次 43 查询 + `snapshotJson` |
| `tools/track2/run_track1_matrix.py` | 交错矩阵与 oracle 报告；cell 是 `Cell` NamedTuple，带准入 / 预算 |
| `tools/track2/tests/test_track1_matrix.py` | schedule / resume / 排名 / 准入扫描 cell 的 argv |
| `tools/track2/rtt_regime_project.py` | §6.7 的 regime 外推；纯算术，输入是已有的表 |

### 7.3 系统属性

| 属性 | 默认 | 含义 |
| --- | --- | --- |
| `track1.d1` / `d2` / `d4` | false | 维度开关 |
| `track1.d2.wait.us` | 0 | D2 攒批窗口 |
| `track1.d1.block.bytes` | 1 MiB | 缓存块；应 ≥ 准入上限 |
| `track1.d1.cache.mib` | 256 | 缓存预算；必须与准入上限同向调 |
| `track1.d1.admit.bytes` | 256 KiB | D1 准入上限，0 = 全部准入 |
| `track1.d2.workers` | 16 | 并发 GET 上限 |
| `track1.d4.max.fetch.mib` | 8 | 单次合并上限 |
| `track1.d4.max.waste` | （配置内默认） | 浪费字节比例上限 |
| `track1.s3a.probe.dir` | 关 | 探针 NDJSON 目录 |

### 7.4 复跑

```bash
python3 tools/track2/run_track1_matrix.py \
    --data s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2 \
    --out docs/adaptive-range-reader/results/track2/track1_p2_matrix
```

D1 准入 / 预算扫描（§5.2）：

```bash
python3 tools/track2/run_track1_matrix.py \
    --cells 000,100,100-4m1g,100-8m2g --runs 3 \
    --out docs/adaptive-range-reader/results/track2/track1_d1_budget_sweep
```

单格：

```bash
python3 tools/track2/run_benchmark.py \
    --data s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2 \
    --track1-s3a --track1-d1 \
    --out docs/adaptive-range-reader/results/track2/track1_p2_matrix/cells/100/manual
```

---

## 8. 未做 / 明确不做

- D3 门控预取、D5 长尾对冲。等 D1 主链稳定且有证据再开。
- 在线决策头 / 每维浅树。P3 第一版是可解释 soft controller，不是树。
- Track1 × Track2 的 2×2 正式实验。P4 等 P3 交付物（online 或固定 `100`）。
- E0 GET 轨迹重放。
- 不为 P3 重写 joint2 / SF8 joint2；第一轮只在 canonical SF1 / SF8 上测自适应缓存。
- 把本轮数字写进 `PROJECT3.md`。阶段总路线仍在那里；Track1 的实现与实验只维护本文件。

---

## 8.1 P3 自适应缓存（进行中，2026-09-20）

同区固定档位已经压平，P3 不再把绝对 MiB 当最终策略。控制器只软控制
`AppCache`：不重建、不 stop-the-world 清空。

- **SDK telemetry**：两个 `WorkingSetWindow`。admission 窗看全部 range；
  budget 窗只收 `≤ admit_max` 的可准入子集。窗口按字节 horizon 滚动
  （默认 4GiB，事件上限 131072 只作安全阀），不再用固定 4096 条尾窗。
  不读 `query_id`、SQL、数据集总大小或 `sf1/sf8` 标签。
- **soft bypass / admission / resize**：`D1SoftController` 四态
  OBSERVE → TRACK / BYPASS / SHRINK。`target=0` 停准入并 LRU 渐出；升高 target
  不驱逐；`GlobalBudget` 仍是 hard cap（adaptive 默认 4096 MiB）。
  **预算有空也不抬 admission**；第一版 `admit_max` 锁在保守 256KiB。
  `target_budget = clamp(B_min, coverage × R_admit, B_hard)`，coverage 默认 1.0
  以便 SF8 自然靠近约 1GiB、SF1 停在 ≤256MiB。
- **开关**：`-Dtrack1.d1.adaptive=true`，覆盖率 `-Dtrack1.d1.coverage`（默认 1.0），
  horizon `-Dtrack1.d1.horizon.mib`（默认 4096），heap 高/低水位 0.70 / 0.50。
  冻结路径行为不变。
- **跑手**：`tools/track2/run_track1_p3.py`。P3-0 先在
  `s3a://home-haoyue/track2/clickbench_sf1` 与 `clickbench_sf8` 上跑
  `000 / 100 / 4m1g / 8m2g` 各 1 轮，再对可能的 winner 和 `online` 补轮。
- **判据**：墙钟为主。online 用同一套 coverage/heap 参数跨规模；比不过各自固定
  oracle 超过 1%，就诚实交付 `100`。
- **论文伴随成本表**（`tools/track2/track1_cost_ledger.py`，`used_for_ranking=false`）：
  只回答「墙钟快了的同时付了什么」，不进 oracle。每格对照 `000` 记：
  GET 数与 GET \$、远端字节与传输 \$（同区 \$0）、peak heap、缓存占位、
  tee 拷贝、驱逐、GC、机时（= 墙钟比）。heap 只有越过 85% Xmx 才写成
  「必须换更大机型」。`tee_efficiency = useful / teed` 写拷贝是否回本。
- **已有 joint2 固定档乐观界**（`track1_d1_budget_sweep`，不是 canonical SF1）：
  逐查询无代价切换上界 254.4s，相对 `8m2g` 只多 1.8%，相对 `100` 只多 2.4%。
  查询感知切换在同区几乎没有墙钟空间；P3 的价值在跨规模免调参和省堆。

### P3-0 筛选轮（n=1，canonical 布局，2026-09-20）

墙钟排名；成本只作伴随。两规模各自的固定 oracle 都指向 **`100`**（SF1 上
`4m1g` 快 0.9% 但在误差带内；SF8 上大预算明确回退）。

| 规模 | cell | wall | vs `000` | GET | 远端 | peak heap | 缓存占位 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SF1 | `000` | 321.7s | — | 33038 | 46.6GiB | 6.4GiB | 0 |
| SF1 | **`100`** | **275.0s** | **−14.5%** | 9805 | 45.9GiB | 17.6GiB | 126MiB |
| SF1 | `4m1g` | 272.6s | −15.3% | 5743 | 40.0GiB | 19.9GiB | 1.00GiB |
| SF1 | `8m2g` | 273.0s | −15.2% | 5107 | 36.6GiB | 21.8GiB | 2.00GiB |
| SF8 | `000` | 2225.1s | — | 262309 | 371.8GiB | 6.6GiB | 0 |
| SF8 | **`100`** | **1996.0s** | **−10.3%** | 105339 | 367.7GiB | 21.0GiB | 256MiB |
| SF8 | `4m1g` | 2135.0s | −4.1% | 183572 | 369.8GiB | 25.6GiB | 1.00GiB |
| SF8 | `8m2g` | 2134.6s | −4.1% | 167627 | 366.8GiB | **30.8GiB** | 2.00GiB |

要点：

1. **最佳绝对 MiB 随规模变了含义，但最佳策略没变。** SF1 上加大预算几乎不换墙钟；
   SF8 上 `4m1g`/`8m2g` 比 `100` 慢约 7%，GET 反而更多（命中率 60% → 30%/36%）。
   大 range 准入把可复用的小热点挤出去了。
2. **可复用工作集不随数据线性涨。** SF8 是 8 份月份副本，墙钟 / GET 约 7×，不是
   8× 的纯扫描放大。把缓存按 8× 加到 2GiB 是错的。
3. **成本伴随（不进排名）。** 同区传输 \$0。SF8 `100` GET 约 −\$0.063/轮，堆 +14.4GiB、
   未换机；`8m2g` peak 30.8GiB 越过 85% × 32g，是唯一的换机风险。机时随墙钟：
   `100` −10%，大预算只有 −4%。
4. **窗口遥测的限制。** 快照里的 `U_H`/`R_H` 只覆盖最近 4096 个 exact range。SF8
   一轮 26 万次观察，尾窗落在收尾小读上（`R_H≈21MiB`），**不能**和 SF1 的 2.3GiB
   比。控制器要用这个窗口做在线决策时，必须先证明窗口统计对 SF8 仍稳定，或改
   成按字节而不是按条数滚动。
5. 逐查询无代价切换：SF8 相对 `100` 只多 1.3%。查询感知切换仍然没有墙钟空间。
6. **准入和预算是两维，不能再绑成一档。** SF8 budget-only（准入锁 256KiB，n=1）
   已证实：变差是准入，不是预算。

   | cell | 准入 | 预算 | wall | vs `100` | GET | vs `100` | 远端 vs `100` | 占位 |
   | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
   | `100` | 256KiB | 256MiB | 1996s | — | 105339 | — | — | 256MiB 满 |
   | **`100-1g`** | 256KiB | 1GiB | 1987s | −0.5% | **77830** | **−26%** | −1.66GiB | 991MiB |
   | `100-2g` | 256KiB | 2GiB | 1984s | −0.6% | 77830 | −26% | −1.66GiB | 991MiB |
   | `4m1g`（对照） | 4MiB | 1GiB | 2135s | +7.0% | 183572 | +74% | +2.1GiB | 1GiB |

   `100-1g` 与 `100-2g` 的 GET/字节/命中完全一样：已准入可复用集大约 1GiB，
   再加到 2GiB 是空头寸。拒纳次数三格同为 58013，确认准入没动。墙钟只快
   0.5%（同区安全门内），机制有效性在 GET −26%。
   - 准入 = 过滤器：谁可以进。信号是重用尺寸，**预算有空也不抬 cap**。
   - 预算 = 水库：留下多少已准入字节。SF8 上约 1GiB，不是 256MiB，也不是 2GiB。
   - 之后按两维独立自适应，不再使用 `4m1g`/`8m2g` 联合档。

   **256KiB 不是扫出来的准入最优。** 它来自三件事叠在一起：旧页缓存习惯
   （PROJECT2 的 64–256KiB page）、GET **条数**众数在 64–256KiB（§6.5），以及
   「无上限准入会把小读挤掉」。真正做过的对照原先只有 256KiB vs **4/8MiB 且预算一起涨**。
   SF1 锁预算=1GiB 的 admit-only 已补上（见下节）：GET 随 cap 单调下降，2MiB 最优。

   用本轮 SF1 `000` 的 33034 条 exact-range 轨迹按尺寸算重用（同一
   `(object,start,end)` ≥2 次）：

   | 尺寸桶 | GET | distinct | key 重用率 | 可复用 unique |
   | --- | ---: | ---: | ---: | ---: |
   | 4–64KiB | 10230 | 1598 | 58% | 34MiB |
   | 64–128KiB | 6794 | 2760 | 61% | 109MiB |
   | 128–256KiB | 709 | 199 | 56% | 21MiB |
   | **256–512KiB** | 1192 | 278 | **75%** | **77MiB** |
   | **512KiB–1MiB** | 952 | 254 | **70%** | **132MiB** |
   | 1–2MiB | 1430 | 358 | 72% | 385MiB |
   | 2–4MiB | 1671 | 362 | 87% | 893MiB |

   累计可复用工作集：`≤256KiB` → 163MiB；`≤512KiB` → 240MiB；`≤1MiB` → 372MiB；
   `≤2MiB` → 757MiB；`≤4MiB` → 1.65GiB。4MiB 在 SF8 变差，更像是 **R 超过 1GiB
   水库把小热点挤掉**，不是「>256KiB 的 range 不能缓存」。online 把 cap 锁死在
   256KiB 是未完成的 admission 维。SF1 admit-only 实测见下节。

### P3-2 SF1 smoke（n=1，grow-only JAR）

| cell | wall | vs `000` | vs `100` | GET | occupancy | target |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `000` | 329.9s | — | — | 33038 | 0 | — |
| `100` | 277.1s | −16.0% | — | 9805 | 132MiB | 256MiB |
| **online** | **280.8s** | **−14.9%** | **+1.4%** | **9805** | **132MiB** | **256MiB** |

grow-only 之前的 online 把 target 收到 163MiB，GET 11066、占位 111MiB。修好后
GET/命中/占位与 `100` 完全一致；墙钟 +1.4% 落在 n=1 抖动带，机制已贴上 SF1
固定点。SF8 smoke 另目录跑，不与这轮混。

### P3-2 SF8 smoke（n=1，grow-only JAR）

报告：`docs/adaptive-range-reader/results/track2/track1_p3_sf8_p32/p3_report.json`

| cell | wall | vs `000` | vs `100` | GET | vs `100` | occupancy | target |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `000` | 2229.4s | — | — | 262309 | — | 0 | — |
| `100` | 2020.0s | −9.4% | — | 105346 | — | 256MiB 满 | 256MiB |
| **`100-1g`** | **1984.7s** | **−11.0%** | **−1.8%** | **77830** | **−26.1%** | **991MiB** | 1GiB |
| online | 2002.3s | −10.2% | −0.9% | 95577 | −9.3% | 658MiB | 930MiB |

`100-1g` 与 P3-0 budget-only 一致（GET 77830、占位 991MiB）。online 从 256MiB 往上涨到
target 930MiB / 占位 658MiB，墙钟落在 `100` 与 `100-1g` 之间，GET 只走完 `100`→`100-1g`
降幅的约三分之一。22 次 BYPASS 打断过增长。admission 仍锁 256KiB。

### P3 admit-only（SF1 n=1，预算锁 1GiB）

报告：`docs/adaptive-range-reader/results/track2/track1_p3_sf1_admit_only/p3_report.json`

只动准入，预算一律 1GiB（`100` 仍是旧默认 256KiB/256MiB）。

| cell | 准入 | 预算 | wall | vs `000` | GET | vs `256k-1g` | occupancy | 驱逐 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `000` | off | — | 329.6s | — | 33038 | — | 0 | 0 |
| `100` | 256KiB | 256MiB | 280.5s | −14.9% | 9805 | 0% | 126MiB | 0 |
| `64k-1g` | 64KiB | 1GiB | 281.7s | −14.5% | 10798 | +10.1% | 76MiB | 0 |
| `128k-1g` | 128KiB | 1GiB | 282.0s | −14.4% | 10357 | +5.6% | 94MiB | 0 |
| `256k-1g` | 256KiB | 1GiB | 279.5s | −15.2% | 9805 | — | 126MiB | 0 |
| `512k-1g` | 512KiB | 1GiB | 279.1s | −15.3% | **8832** | **−9.9%** | 216MiB | 0 |
| `1m-1g` | 1MiB | 1GiB | 277.4s | −15.8% | **8048** | **−17.9%** | 378MiB | 0 |
| **`2m-1g`** | **2MiB** | 1GiB | **277.0s** | **−16.0%** | **6888** | **−29.8%** | **837MiB** | 0 |

要点：

1. **256KiB 不是 GET 最优。** 锁住 1GiB 后 GET 随 cap 单调下降：64k→2m 为 10798→6888。
   轨迹里 256KiB–2MiB 的高重用在真实路径上兑现了。
2. **`100` 与 `256k-1g` GET/占位完全一样**（9805 / 126MiB）。SF1 在 256KiB 过滤下工作
   集约 126MiB，多给的 1GiB 用不上。
3. **墙钟几乎被 CPU 盖住**：2m 只比 256k 快 0.9%，但 GET −30%。同区仍以 GET/字节为
   机制证据，墙钟只作安全门（没有任何一格比 `000` 慢）。
4. 1GiB 在 2MiB 准入下仍未装满（837MiB、驱逐=0、heap ~19GiB 无换机）。SF1 上把 cap
   收到 256KiB 是把可缓存的中等 range 拒掉了，不是水库不够。
5. n=1，墙钟排序不可当作正式 oracle；GET 排序方向与轨迹预测一致。SF8 上 4MiB 仍
   可能污染，不能把 2MiB 直接外推成跨规模 cap。

下一步：P3-1 双窗已落地。P3-2 先各跑 1 轮 online smoke（SF1：`000/100/online`；
SF8：`000/100/100-1g/online`）。同区墙钟不是唯一有效性：S3 时延太低时，I/O 藏在
CPU 后面（§6.1 不可隐藏 I/O = 0）。**墙钟仍是安全门**（不比 `000` 慢、不比
`100` 回退超过约 1%），但「这一维有没有用」还要看 GET 和远端字节——这两项
会随 per-GET 成本线性放大，是换区后墙钟的库存。budget-only 若墙钟贴着 `100`
但 GET/字节明显下降，仍算小准入 + 大水库成立，应拆两维；只有墙钟持平 *且*
GET/字节也不动，才交付固定 `100`。不读 `sf8` 或总字节。

## 9. 下一步

当前执行的是 **P3-0 SF1/SF8 工作集标定**，不是换区。§6.7 的 eu-west-1 仍是
regime 外推，等 P3 数字落地后再排。§6.1 之后「继续调固定 Track1 参数」已无期望值。

0. **把 bucket 换到 `eu-west-1` 重跑，这是第一优先。** per-GET 从 26ms 到 102ms，
   两种 K 假设下单 Track1 都外推到 ≥29%（§6.7）。选它而不选 `ap-southeast-1`（242ms、
   外推更高）的三个理由：跨区出口费 \$0.02/GB vs \$0.09/GB（整个实验 \$5 vs \$30）；
   合理性论证最硬（数据驻留合规导致数据在 EU、算力在美国，是真实且普遍的部署）；
   29% 已经过了 16.4% 的门，不需要再往极端跑。
   - **代码不用改**：`s3a_session.REGION` 读 `AWS_DEFAULT_REGION`，改环境变量即可。
   - **卡在权限上**：`s3:CreateBucket` 对用户 `haoyue` 是 AccessDenied，账号里 17 个
     bucket 全在 us-east-2。需要管理员在 eu-west-1 建一个桶（约 22GiB 放 baseline +
     joint2 两个布局，存储费 ~\$0.5/月）。
   - 矩阵带 `000` / `100` / `8m2g` 三个 cell：per-GET 贵 4 倍之后，`8m2g` 多砍一倍请求
     这件事会重新值回它的 6.4GiB 堆，§5.2 的预算结论在新 regime 下要重测。
   - **Track2 也要在同一 regime 下重跑**：更大的 row group 与列序同样在减少请求数，
     16.25% 只是同区那个点的值，联合 2×2 的基线会整体平移。
   - 报的是**曲线不是单点**：26ms 这个零结果留在最左端。「SDK 层缓存在 per-GET 成本
     超过某个拐点后才转化为墙钟」本身就是结论，也顺带免疫「是不是挑了个友好环境」
     这个必然会被问到的质疑。
   - 跨云（腾讯云 → S3，228ms）作为部署场景**已否决**：不是常规部署。跨区是同一延迟
     区间内、全程 AWS 内部、任何人可复现的替代，并且可以当作 on-prem / hybrid Hadoop
     读 S3 的代理——S3A 这个 connector 本来就是为那类场景而存在的。
1. **`100`（D1 only，256KiB / 256MiB）是同区 regime 下的推荐配置**。同区补满 5 轮的
   价值已经不大（CV 0.10%，且这个 regime 本身要被换掉）；5 轮留给 eu-west-1 的正式矩阵。
   不要用 `8m2g`：同区多 0.57 个百分点要多 6.4GiB 堆。
2. **D2 / D4 从动作空间移除**，理由见 §6.5（合并率 2.9%、大读平均连续段 1.07、
   与 D1 抢同一块矿）。保留代码和开关，不再进 oracle 搜索。
3. **D3 预取的期望值要按 §6.1 的比例先算再做**：可攻击的墙钟只有 62s，而且这 62s 里
   大部分本来就与解码重叠，1 秒 I/O 只值 0.05 秒墙钟。除非先证明存在「不重叠的关键路径
   I/O」，否则不值得实现。**注意这条在新 regime 下要重新评估**：eu-west-1 的不可隐藏
   I/O 是 374s 而不是 0s，预取第一次有了攻击面。
4. **16.4% 的目标需要改口径。** 现在有测量支撑的说法是：Track1 在这条负载上的天花板
   是 I/O 覆盖窗口的 24%，实际能兑现约 7%。剩下的 69%–76% 墙钟是解码 / shuffle，
   属于 Track2 的物理布局与编码维度。
   **补充（同日）**：那 76% 里的解码部分随后也被实测否掉了——一次只改 3 列编码、
   task 数固定的 A/B 把解码总量定位在 task CPU 的 2.4%（约 1.5% 墙钟），
   `decode_rerank` 预测的 7.8% 不成立。见 `DECODE_AXIS.md`。所以剩余墙钟既不在
   I/O 也不在解码，而在过滤 / 聚合 / shuffle。
5. **唯一还没被排除的读路径方向是提高 reader 并发**
   （`fs.s3a.vectored.active.ranged.reads` 当前 4，延迟在 18 并发前平坦）。但它属于
   TRACK2_M0_CONTRACT 1.4 的冻结 reader 配置，改了就失去与 Track2 基线的可比性——
   要动必须先单独立项、重做基线。
6. **2×2 联合实验继续推迟**，理由同上。

---

## 10. 更新日志

- **2026-09-22**：D1 本地缓存热路径改造完成本地验证，云上消融待具备 PySpark 的 benchmark
  主机执行，不能将其计入任何 oracle 或墙钟结论。
  - 请求分块改为 descriptor-first：准入比较和完整 victim plan 在 payload 拷贝前完成；被拒
    请求的 `d1_rejected_payload_copy_bytes` 应为 0。成功准入时可转移 tee/fetch buffer
    所有权，让相邻块引用同一 immutable backing；兼容 A/B 档仍可选择成功后复制。
  - hit 采用一次锁定的 `pinCoveringSpan` 和关闭时释放的 composite stream；不在 cache lock
    内 materialize。被 pin 的块不会作为驱逐 victim，因而未关闭的响应不会形成未计入 hard-cap
    的 payload。
  - adaptive 首触 doorkeeper 只更新频率并以计时 passthrough 读取，不分配 tee buffer；
    重复访问再参与准入。`read()` 逐字节路径不再分配 `byte[1]`。
  - 诊断开关 `track1.d1.profile=true` 输出 lookup、hit-copy、stitch、put、tee-copy
    bytes/nanos；`track1_overhead_ledger.py` 明确标记 `used_for_ranking=false`。
    临时消融档为 baseline / deferred-copy / zero-copy / full，可由
    `run_track1_p3.py --phase hotpath` 运行；`--cells` 可选 4GiB 档用于 SF8。
  - 本地模块完整测试通过；本机尝试 SF1 smoke 时在创建 Spark 前失败：
    `ModuleNotFoundError: pyspark`。因此没有新增 SF1/SF8 墙钟、GET 或远端字节结果。
- **2026-09-21**：P3-1 双窗落地。`WorkingSetWindow` 按字节 horizon 滚动；
  admission / budget 分窗；`admit_max` 不随空预算上浮；coverage 默认 1.0；
  TRACK 只升不降。SF1 P3-2 smoke：`000` 329.9s / `100` 277.1s / online 280.8s，
  GET 与 `100` 同为 9805、占位同 132MiB。墙钟 +1.4% 视为 n=1 抖动。
  下一步 SF8 `000/100/100-1g/online`。SF8 smoke（n=1）：`000` 2229s / `100` 2020s /
  `100-1g` 1985s GET 77830 / online 2002s GET 95577、占位 658MiB、target 930MiB。
  admit-only 改到腾讯云 CVM。后在本机 SF1 补跑：锁 1GiB 后 GET 随 cap 单调下降，
  `2m-1g` GET 6888（相对 `256k-1g` −30%），墙钟只再快 0.9%。256KiB 不是 GET 最优。
- **2026-09-15（晚）**：把 7.9% 的归因从「机制」推到「运行环境」，并定出换 regime 的
  方案（§6.7 / §9.0）。本条**没有任何新 benchmark**，输入全部是 §5.2 / §6.1 已有的表
  加上一次 endpoint 延迟测量。
  - 从 D1 扫描两端联立反解出 per-GET = 26.1ms、557 MiB/s；`000` 的请求时间里
    **91.3% 是延迟、只有 58.1s 是传输**。推论：D1 是请求数优化（−81.5% 请求 /
    −28.6% 字节），降带宽帮不到它，那是 Track2 的杠杆。
  - 实测同区 S3 网络 RTT 只有 **2.2ms**，所以 26.1ms 里约 24ms 是 S3 服务端首字节
    地板，只能靠「不发请求」消掉。机器 `m5dn.4xlarge` 是网络优化变体（16 vCPU /
    25 Gbps）且与 bucket 同区，是最不利于暴露 I/O 的配置。
  - `不可隐藏 I/O = max(0, 152s − 181s) = 0s`：同区墙钟里没有留给读路径优化的份额。
  - 找到高延迟下的 K 实测值：`sysconst.json` 的腾讯云种子 `K_busy = 6.77` @ 228ms，
    对比本机 4.42 @ 26ms——延迟 8.7 倍、并发只 1.5 倍，排除「K 等比上升把 I/O 藏回去」
    的悲观分支。机制：vectored read 每 row group 发一批有界 range 后阻塞等完成。
  - 外推：`eu-west-1`（per-GET 102ms）下单 Track1 = 29%..74%，两种 K 假设都 ≥29%。
    推荐它而非 ap-southeast-1（出口费 4.5 倍、论证不更硬）。
  - 阻塞项：`s3:CreateBucket` AccessDenied，需管理员在 eu-west-1 建桶。
  - 同期的解码侧结论见 `DECODE_AXIS.md`：解码只占 task CPU 的 2.4%（~1.5% 墙钟），
    所以「把缺口交给 Track2 解码侧」这条路已排除，换 regime 成为唯一有期望值的动作。
- **2026-09-15**：D1 准入 / 预算扫描，以及「墙钟杠杆为什么小」的测量定论。
  - 实现：准入上限 `track1.d1.admit.bytes`、tee 流、`AppCache` 按对象有序索引
    （+ 新增 `AppCacheTest`）、`run_track1_matrix.py` 的 cell 改为带准入/预算的
    `Cell` NamedTuple。
  - 扫描（3 轮，`track1_d1_budget_sweep`）：`000` 280.84s；`100` 260.56s（−7.22%）；
    `100-4m1g` 259.76s（−7.51%）；`100-8m2g` 258.95s（−7.79%）。
  - 中间指标全部兑现且超出 trace 重放预测：命中率 64.3% → 81.5%，网络字节
    −2.9% → −28.6%，GET 8370 → 4334。墙钟只多赚 0.57 个百分点，峰值堆 +6.4GiB。
  - 机制定论（§6.1）：`8m2g` 下只有 62s/257s 墙钟有 GET 在飞，零 GET 时间反而从
    132.5s 升到 195.4s；1 秒 I/O ≈ 0.05 秒墙钟。Track1 的天花板是结构性的。
  - D4 剩余空间量化为零（§6.5）：大读 2520 个 distinct range 只有 169 个严格相邻。
  - 推荐 oracle 改为 `100`；D2/D4 退出动作空间；16.4% 目标需改口径。
- **2026-09-14**：启动修正版 6×5（`track1_p2_matrix_v2`）。
  - 使用含 async bootstrap 的 JAR；`011/111` wait = 200μs；不 resume 旧矩阵。
  - 日志：`docs/adaptive-range-reader/results/track2/track1_p2_matrix_v2.log`
  - 报告（跑完才有）：`docs/adaptive-range-reader/results/track2/track1_p2_matrix_v2/p2_report.json`
- **2026-09-14**：D4 零合并排查与修复。
  - 根因不是 range 判定，而是 S3A 没有创建 async client，D2/D4 从未 submit。
  - factory 在 D2 开启时自建托管 async client；bootstrap 失败保留原始 streaming sync 路径。
  - 增加队列 / same-object / mergeable / budget fallback 诊断指标。
  - 修复后 Q1–Q7：200μs 合并 136 组 / 329 成员，GET 2800 → 2607（−6.9%），
    墙钟 22.019s → 21.800s（−1.0%，n=1）。后续矩阵 D4 wait 改为 200μs。
  - 首轮 P2 的 D1 数字保留；D2/D4 格标为无效，等待重跑。
- **2026-09-14**：创建本文件，记录 P0–P2。
  - P0：`Track1S3ClientFactory` + JDK proxy passthrough；Q1–Q7 正确性过门；入口是
    sync `getObject`。
  - P1：D1/D2/D4 基础版，默关，可独立开关；D4 单 dispatcher；D1 命中回带 ETag。
  - P2：6×5 交错矩阵跑完。联合 oracle = `100`（D1 only），墙钟 −2.39%、GET −34.7%；
    D2 单独回退；D4 `merged_gets = 0`。正式报告见
    `docs/adaptive-range-reader/results/track2/track1_p2_matrix/p2_report.json`。
