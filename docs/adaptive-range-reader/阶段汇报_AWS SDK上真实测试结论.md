# 阶段汇报 —— AWS S3 真实云侦察轮结论（修正版）

> 对应 `PROJECT2.md` §8.6–§8.8 留下的「真实 latency 门禁」，以及 `PROJECT3.md` §3.4 / §3.5 / §6 中必须实测才能回答的问题。
> 本轮是**侦察 / 可行性验证**，非正式投稿数字：每配置 1–2 次、矩阵刻意收紧。正式轮恢复 ≥5 次 median。
> 全部原始数据见本文 §8「实验产物索引」。

---

## 0. 一句话结论

在 us-east-2 同区 EC2 → S3 上重放 `mixed_holdout`（11,189 reads / 15.8 GiB 对象）：

1. **缓存收益是真实的，且被合成实验低估**（wall-clock −40.4%、GET −54.9%；合成实验估 IO −23.7%、GET −43.7%）。
2. **决策树输给了最佳静态策略 3.98%**——不是「树不够深」，而是标签来源、口径、工作点三层都错了。这棵树按 PROJECT3 本就要降级为 baseline，**不要重训**。
3. **投机预取在真实网络下依然无效**。默认应锁 **depth = 0**（不是 1）。
4. **cost proxy 保序完美（Spearman = +1.000），绝对值系统性偏高**。RTT 假设 50ms 实际 25ms；BW 假设 100 MiB/s 实际 102 MiB/s。
5. **g\* ≈ RTT × BW 在真实云上站得住**（预测 1.7–2.6 MiB，实测最优在 1 MiB，4 MiB 仍接近）。
6. **D5 长尾降级为次要维度**（等尺寸探针 P99/P50 = 2.58 / Java 复测 2.79，均 &lt; 3；高并发下比值往往更低）。
7. **同区 S3 是「25ms RTT × 单连接 ~100MiB/s × 多连接 ~1GiB/s 封顶」**。小读杠杆是合并（D4），大读杠杆是有节制的并发（D2，甜区约 16 路），缓存两边都吃。
8. **PROJECT2 四分类动作空间可以关闭**；主线回到 PROJECT3 的五维参数化（D1 缓存 / D2 同步转异步 / D3 门控预取 / D4 请求合并 / D5 长尾）。

本轮最有价值的产出不是某个百分比，而是：**PROJECT2 的 cost-proxy 立论在真实 wall-clock 下部分翻车，而「换动作空间」的判断被坐实。**

---

## 1. 实验设置

| 项 | 值 |
| --- | --- |
| 客户端 | `m5dn.4xlarge`，16 vCPU / 62 GB，us-east-2c |
| 对象存储 | `s3://home-haoyue/`，同 region |
| Trace | `traces/mixed_holdout_s3.csv`（key 带 `mixed_holdout/` 前缀） |
| 对象 | 33 个确定性合成字节对象，共 15.79 GiB（**不是** `data.tar.gz` 里的原始 parquet） |
| SDK | 模块 `s3-adaptive-range-reader`，JDK 17 编译、release=8 |
| 重复 | E2 ×2；E3 256MiB 四列 ×2；其余 ×1 |
| 主指标 | 整条 trace wall-clock；GET / remote bytes / 读放大并列报告 |
| 禁止作为优化目标 | `cacheHitRate`、单独的 remote bytes |

一个干净的对照前提：128MiB 下真实 S3 跑出的 IO 形状与 PROJECT2 §8.7 的表**逐位一致**（passthrough 11189 GET / 2468.16 MiB、template_auto 5860 / 3037.28、决策树 5595 / 2748.67）。唯一的新变量是时间，所有差异都能归因到 wall-clock。

---

## 2. 有效果的部分

### 2.1 缓存收益真实，且比合成实验更高 ✅

`mixed_holdout`，@256MiB，depth=0：

| 配置 | GETs | remote MiB | wall-clock | vs passthrough |
| --- | --- | --- | --- | --- |
| passthrough | 11189 | 2468.2 | 394.4s | — |
| template_auto | 5360 | 2701.2 | 248.2s | −37.1% |
| 决策树 S2 | 5042 | 2262.6 | **234.9s** | **−40.4% / GET −54.9%** |

PROJECT2 §9 用 `rtt=5ms / bw=200MiB·s⁻¹ / think=5ms` 估出 IO 时延 −23.7%、GET −43.7%。真实同区 S3 的 RTT 约 25ms、有效带宽约 100 MiB/s，RTT 项更重，**缓存省 GET 的收益被放大**。缓存作为主线的判断不仅成立，而且被低估。

缓存收益随预算单调增长：

| 预算 | passthrough | 决策树 S2 | 缓存收益（S2 vs pt） |
| --- | --- | --- | --- |
| 128MiB | 396.7s | 256.3s | **−35.4%** |
| 256MiB | 394.4s | 234.9s | **−40.5%** |
| 1024MiB | 387.2s | 205.3s | **−47.0%** |

### 2.2 g\* ≈ RTT × BW 在真实云上站得住 ✅

两个独立估计：

| 来源 | RTT | BW | g\* |
| --- | --- | --- | --- |
| E1 裸探针（boto3 长连接，尺寸扫描） | 25.0ms | 102.3 MiB/s | **2.56 MiB** |
| 从 8 个配置回归 `wall = a·GETs + b·bytes`（R²=0.888） | 33.6ms | 50.3 MiB/s | **1.69 MiB** |

E4 实测（`forcePolicy=s3a_prefetch`，depth=0，@256MiB）：

| block | GETs | remote MiB | amp | wall-clock |
| --- | --- | --- | --- | --- |
| 256KiB | 5110 | 2839.5 | 1.15 | 247.7s |
| **1MiB** | 4176 | 5299.4 | 2.15 | **230.1s（最优）** |
| 4MiB | 3244 | 13168.5 | 5.34 | 245.4s |
| 16MiB | 2655 | 41775.7 | 16.93 | 486.6s |

最优点在 1–4 MiB 之间，两个预测都落在这个区间或紧邻处。**解析式可以作为论文的 analytical model**；正式轮需要把扫描点加密（例如 0.5 / 1 / 2 / 3 / 4 / 8 MiB）。

机制说明：`(block=X, depth=0)` 下的 `s3a_prefetch` **不是投机预取**，而是「块对齐把需求读合并成更大的单次读」——即 PROJECT3 的 **D4**。因此不用实现 D4 就已经测到了 g\*。

### 2.3 cost proxy 保序完美，可继续用来挑 oracle 表 ✅

PROJECT2 §8.6 定义：

```
cost = remoteGets × RTT + remoteBytes / BW      # 假设 RTT = 50ms, BW = 100 MiB/s
```

实测对照：**BW = 102.3 MiB/s（几乎完全命中）**；**RTT = 25.0ms（假设值高了一倍）**。两个误差方向相反，恰好抵消。

八个 @256MiB 配置的排序 Spearman 相关系数 = **+1.000**（零翻转）。绝对值系统性高估 12.8%–48.1%，且高估幅度随 GET 数递增。

**结论**：proxy 可以继续用于**挑选** oracle 表（§8.6 原本就是这个定位），**绝不能报绝对秒数**。标注口径继续只认真实 wall-clock。

---

## 3. 没有效果 / 被证伪的部分

### 3.1 决策树输给最佳静态策略 ❌

@256MiB 真实 wall-clock：

| 配置 | wall-clock | 相对最佳静态 |
| --- | --- | --- |
| `oracle_perworkload`（proxy 的 cost 表） | **211.1s** | −6.55%（天花板） |
| 最佳静态 = `s3a_prefetch` | **225.9s** | — |
| 决策树 S2 | 234.9s | **+3.98%（输）** |
| template_auto | 248.2s | +9.9% |
| `s3a_random` / `template_multimodal` / `template_locality` | 250–254s | +11–12% |

PROJECT2 §8.7 在 128MiB **cost proxy** 口径下说「决策树已吃掉 61% 的可用空间、优于**所有**静态策略」。真实 wall-clock、256MiB 上这句话不成立。

这不是「树不够深」，是标签的三层问题，只修一层没用：

| 层次 | 问题 | 出处 |
| --- | --- | --- |
| 标签**来源** | 来自离线 simulator，不是 SDK 真实执行 | PROJECT2 §8.3 |
| 标签**口径** | 用 proxy / 字节，不是真实 wall-clock | §8.7：两个口径给出方向相反的答案 |
| 标签**工作点** | 在单一预算上打的标签，不跨预算泛化 | §8.7.1：256MiB 起最佳静态策略已迁移 |

学习式相对手写规则的增益始终只有 **3.7%–5.4%**，且不随预算单调——和缓存 −35%→−47% 的杠杆完全不在一个量级。

**处置**：这棵树按 PROJECT3 §1.3 本就要降级为 baseline。这个结论的价值是给「换动作空间」提供证据，**不是给「重训」提供任务**。不要花时间重训一棵即将废弃的树。

自适应天花板（最佳静态 → 逐负载 oracle）= **6.55%**（proxy 在 256MiB 预测 9.93%，同量级偏小）。按 PROJECT2 §8.6 判定规则落在第 2 条——但正确的响应是换动作空间，不是在四条命名策略里重训。

> caveat：forced policy 四组与 oracle 都只跑了 1 次。3.98% 是 E2 噪声（0.62%）的约 6 倍，大概率真实，正式轮需重复。

### 3.2 投机预取无效，默认锁 depth = 0（不是 1）❌

必须给异步预取留重叠窗口，所以 depth 对比在 `thinkMs=5` 下做。`thinkMs` 对四列同等施加，passthrough 被抬高 53.7s（理论 55.9s），校准通过。**同一次运行内**比较：

| | S2（depth=0） | S3（depth=1） |
| --- | --- | --- |
| wall-clock | 286.6s | 282.1s（**−1.57%**） |
| GETs | 5042 | 5297（+255） |
| prefetch useful | 0 | **13.2 MiB** |
| prefetch wasted | 0 | **153.8 MiB** |
| useful 占比 | — | **7.9%** |

PROJECT2 合成实验：S3 vs S2 **+1.6%（打平略亏）**，useful ≈4%。真实云上数字几乎一模一样，只是符号翻到了略微为正。−1.57% 相对 E2 噪声 0.62% 勉强超噪声，但代价是 12 倍于 useful 的 wasted bytes。

**正确表述**：投机无效意味着**不做投机**。默认 **depth = 0**。D3 重新设计为置信度门控——仅在**已确认**的顺序段上开启，段一断立即回落到 0（PROJECT3 §3.3）。不是锁死某个正深度。

### 3.3 D5 长尾降级为次要维度 ⚠

等尺寸、绕过缓存的裸探针（固定 1MiB，n=300）：

| 尺寸 | p50 | p99 | p99/p50 |
| --- | --- | --- | --- |
| 4KiB | 25.4ms | 55.3ms | 2.18 |
| 64KiB | 25.6ms | 59.4ms | 2.32 |
| **1MiB** | **26.7ms** | **68.8ms** | **2.58** |
| 16MiB | 181.4ms | 194.6ms | 1.07 |

Java `S3AsyncClient` 复测 1MiB p99/p50 = **2.79**，同一结论。高并发下该比值往往更低（见 §5.4）。

门禁判据 >3× 不成立。离线对冲上界：P95 触发只能换来 **10.1% 的 P99 改善**、代价 5% 额外请求；P99 触发改善为 0。

**侦察轮结论：D5 降级**，把时间还给 D1/D4。但 2.58 离经验阈值 3.0 并不远；且只测了单一时段、单个对象、单一尺寸。PROJECT3 §10 要求回答「是否**稳定可复现**」，这一点还没答。正式轮补一次跨时段观测再定生死，**不是删掉这个维度**。

### 3.4 分位数不是「作用不明显」，是「不能这样用」❌

两个口径陷阱，和 `cacheHitRate` 误导性是同一类问题：

1. **开缓存后 p50 和 p99 描述两个不同的群体。** S2 的 p50 = 0.06ms（缓存命中），p99 = 128ms（穿透到网络）。拿分位数比较列与列毫无意义。
2. **回放里请求大小从 4KB 到数 MB 混在一起。** passthrough 的 p99/p50 = 5.09 **主要是尺寸混合的产物，不是 straggler**。等尺寸探针给出的才是 2.58。

**正确表述**：评价缓存 / 策略只能看 wall-clock；分位数只在等尺寸、绕过缓存的请求上才有意义（E1 那把尺子）。这条本身值得写进论文的方法学陷阱。

---

## 4. 对 PROJECT3 动作空间的含义

结合 §2.1 和 §3.1：缓存杠杆 −35%→−47%，策略路由天花板 6.55% 且现有树吃到负空间。这正是 PROJECT3 §1 换动作空间的理由，现在有了真实云证据。

| 维度 | 侦察轮结论 | 下一步 |
| --- | --- | --- |
| **D1 缓存管理** | 主杠杆，收益被低估 | 实现容量/粒度/准入/淘汰；ρ 扫描待 SF300 |
| **D2 同步转异步** | 1MiB 从 ~20 路到 ~128 路吞吐 135→1055 MiB/s（约 8×）；甜区约 16 路 | 默认 worker 并发 ~16，吞吐不再涨时回落；单线程 sync API 必须经 D2 才能吃到这条链路 |
| **D3 预取** | 投机深度无效 | 默认 depth=0；门控式顺序预取，不做预测 |
| **D4 IO 请求合并** | 单连接 g\*≈2.5MiB 成立；小读始终 RTT 受限，大读高并发后变 BW 受限 | `g* = RTT×BW` 在线自估；**饱和后把 g 往回收**，否则只多传废字节 |
| **D5 长尾** | 空载 P99/P50=2.79；高并发下比值更低（慢是全局的） | 降级；正式轮跨时段复验；抑制器「带宽已饱和」标定约 1 GiB/s |

四条命名策略（`s3a_random` / `template_locality` / `s3a_prefetch` / `template_multimodal`）冻结为 baseline，不再演进。新动作空间是**五个维度上的少量离散档位**，由预算/安全仲裁层协调——不是再训练一棵四分类树。

**PROJECT2 的 `dynamic_oracle`（四策略事后最优）随旧动作空间作废**，现在不必做。但它承担的门禁职责还在：五维实现之后，需要**逐维度的参数 oracle**（每维用事后最优参数跑，看叠加上界够不够）——PROJECT3 §10 第一条风险缓解。那是「这篇论文有没有主贡献」的门禁，排在各维实现之后。

---

## 5. E1 Java `S3AsyncClient` 并发重测

### 5.1 为什么必须重测

第一轮 E1 用 boto3，**每个请求新建一个 client**，等于每次都付 TLS 握手。同样是 1MiB：尺寸扫描（长连接）报 26.7ms，并发扫描 c=1 报 60ms。那条 15.9 → 77.4 MiB/s 的「饱和曲线」只能当下界，**不能当 D2/D5 的输入**。E2/E3/E4 走 Java SDK 单线程，不受影响。

重测改用与未来 D2 相同的栈：单一复用的 `S3AsyncClient` + `NettyNioAsyncHttpClient(maxConcurrency=128)`，预热后打 `tpch300/lineitem/` 下 32 个 part，尺寸 {64KiB, 1MiB, 8MiB}。503 / 重试全程为 **0**。代码：`S3AsyncLatencyProbe`；产物：`results/aws-s3_scout_e1_java/e1_java_summary.json`。

**读数口径**：表里的 `c` 没有做真正的 in-flight 信号量，而是一次发出 `c × perWorker` 个 future，再被 Netty 的 128 连接上限卡住。因此 `c=1` 实际约 20 路，`c=16/64` 实际 in-flight 顶在 128。下面按这个理解读，不当成单线程。甜区「约 16 路」是从吞吐曲线读出来的，正式钉 D2 默认值时应用 semaphore 精确扫 {1,2,4,8,16,32,64}。

### 5.2 尺寸扫描：与 boto3 长连接重合，g\* 被独立复现

| | boto3 长连接 | Java `S3AsyncClient` |
| --- | --- | --- |
| 有效 RTT | 25.0ms | **24.4ms** |
| 有效 BW（单连接） | 102.3 MiB/s | **103.4 MiB/s** |
| g\* = RTT × BW | 2.56 MiB | **2.52 MiB** |
| 1MiB p50 | 26.7ms | 29.1ms |
| 16MiB p50 | 181ms | 179ms |
| 1MiB p99/p50 | 2.58 | **2.79**（仍 &lt; 3） |

两套 client、两种语言，RTT/BW/g\* 差不到 2%。**g\* ≈ RTT×BW 不是拟合巧合。** 小读完全被 RTT 吃掉：4KiB 和 64KiB 的 p50 都是 ~25ms。

### 5.3 并发饱和：平台期约 1.0–1.25 GiB/s，不是 S3 限流

Java 探针聚合吞吐（实际 in-flight ≈ min(发出量, 128)）：

| 实际并发（约） | 64KiB | 1MiB | 8MiB |
| --- | --- | --- | --- |
| ~20 路 | 6.3 MiB/s | 135 MiB/s | 232 MiB/s（5 路） |
| ~80 路 | 25 MiB/s | 390 MiB/s | 810 MiB/s（20 路） |
| ~128 路 | 59 / **111** MiB/s | 923 / **1055** MiB/s | 947 / **1248** MiB/s |

要点：

- 实例网卡 25Gbps，理论上还能再往上；本轮 **503=0**，所以这个平台期是 SDK/连接池/CPU 侧的，不是 S3 SlowDown。
- **1MiB 从 ~20 路到 ~128 路：135 → 1055 MiB/s，约 8 倍。** 这就是 D2 存在的理由——单线程 sync `read()` 永远用不满这条链路。
- 甜区在 **16 路量级**：1MiB 已达 923 MiB/s，再拉到 128 只再加 ~14% 吞吐，8MiB 的 p50 却从 151ms 抬到 **1.21s**。默认 worker 并发应在此回落。
- **64KiB 即便 128 连接也只有 111 MiB/s**，离 1 GiB/s 平台差 10 倍，纯 RTT 受限。小读的杠杆是 D4 合并，不是加并发。

### 5.4 对 D2 / D4 / D5 的具体含义

**D2** —— 现在有默认值了。worker 并发默认 ~16，吞吐进入平台期后回落；单线程负载必须把窗口 `w` 收成 0（PROJECT3 §3.2 的约束被这条曲线证实）。

**D4** —— g\* 是**单连接 / 低并发**公式。小读始终适用（合并把 64KiB 拉到 ~2.5MiB 才能碰到带宽）。1MiB+ 在十几路并发后已经 BW 受限：再把请求拼得更大，省不下 RTT，只会多传废字节——这和 E4 里 16MiB block 把 wall-clock 从 230s 打到 487s 是同一件事。**饱和后必须把 g 往回收。**

**D5** —— 降级被加强，不是削弱。空载 1MiB p99/p50=2.79 仍低于 3；负载上来后比值往往**更低**（1MiB 高并发 2.03，8MiB 1.43）：大家一起变慢，p50 抬得比 p99 还狠，对冲没有对手可赢。这正是 PROJECT3 「慢是全局的」抑制条件。抑制器「带宽已饱和」可标定在 **~1 GiB/s**。唯一例外是 64KiB 中等并发时比值到 5.1——小请求、未饱和时尾部更可见，即使做 D5 也该只在低负载+小请求上开。

这条链路和回放也对得上：E2 passthrough p50≈25.5ms，就是空载 RTT；mixed_holdout 平均读 226KB，单连接传输约 2ms，端到端就是一次 RTT。缓存把 GET 从 11189 砍到 5042，wall-clock −40%，本质是少付了约 6000 次 25ms。

### 5.5 仍薄的地方

- `c` 没有真正限流，甜区「16」不是精确 in-flight 扫描。
- 高并发格子样本短（有的 wall 只有 0.2s），平台期 ±10% 可能是噪声。
- 只测了 us-east-2 同区、一种实例。跨 AZ / 跨 region RTT 会变，g\* 必须在线自估，不能写死 2.5MiB。

---

## 6. 本轮明确不做 / 留给正式轮

| 项 | 原因 |
| --- | --- |
| 重训决策树 | 树本身要废弃 |
| 锁 depth=1 | 方向反了，应锁 0 |
| PROJECT2 `dynamic_oracle` | 旧动作空间的产物 |
| SF300 回放 / ρ 全扫 | object key 与 SF1 trace 对不上，须新采；本轮只备份了 101 GiB |
| D5 实现对冲 | 降级；正式轮先跨时段观测 |
| 24h 稳定性 | 侦察轮砍掉 |
| 论文主表引用本轮绝对数 | 每配置 1–2 次，非正式 |

---

## 7. 对 PROJECT2 未决问题的逐条关闭

| PROJECT2 问题 | 答案 | 状态 |
| --- | --- | --- |
| §8.6 真实 latency 是否保序 | Spearman +1.000，保序；绝对值偏高 13–48% | **关闭** |
| §8.6 三条判定落在哪条 | 天花板 6.55% 仍在，但树吃到负空间 → 换动作空间 | **关闭（改述）** |
| §8.7.1 冻结结论 3：depth≥1 | 真实重叠也救不了，useful 7.9%，默认 depth=0 | **关闭** |
| §8.8 S4 主结果 | 缓存 −40.4%；树输给静态 3.98% | **关闭** |
| §8.4 256MiB 是否仍较优 | 缓存收益随预算单调，256 不是峰；「较优」是 ρ 的函数 | **方向确认，曲线未画完** |
| §8.3 `dynamic_oracle` | 随四策略动作空间作废；五维参数 oracle 延后 | **改述后搁置** |
| §6.1 真实带宽饱和 / D2 并发上限 | 平台期 ~1.0–1.25 GiB/s，503=0；D2 甜区约 16 路 | **关闭（Java E1）** |

---

## 8. 实验产物索引

| 产物 | 路径 |
| --- | --- |
| 侦察轮全部 CSV / report | `docs/adaptive-range-reader/results/aws-s3_scout_20260812/` |
| 汇总 CSV | `.../results-v2.all.csv` |
| E1 boto3 探针（尺寸扫描可用；并发扫描**作废**） | `docs/adaptive-range-reader/results/aws-s3_scout_e1/e1_summary.json` |
| E1 Java `S3AsyncClient` 重测（并发可用） | `docs/adaptive-range-reader/results/aws-s3_scout_e1_java/e1_java_summary.json` |
| 编排脚本 | `tools/run_scout_track1.sh` |
| 汇总脚本 | `tools/summarize_scout.py`、`tools/proxy_vs_wallclock.py` |

复现 E2 起（凭证走环境变量 `S3ARR_*`）：

```bash
export S3ARR_ENDPOINT=https://s3.us-east-2.amazonaws.com
export S3ARR_BUCKET=home-haoyue
export S3ARR_REGION=us-east-2
# S3ARR_ACCESS_KEY / S3ARR_SECRET_KEY 从 aws configure 填

mvn -q -pl services-custom/s3-adaptive-range-reader \
  -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
  -Dtest=AdaptiveReaderSystemBenchmark test \
  -Ds3arr.backend=s3 \
  -Ds3arr.trace=$PWD/traces/mixed_holdout_s3.csv \
  -Ds3arr.stripKeyPrefix=mixed_holdout \
  -Ds3arr.cacheBudgetMiB=256 -Ds3arr.prefetchBlockMiB=1 -Ds3arr.prefetchDepth=0 \
  -Ds3arr.warmup=0 -Ds3arr.iters=1 -DargLine="-Xmx12g"
```

Java 并发探针：

```bash
mvn -q -pl services-custom/s3-adaptive-range-reader \
  -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
  -Dtest=S3AsyncLatencyProbe test
```
