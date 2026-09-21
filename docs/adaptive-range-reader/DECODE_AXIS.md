# 解码轴：还有多少空间（测量与结论）

> **定位**：本文件只回答一个问题——ClickBench 上 76% 的非 I/O 墙钟里，Parquet
> codec/encoding 这一轴还能拿回多少。结论是负面的，而且是被一次受控 A/B 识别出来的，
> 不是靠模型推断。创建：2026-09-15。
> 前置：Track1 已证明 I/O 侧只剩 ~7%（见 `TRACK1.md` §6.1）。

---

## 0. 一句话结论

**解码轴的全部空间约为墙钟的 1.5%，不是 L1 模型预测的 7.8%。**

L1 的 `t_decode` 把解码算成 task CPU 的一半（1595 of ~2940 core-s）。一次把
task 数、查询集、文件与 row group 几何全部固定、只改 3 列编码的 A/B 显示：

- 按**实际读取字节**加权，被改的 3 列占解码成本权重的 **62.6%**，这次改动应当削掉
  **36.7%** 的解码工作量；
- 实测 task CPU 只降 **0.88%**（2939.9 → 2914.2 core-s）；
- 因此**解码只占 task CPU 约 2.4%**（≈70 core-s），16 核折算约 4.4s，
  占 283s 墙钟的 **1.5%**。

也就是说，即便把解码成本降到零，这条轴也只值 1.5%。`DECODE_MODELLED` 必须保持
`False`，而且不是因为"还没校准"，是因为**校准后的量级本身被否定了**。

---

## 1. 为什么会去查解码轴

Track1 的测量（`TRACK1.md` §6.1）给出：`8m2g` 配置下只有 62s/257s 的墙钟有 GET
在飞，零 GET 时间反而从 132.5s 涨到 195.4s。把 Track1 扫描四个 cell 的事件日志按
CPU / 非 CPU 等待拆开：

| cell | task run | task CPU | CPU 占比 | 非 CPU 等待 |
| --- | ---: | ---: | ---: | ---: |
| `000` | 3950.2s | 2891.6s | 73.2% | 1058.7s |
| `100` | 3656.4s | 2972.9s | 81.3% | 683.5s |
| `100-8m2g` | 3598.1s | 3000.4s | 83.4% | 597.7s |

Track1 削掉的全是最后一列，**CPU 那一列一动不动甚至微涨**。2973 core-s / 16 核
≈ 186s，占 257s 墙钟的 72%。这就是解码轴名义上的战场。

`decode_rerank.json`（已有产物）当时给出的答案很吸引人：312 个合法候选里
`same_winner: false`，解码感知的赢家比 t_io 赢家再快 **15.3s（7.77%）**：

| 候选 | t_io | t_decode | t_cost | 远端字节 |
| --- | ---: | ---: | ---: | ---: |
| 上线的 `joint-codec-encoding` | 97.0s | 99.7s | 196.7s | 18.1GiB |
| `per-column-codec` | 110.8s | 70.7s | **181.4s** | 23.5GiB |

---

## 2. 找到一个比两个赢家都好的点

`per-column-codec` 用 +30% 字节换解码，代价太大。把字节侧探针
（`layout_probe.json`）和解码侧探针（`decode_probe.json`）并排放，发现当前候选里有
一笔明确的坏交易：

| 列 | 编码 | 字节比 | 解码相对成本（pyarrow / parquet-mr） |
| --- | --- | ---: | ---: |
| `Title` | `zstd\|baseline` | 0.4641 | 0.84 / 0.97 |
| `Title` | `zstd\|DELTA_BYTE_ARRAY` | **0.4442** | **2.21 / 2.39** |
| `URL` | `zstd\|baseline` | 0.6046 | 0.84 / 0.97 |
| `URL` | `zstd\|DELTA_BYTE_ARRAY` | **0.5986** | **2.21 / 2.39** |

为了在这两列上多省 4.3% 和 1.0% 的字节，付出 2.2–2.4 倍解码 CPU——而它们是全表最大的
两个 BYTE_ARRAY 列。这笔交易之所以被选中，正因为 `DECODE_MODELLED = False` 让解码对
搜索不可见，字节侧的微小优势自动获胜。

### 2.1 否决规则（不需要汇率）

`tools/track2/decode_veto_plan.py` 实现的规则是**否决**而不是权衡：

> 一个编码只在它的实测解码成本不劣于同列 baseline 编码时保留。

这不需要字节与 CPU 之间的汇率，因此不依赖任何未识别的常数。相关的 reader 是**将要读**
候选的引擎（基准用 Spark，所以默认 `--reader parquet-mr`；`DELTA_BINARY_PACKED` 在
PyArrow 下是 1.01 近似中性，在 parquet-mr 下是 1.53）。

作用在 `joint2` 的 plan 上，93 个动作里正好否决 3 个：

```text
dropped hits.Title     DELTA_BYTE_ARRAY     cost 2.393 > baseline 0.973
dropped hits.URL       DELTA_BYTE_ARRAY     cost 2.393 > baseline 0.973
dropped hits.WatchID   DELTA_BINARY_PACKED  cost 1.534 > baseline 1.000
```

18 个 `PLAIN` 动作全部保留——它们既省字节又**更便宜**（0.62–0.86），正是规则要放行的那类。

### 2.2 写出来之后，字节侧比预测更好

`clickbench_sf1_e0_uc1_decode_veto`，22 文件 / 169 row group，列顺序与 `joint2` 完全一致，
只有那 3 列变化：

| | 压缩字节 | 未压缩字节 | 解码加权成本 |
| --- | ---: | ---: | ---: |
| `joint2` | 10.216 GiB | 74.1 GB | 76.9 G-units |
| `decode_veto` | 10.197 GiB | 80.2 GB | 60.1 G-units |
| | **−0.19%** | +8.2% | **−21.8%** |

值得单独记一笔：样本尺度的探针预测这次否决要付 **+3.3%** 字节，全表实际是 **−0.19%**。
`Title` 在全表尺度上 `PLAIN+zstd` 反而比 `DELTA_BYTE_ARRAY` 小 5.1%——**当初用来
justify 2.39 倍解码的那点字节收益，在真实规模上根本不存在**。200k 行样本不足以代表
1 亿行列的可压缩性。

所以这个候选在两个轴上同时优于 `joint2`，理论上应当是三者中最好的点。

---

## 3. A/B 实测：预测落空

协议：`run_e0_smoke.py --paired`，3 轮交替，同一套 43 条 ClickBench，`local[16]`、
driver 32g、冻结 reader、**不开 Track1**（隔离布局效应）。

| 布局 | 三轮 | median | 变化 |
| --- | --- | ---: | ---: |
| `joint2` | 284.5 / 281.4 / 283.0 | 283.03s | — |
| `decode_veto` | 279.8 / 286.4 / 278.2 | 279.80s | **−1.14%** |

远端字节 +0.50%，GET 23421 → 23334。

**−1.14% 不能算证实**：候选的散布（278.2–286.4s）比基线（281.4–284.5s）大得多，
中间那轮比基线全部三轮都慢。n=3 下这个差值与噪声不可区分。

### 3.1 事件日志给出了原因

| 布局 | task run (median) | task CPU (median) | task 数 |
| --- | ---: | ---: | ---: |
| `joint2` | 3969.6 core-s | 2939.9 core-s | 4170 |
| `decode_veto` | 3920.1 core-s | 2914.2 core-s | 4170 |
| | −1.25% | **−0.88%** | 相同 |

解码工作根本没有离开。如果解码真占 CPU 的 54%（模型口径），−36.7% 的解码量应当见到
约 −12% 的 task CPU。

---

## 4. 这次 A/B 顺带完成了缺失的识别实验

`advisor_policy.DECODE_RATE_SCALE` 的注释写明它没被识别：事件日志只报总 task CPU，
`CPU = s × decode + N` 有三个未知量、两个方程，6.03 这个数依赖「非解码 CPU 随 task 数
下降」的假设（当时 5847 → 4170）。

本次 A/B 把这个混淆变量消掉了：**两侧 task 数相同（4170 vs 4170）**，查询集、文件数、
row group 几何、列顺序全部相同，唯一差异是 3 列的编码。所以 `N` 按构造不变：

```text
ΔCPU = s × Δdecode
−25.7 core-s = s × Δdecode
```

按实际读取字节归因（用 `candidate_observations.parquet` 把 GET 的 overlap 归到
column chunk，再乘探针的相对成本）：

| 列 | 读取未压缩 GB | 占读取 | 编码 | rel | 解码权重 |
| --- | ---: | ---: | --- | ---: | ---: |
| `URL` | 39.87 | 24.2% | DELTA_BYTE_ARRAY | 2.39 | **44.1%** |
| `Title` | 15.17 | 9.2% | DELTA_BYTE_ARRAY | 2.39 | **16.8%** |
| `SearchPhrase` | 17.10 | 10.4% | PLAIN | 0.80 | 6.3% |
| `Referer` | 14.19 | 8.6% | PLAIN | 0.80 | 5.3% |
| `UserID` | 10.82 | 6.6% | PLAIN | 0.81 | 4.1% |
| `WatchID` | 2.38 | 1.4% | DELTA_BINARY_PACKED | 1.53 | 1.7% |

被否决的 3 列占解码权重 **62.6%**，`Δdecode = −36.7%`。于是

```text
解码总量 ≈ 25.7 / 0.367 ≈ 70 core-s ≈ task CPU 的 2.4%
```

对照：`decode_rerank` 给 `joint2` 的 `decode_core_s = 1595`；经 `DECODE_RATE_SCALE`
校准后的口径约 226 core-s。实测约 70 core-s。**模型高估 4–23 倍。**

诚实的替代解释只有一个：探针的 `relative_cost` 在真实扫描里不成立（parquet-mr 的
向量化读路径与探针的物化 sink 不同，探针自己的 caveat 也说 parquet-mr 的数是下界）。
但无论是哪一个，对项目的结论相同——**解码轴按当前的建模与探针方式不转化为墙钟**。

---

## 5. 叠加实验：`order_refined` × `decode_veto`（2026-09-20）

上一轮把两条从 `joint2` 分出的平行支各自测了一遍：列顺序细化 −0.89%（n=5，stable），
编码否决 −1.14%（n=3，未 stable）。它们改的维度不同，从未叠在一起。朴素相加约 2%。
这次把 `decode_veto` 规则作用在 `plan_deterministic.order_refined.json` 上（否决同一 3
个编码动作，列顺序哈希与 refined 一致），写出
`clickbench_sf1_e0_uc1_order_refined_decode_veto`，对 `joint2` 做 5 轮 paired 交替。

| 布局 | 五轮 | median | CV | vs `joint2` |
| --- | --- | ---: | ---: | ---: |
| `joint2` | 273.7 / 274.7 / 272.1 / 273.4 / 273.0 | 273.389s | 0.35% | — |
| 叠加候选 | 282.6 / 272.9 / 270.5 / 273.6 / 270.6 | 272.910s | 1.82% | **−0.18%** |

两侧都 `stable`。配对差：+8.90 / −1.83 / −1.64 / +0.19 / −2.48s，median −1.64s，
**mean +0.63s**。候选第一轮 282.6s 是离群点；去掉之后其余四轮也只有约 −0.5%。
报告自己的稳定性注释适用：差值小于候选 CV，**不是证据**。

I/O 中间指标走反：GET 23417 → 23866（+1.92%），远端字节 31.62 → 31.76 GiB（+0.47%）。
这与单独的 `order_refined`（GET 23362 → 23933）同号，与 L1 预测的 GET 下降相反。
逐查询 28 条快（合计 −3.66s）对 15 条慢（合计 +2.59s），净 −1.07s，对冲。

对照三条已测支：

| 候选 | vs `joint2` | n | GET |
| --- | ---: | ---: | ---: |
| `order_refined` | −0.89% | 5 stable | +2.4% |
| `decode_veto` | −1.14% | 3 未 stable | −0.4% |
| **叠加** | **−0.18%** | 5 stable | **+1.9%** |
| 朴素相加 | ~−2.0% | — | — |

两个已测收益**不叠加**，合在一起甚至比任一单支都小，且落在噪声里。这两条剩余杠杆
就此关掉。还没被这次实验碰到的，只剩 row-group 那条一侧 3 点网格，以及 L1 字节模型
在推荐点上 +74% 的误差（vectored merge 未定价）。

报告：`docs/adaptive-range-reader/results/track2/order_refined_decode_veto_ab/e0_report.json`

---

## 6. 结论与后续

1. **解码轴空间约 1.5% 墙钟**（70 core-s / 16 核 / 283s）。`decode_rerank` 的 7.8%
   不成立，`per-column-codec` 不值得再写一版布局去验证——它付 +30% 字节买的是一个
   只有 1.5% 上限的东西。
2. **`DECODE_MODELLED` 保持 `False`**，理由升级：不是"待校准"，是量级被否定。
3. **`decode_veto` 单独是一次未证实的 −1.14%；和 `order_refined` 叠在一起是噪声**
   （§5，−0.18%，GET/字节还略升）。默认候选仍用 `joint2`，不要换成叠加布局。
4. **样本尺度探针的字节预测需要复核**。`Title` 上预测与实际反向（+4.3% vs −5.1%），
   说明 200k 行样本对亿行列不可靠。这是个独立于解码的问题，会影响 L1 在**字节**轴
   上的排序。
5. **剩下的 CPU 不属于 Parquet 布局**。2940 core-s 里解码约 70，其余是过滤、聚合、
   shuffle、序列化和调度。30% 的联合目标在 Track1（I/O，~7%）+ Track2 解码轴（~1.5%）
   这两条路上都拿不到。

---

## 7. 资产

| 文件 | 内容 |
| --- | --- |
| `tools/track2/decode_veto_plan.py` | 否决规则；从 plan + 两个探针派生候选 plan |
| `tools/track2/tests/test_decode_veto_plan.py` | 规则单测（5 条） |
| `results/track2/e0_smoke_uc1/plan_deterministic.decode_veto.json` | 派生出的 plan |
| `results/track2/decode_veto/layout_manifest.json` | 写入 manifest |
| `results/track2/decode_veto/footer.parquet` | 新布局 footer（校验用） |
| `results/track2/decode_veto_ab/` | 3 轮交替 A/B 与事件日志 |
| `results/track2/e0_smoke_uc1/plan_deterministic.order_refined.decode_veto.json` | 叠加 plan |
| `results/track2/order_refined_decode_veto/` | 叠加布局 manifest |
| `results/track2/order_refined_decode_veto_ab/` | 5 轮 paired A/B（vs `joint2`） |

复跑：

```bash
E=docs/adaptive-range-reader/results/track2/e0_smoke_uc1
python3 tools/track2/decode_veto_plan.py \
    --plan $E/plan_deterministic.json \
    --decode-probe $E/decode_probe.json \
    --layout-probe $E/layout_probe.json \
    --out $E/plan_deterministic.decode_veto.json

python3 tools/track2/write_layout_pyarrow.py \
    --source s3a://home-haoyue/track2/clickbench_sf1 \
    --out s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_decode_veto \
    --plan $E/plan_deterministic.decode_veto.json --tables hits --verify

python3 tools/track2/run_e0_smoke.py --paired --runs 3 \
    --source s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_joint2 \
    --candidate-out s3a://home-haoyue/track2/clickbench_sf1_e0_uc1_decode_veto \
    --queries-file tools/track2/clickbench_queries.json \
    --sysconst docs/adaptive-range-reader/results/track2/e5_whatif/sysconst.json \
    --regime same_region_m5d --use-case uc1 \
    --skip footer correlate snapshot profile plan rewrite candidate-io \
    --out docs/adaptive-range-reader/results/track2/decode_veto_ab
```
