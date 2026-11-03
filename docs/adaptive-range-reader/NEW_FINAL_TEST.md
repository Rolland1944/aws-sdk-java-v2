# 新数据集实验存档

后续实验结果追加在本文档末尾，保留已有章节。每节记录一次完整比较：数据、协议、五轮原始数、中位数和配对差。排名只看同一轮网格里的墙钟中位数。GET、远端字节和堆用来解释结果。

比较协议与原先的 2×2 相同。四格为原始布局、原始布局加 Track1、joint 布局、joint 布局加 Track1。每格 5 轮，轮内顺序轮换，每轮单独起一个 Spark 进程。Track1 为 4 GiB fixed-capacity，doorkeeper 关，不采集 profile。正式比较要求 n=5 且 CV < 5%。CV 是样本标准差除以均值。GET 用报告里的 `io.gets`。峰值堆是 `track1.peak_heap_bytes` 的中位数。

---

## 1. NYC Yellow Taxi，COS，2026-10-19

完成时间 2026-10-19 21:48 +08:00。20 格全部成功，没有查询错误。四格 CV 都低于 5%。

### 1.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | NYC TLC Yellow Taxi 2009–2025 月度 Parquet，收成一张表 |
| 行数 | 1,826,960,642 |
| 原始布局 | 204 文件，14,973 个行组，39.27 GiB |
| 查询 | 4 个 epoch，同一组训练列各扫一遍 |
| 机器 | 腾讯云 CVM，Spark `local[16]`，driver 32g |
| 存储 | COS `haoyueli-cos-1257943044`，`ap-guangzhou`，虚拟主机 |
| 原始前缀 | `s3://haoyueli-cos-1257943044/nyc_yellow/` |
| joint 前缀 | `s3://haoyueli-cos-1257943044/nyc_yellow_joint2/nyc_yellow/` |
| sysconst | `measured_cross_cloud`：RTT 97.1 ms，带宽 134.2 MiB/s，K_busy 21.13，K_data 20.74，K_metadata 1.65 |
| 查询文件 | `tools/track2/nyc_yellow_epoch_queries.json` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/cvm_nyc_yellow/` |

规划用这次 00 的 IO 轨迹，`S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。L1 选择 `order=baseline`、`compression=joint-codec-encoding`、`page=baseline`、`file=128f`、`rg=2rg`。虚拟候选的 `t_io` 是 182.7 s。24 列改为 zstd，编码包含 `BYTE_STREAM_SPLIT`、`DELTA_LENGTH_BYTE_ARRAY` 和 `PLAIN`。页大小轴没有测出差别。

几何门没有通过。行组数对齐（预测 408，写出 409）。文件数预测 82、写出 109；压缩后体积预测 31.5 GiB、写出 38.39 GiB（41,223,818,584 字节）。网格测量的是写出的这 109 个文件。182.7 s 是虚拟候选的模型数，不是这份写出布局的预测。

### 1.2 墙钟

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 632.041 / 631.307 / 626.068 / 656.521 / 683.457 | 632.04 | 3.73% | 121,730 | 104.1 GiB | 2.10 GiB |
| 10 原始 + Track1 | 550.276 / 554.185 / 534.214 / 585.144 / 577.473 | 554.18 | 3.71% | 108,701 | 92.1 GiB | 12.04 GiB |
| 01 joint | 204.628 / 207.731 / 206.392 / 207.417 / 207.329 | 207.33 | 0.61% | 9,266 | 109.3 GiB | 2.60 GiB |
| 11 joint + Track1 | 185.388 / 190.049 / 191.710 / 192.636 / 191.689 | 191.69 | 1.52% | 5,357 | 100.4 GiB | 16.78 GiB |

10 的 GET 范围是 108,617–108,731，11 是 5,338–5,360，表中取中位数。远端字节各轮相同。

按两位小数中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −77.86 s | −12.3% |
| Track1，加在 joint 上（11−01） | −15.64 s | −7.5% |
| Track2，没有 Track1（01−00） | −424.71 s | −67.2% |
| Track2，有 Track1（11−10） | −362.49 s | −65.4% |
| 两项一起（11−00） | −440.35 s | −69.7% |

分开两项的和是 −502.57 s，一起跑下来是 −440.35 s。

按轮配对，五项差值符号都一致：

| 轮 | 00→10 | 01→11 | 00→01 | 10→11 |
| --- | ---: | ---: | ---: | ---: |
| 1 | −81.8 s | −19.2 s | −427.4 s | −364.9 s |
| 2 | −77.1 s | −17.7 s | −423.6 s | −364.1 s |
| 3 | −91.9 s | −14.7 s | −419.7 s | −342.5 s |
| 4 | −71.4 s | −14.8 s | −449.1 s | −392.5 s |
| 5 | −106.0 s | −15.6 s | −476.1 s | −385.8 s |

### 1.3 读法

时间降下来，读的字节没有降。joint 一轮读 109.3 GiB，原始布局读 104.1 GiB。少掉的是请求：121,730 次 GET 变成 9,266 次。原始表的行组大约 3.3 MiB，四个 epoch 反复打开；写出的布局是 409 个行组、109 个文件。COS 上单次请求大约 97 ms，请求数决定墙钟。

Track1 在 joint 上再消掉大约 3,900 次远程 GET。4 GiB 缓存装不下一个大约 27 GiB 的 epoch，所以墙钟只再少 7.5%。峰值堆从原始布局的 2.10 GiB 升到开 Track1 后的 12.04 GiB；joint 上开 Track1 是 16.78 GiB。

---

## 2. TPC-H SF350，COS，2026-10-24

完成时间 2026-10-24 13:36 +08:00。20 格全部成功，没有查询错误。四格 CV 都低于 5%。Driver 是 48g。此前 32g 的第 1 轮 10 在 Q4 上耗尽执行内存，那次结果在 `grid-32g/`，不计入本表。

### 2.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | TPC-H SF350，八张表，Snappy |
| 原始布局 | 2,130 个文件，117.48 GiB |
| joint 布局 | 514 个文件，85.55 GiB（91,839,624,057 字节） |
| 查询 | DuckDB 导出的 22 条，Q11 比例 `0.0001000000` |
| 机器 | 腾讯云 CVM，Spark `local[16]`，driver 48g |
| 存储 | COS `haoyueli-cos-1257943044`，`ap-guangzhou`，虚拟主机 |
| 原始前缀 | `s3://haoyueli-cos-1257943044/tpch_sf350/` |
| joint 前缀 | `s3://haoyueli-cos-1257943044/tpch_sf350_joint2/` |
| sysconst | `measured_cross_cloud`：RTT 95.6 ms，带宽 58.1 MiB/s，K_busy 17.86，K_data 16.85，K_metadata 3.11 |
| 查询文件 | `docs/adaptive-range-reader/results/track2/cvm_tpch_sf350/_tpch_queries.json` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/cvm_tpch_sf350/grid/` |

规划用 2026-10-21 那次 00 的 IO 轨迹，`S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。大表的选择是：lineitem `baseline / global-zstd / 32f / 4rg`，orders `seriation / global-zstd / 32f / 2rg`，partsupp `seriation / joint-codec-encoding / 64f / 1rg`。customer、part 也是 seriation 加 global-zstd。lineitem、orders、partsupp、customer、part 的几何门通过，行数八张表都对齐。nation、region、supplier 三张小表的文件数或体积没对上预测。网格测量的是写出的 514 个文件。

### 2.2 墙钟

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 10708.954 / 10899.270 / 11132.236 / 10687.759 / 10610.664 | 10708.95 | 1.94% | 1,345,049 | 568.4 GiB | 22.00 GiB |
| 10 原始 + Track1 | 10364.959 / 10283.233 / 10486.184 / 10299.219 / 10427.936 | 10364.96 | 0.83% | 1,179,632 | 543.4 GiB | 46.52 GiB |
| 01 joint | 5742.899 / 5641.157 / 6010.961 / 5577.762 / 5746.891 | 5742.90 | 2.88% | 301,929 | 390.0 GiB | 32.96 GiB |
| 11 joint + Track1 | 5182.011 / 5206.426 / 5108.932 / 5115.207 / 5042.149 | 5115.21 | 1.27% | 123,517 | 361.2 GiB | 42.90 GiB |

10 的 GET 范围是 1,174,120–1,193,753，11 是 121,605–123,811，表中取中位数。00 和 01 的 GET 与远端字节各轮相同。

按两位小数中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −343.99 s | −3.2% |
| Track1，加在 joint 上（11−01） | −627.69 s | −10.9% |
| Track2，没有 Track1（01−00） | −4966.05 s | −46.4% |
| Track2，有 Track1（11−10） | −5249.75 s | −50.6% |
| 两项一起（11−00） | −5593.74 s | −52.2% |

分开两项的和是 −5310.04 s，一起跑下来是 −5593.74 s。

按轮配对，五项差值符号都一致：

| 轮 | 00→10 | 01→11 | 00→01 | 10→11 |
| --- | ---: | ---: | ---: | ---: |
| 1 | −344.0 s | −560.9 s | −4966.1 s | −5182.9 s |
| 2 | −616.0 s | −434.7 s | −5258.1 s | −5076.8 s |
| 3 | −646.1 s | −902.0 s | −5121.3 s | −5377.3 s |
| 4 | −388.5 s | −462.6 s | −5110.0 s | −5184.0 s |
| 5 | −182.7 s | −704.7 s | −4863.8 s | −5385.8 s |

### 2.3 读法

这次字节和请求一起降。原始布局一轮远端 568.4 GiB、1,345,049 次 GET；joint 是 390.0 GiB、301,929 次 GET。存放体积从 117.48 GiB 降到 85.55 GiB，22 条查询又少扫了一部分列，所以远端字节降得比存放体积更多。墙钟从 3.0 小时降到 1.6 小时。

省下的时间集中在扫描和过滤重的查询。Q6、Q12、Q14、Q15、Q19 的中位数各少 250–625 秒。Q8、Q9 几乎不动，Q8 的 joint 中位数还高 31 秒。这两条是大连接，行组变少抵不过 Shuffle。

Track1 的 4 GiB 缓存相对 568 GiB 的原始读很小，所以原始布局上只少 3.2% 墙钟，GET 从 135 万降到大约 118 万。joint 上剩余请求更小，同样的缓存吃掉大约 18 万次 GET，墙钟再少 10.9%。开 Track1 的峰值堆中位数是 46.52 GiB 和 42.90 GiB，joint 不开 Track1 也有 32.96 GiB。32g driver 装不下。

### 2.4 为什么 Track1 在 joint 上更有效

这次是正协同，不是 NYC Yellow 中观察到的替代关系。Track1 的缓存容量没有变化，变化的是缓存块的重用距离和请求组成：

| 指标 | 原始布局 + Track1 | joint + Track1 |
| --- | ---: | ---: |
| 无 Track1 时的逻辑 GET | 1,345,049 | 301,929 |
| cache hit 中位数 | 165,417 | 178,412 |
| 命中率 | 12.3% | 59.1% |
| cache useful bytes 中位数 | 25.1 GiB | 28.9 GiB |
| admitted blocks 中位数 | 38,485 | 16,505 |
| Track1 墙钟收益 | −343.99 s | −627.69 s |

joint 的总请求少了约 78%，cache hit 的绝对数反而更多。它只接纳原始布局约 43% 数量的 block，却产生更多命中和 useful bytes，说明每个保留块被复用得更多，缓存淘汰抖动更小。

原因有三点：

1. **重用距离缩短。** 原始布局有 2,130 个文件和 25,930 个行组；joint 只有 514 个文件，行组也大幅减少。原始布局中，下一条查询再次访问一个块之前，往往已经读过远超 4 GiB 的其他块，旧块已经被淘汰。合并行组和压缩缩小了相邻查询之间的有效工作集，使重复访问更容易落在 4 GiB 缓存窗口内。
2. **列重排提高块重叠。** orders、partsupp、customer、part 使用 seriation，把经常共同读取的列放得更近。Track1 按 1 MiB block 缓存，多个查询的 ranged read 更容易覆盖同一批块。Zstd 也让每个缓存块承载更多有效数据。
3. **TPC-H 有跨查询复用。** 22 条不同查询反复读取相同维表、连接键和过滤列。Q3、Q7、Q12、Q14、Q15、Q20 在 joint 上加 Track1 后各再少约 44–78 秒。NYC 的四个 epoch 是大范围顺序扫描，一个 epoch 约 27 GiB，4 GiB 缓存装不下，扫描回来时旧块通常已经淘汰，因此 Track2 会吃掉更多 Track1 的收益空间。

百分比还受分母影响：joint 已把总时间减半，相同的绝对节省会表现为更高比例。但这次绝对收益也从原始布局上的 343.99 秒增加到 joint 上的 627.69 秒，所以不只是分母效应。

若把 Track1 在原始布局上的收益和 Track2 在无 Track1 时的收益简单相加，预期总收益是 5,310.04 秒；实际联合收益是 5,593.74 秒，多省 283.70 秒。这是本数据集上正交互的量化结果。该结论不推翻 NYC 的结果，而是说明缓存与布局的关系取决于重写后工作集是否进入缓存可复用窗口。

---

## 3. Criteo CTR，COS，2026-10-29

完成时间 2026-10-29 04:57 +08:00。20 格全部成功，没有查询错误。四格 CV 都低于 5%。Driver 是 48g。日志里每格的 `gate FAIL / pending` 只是 n=1，不是查询失败。

### 3.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | Criteo 点击日志，4 个 `day=` 分区，Snappy |
| 行数 | 747,844,676 |
| 原始布局 | 973 个文件，973 个行组，43.60 GiB（46,821,457,841 字节） |
| joint 布局 | 32 个文件，973 个行组，42.02 GiB（45,117,881,452 字节），仍按 `day=` 分目录 |
| 查询 | `Criteo-CTR-FE-v1`，24 条。Q20–Q22 是同一条训练扫描重复三次 |
| 机器 | 腾讯云 CVM，Spark `local[16]`，driver 48g |
| 存储 | COS `haoyueli-cos-1257943044`，`ap-guangzhou`，虚拟主机 |
| 原始前缀 | `s3://haoyueli-cos-1257943044/criteo_clicklogs/criteo/` |
| joint 前缀 | `s3://haoyueli-cos-1257943044/criteo_clicklogs_joint2/criteo/` |
| sysconst | `measured_cross_cloud`：RTT 85.8 ms，带宽 626.0 MiB/s，K_busy 8.60，K_data 3.54，K_metadata 6.42 |
| 查询文件 | `tools/track2/criteo_queries.json`，SHA-256 `415179647688865cfc7d5401d9daa3ff98d02ff173365aeffcdbd67a2085196e` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/cvm_criteo/grid/` |

规划用 2026-10-28 21:39 那次 00 的 IO 轨迹（查询时间和 1332.767 s，GET 81,492）。`S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。L1 选择 `order=seriation`、`compression=joint-codec-encoding`、`page=baseline`、`file=32f`、`rg=baseline`。40 列改为 zstd；4 列写了显式编码（3 列 `DELTA_LENGTH_BYTE_ARRAY`，1 列 `PLAIN`）。页大小轴没有测出差别。虚拟候选的 `t_io` 是 436.9 s。

几何门没有通过。行组数对齐（973），行数对齐。文件数预测 24、写出 32；压缩后体积预测 32.04 GiB、写出 42.02 GiB。文件变多，是因为目标文件约 1.36 GiB，而写出的体积比预测大约 31%，再加上一个文件不能跨 `day=`。网格测量的是写出的这 32 个文件。436.9 s 是虚拟候选的模型数。

字节覆盖率 100%。81,488 次 ranged GET 里有 42,962 次是 footer 一类的格式元数据，所以请求覆盖率是 47.28%。数据字节都对上了。

### 3.2 墙钟

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 1332.459 / 1334.559 / 1357.482 / 1376.253 / 1415.014 | 1357.48 | 2.50% | 81,492 | 196.52 GiB | 7.64 GiB |
| 10 原始 + Track1 | 1060.619 / 1060.879 / 1074.509 / 1141.234 / 1132.717 | 1074.51 | 3.63% | 20,518 | 191.04 GiB | 39.73 GiB |
| 01 joint | 1054.054 / 1056.892 / 1067.213 / 1092.825 / 1156.711 | 1067.21 | 3.93% | 44,242 | 187.87 GiB | 10.45 GiB |
| 11 joint + Track1 | 913.029 / 943.009 / 982.456 / 947.797 / 958.122 | 947.80 | 2.65% | 15,510 | 180.55 GiB | 20.64 GiB |

10 的 GET 范围是 19,120–20,771，11 是 15,494–15,544，表中取中位数。00 和 01 的 GET 与远端字节各轮相同。00 的峰值堆跨度是 5.56–26.97 GiB，01 是 4.53–14.99 GiB；开 Track1 的两格堆在 4 GiB 缓存上，分别稳定在 39.5–40.4 GiB 和 19.4–23.9 GiB。

按两位小数中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −282.97 s | −20.8% |
| Track1，加在 joint 上（11−01） | −119.41 s | −11.2% |
| Track2，没有 Track1（01−00） | −290.27 s | −21.4% |
| Track2，有 Track1（11−10） | −126.71 s | −11.8% |
| 两项一起（11−00） | −409.68 s | −30.2% |

分开两项的和是 −573.24 s，一起跑下来是 −409.68 s。重叠了 163.56 s。

按轮配对，五项差值符号都一致：

| 轮 | 00→10 | 01→11 | 00→01 | 10→11 |
| --- | ---: | ---: | ---: | ---: |
| 1 | −271.8 s | −141.0 s | −278.4 s | −147.6 s |
| 2 | −273.7 s | −113.9 s | −277.7 s | −117.9 s |
| 3 | −283.0 s | −84.8 s | −290.3 s | −92.1 s |
| 4 | −235.0 s | −145.0 s | −283.4 s | −193.4 s |
| 5 | −282.3 s | −198.6 s | −258.3 s | −174.6 s |

### 3.3 读法

这次墙钟降的是请求次数，存放体积几乎没变。原始布局一轮远端 196.52 GiB、81,492 次 GET；joint 是 187.87 GiB、44,242 次 GET。存放体积从 43.60 GiB 降到 42.02 GiB。类别列本身已经是短哈希，zstd 相对 Snappy 挤不出多少字节。文件从 973 个收到 32 个，seriation 把一起读的列排到相邻位置，小 GET 被并成更大的读。带宽拟合到 626 MiB/s，是因为轨迹里有 6,918 次不小于 8 MiB 的 GET（中位 34 MiB）。196 GiB 的传输大约 5 分钟，22.6 分钟的墙钟里剩下的是 RTT 和字符串解码。

省下的时间摊在重复扫描上。Q6 的 joint 中位数少 27 秒，Q8 少 28 秒，Q20–Q22 三个训练 epoch 各少大约 22 秒。Q8 和 Q19 在 joint 上仍是最慢的两条（122 秒和 83 秒）：它们对 26 个类别列做全表聚合，解码本身占掉大部分时间。

### 3.4 为什么 Track1 在原始布局上更有效

这次和 NYC Yellow 同一类：布局吃掉了一部分缓存本来能省的请求。和 TPC-H SF350 的正协同相反。

| 指标 | 原始布局 + Track1 | joint + Track1 |
| --- | ---: | ---: |
| 无 Track1 时的逻辑 GET | 81,492 | 44,242 |
| cache hit 中位数 | 60,974 | 28,732 |
| 命中率 | 74.8% | 64.9% |
| cache useful bytes 中位数 | 5.48 GiB | 7.33 GiB |
| admitted blocks 中位数 | 11,724 | 12,231 |
| Track1 墙钟收益 | −282.97 s | −119.41 s |

每一轮里，远程 GET 加 cache hit 都等于不开 Track1 时的逻辑 GET。原始布局上命中率 74.8%，是因为 24 条查询反复碰同一批列：Q1 先扫过 `label`，后面的质量检查、分桶和 hash 又读它，Q20–Q22 再把训练日扫三遍。这些热列的工作集放得进 4 GiB，所以下一条查询回来时块还在。

joint 把逻辑 GET 从 81,492 降到 44,242，砍掉的正是这些又小又重复的读。缓存还能命中剩下的 65%，useful bytes 也从 5.48 GiB 升到 7.33 GiB，因为活下来的读更大。墙钟跟的是 RTT 次数。原始布局上缓存消掉约 6.1 万次远程 GET，joint 上只再消掉约 2.9 万次，所以绝对收益从 283 秒降到 119 秒。

Q20–Q22 把这一点摊开了。原始布局上 Track1 给每个 epoch 再省 7–9 秒；joint 上这三个中位数几乎不动（97.4→97.1、96.8→97.0、99.0→96.2 秒）。一个训练 epoch 要解码两天的整数列和 26 个类别列，工作集远大于 4 GiB，扫回来时缓存已经换过。joint 上 Track1 还在省时间的是窄扫描：Q6、Q15、Q16、Q2、Q3 各再少 10–13 秒。

若把两项分开的收益相加，预期总收益是 573.24 秒；实际联合收益是 409.68 秒，少省 163.56 秒。这是替代关系的量化结果。Criteo 的重复列扫描让 4 GiB 缓存在原始布局上已经很有效；重写主要是把同一批小请求合并掉，而不是像 TPC-H 那样把一个装不进缓存的工作集收进窗口。

---

## 4. NovoMCP Compound Screen，COS，2026-10-30

完成时间 2026-10-30 13:14 +08:00。20 格全部成功，没有查询错误，四格 CV 都低于 5%。Driver 是 48g。

### 4.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | NovoMCP Open Corpus Lite compounds，化合物属性、ADMET、毒性与 PAINS 字段 |
| 行数 | 122,454,458 |
| 原始布局 | 21 个文件，132 个行组，34.27 GiB（36,797,876,083 字节） |
| joint 布局 | 20 个文件，132 个行组，29.07 GiB（31,212,516,338 字节） |
| 查询 | `NovoMCP-Compound-Screen-v1`，20 条；Q18–Q20 是同一条 candidate-scoring input 扫描重复三次 |
| 机器 | 腾讯云 CVM，Spark `local[16]`，driver 48g |
| 存储 | COS `haoyueli-cos-1257943044`，`ap-guangzhou`，虚拟主机 |
| 原始前缀 | `s3://haoyueli-cos-1257943044/novomcp_open_corpus_lite/compounds/` |
| joint 前缀 | `s3://haoyueli-cos-1257943044/novomcp_open_corpus_lite_joint2/compounds/` |
| sysconst | `measured_cross_cloud`：RTT 92.1 ms，带宽 345.9 MiB/s，K_busy 10.51，K_data 7.17，K_metadata 4.26 |
| 查询文件 | `tools/track2/novomcp_queries.json`，SHA-256 `a2b4bfc30dcffb9d518bd0b2e84cfae4e9df1fda6fb3a7a3176242dce548c1dc` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/cvm_novomcp/grid/` |

规划用一次完整、无错误的原始布局预检轨迹（查询时间和 259.094 s，GET 21,724）。`S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。`hba` 与 `hbd` 在一个 2,353 行小文件中物理类型是 `INT64`，其余文件是 `double`；该文件在本机备份后重写为 `double` 并更新 COS，避免 Spark 的 Parquet 类型不匹配。

第一次候选含 `BYTE_STREAM_SPLIT`，当前 Spark vectorized reader 不能读，joint 预检在 Q2 失败；这份候选没有进入网格。L0 随后把该编码列为不可读，重规划得到有效候选：`order=seriation`、`compression=global-zstd`、`page=baseline`、`file=26f`、`rg=baseline`。其虚拟 `t_io` 是 214.8 s。

有效候选的几何门通过：文件数预测 22、写出 20；行组都是 132；压缩后体积预测 29.96 GiB、写出 29.07 GiB。原始和写出布局的 scan task 都是 132，因此这次不把收益归因于更少的 Spark scan task。轨迹字节覆盖率是 100%。

### 4.2 墙钟

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 274.152 / 255.886 / 257.858 / 255.504 / 257.356 | 257.36 | 3.03% | 21,724 | 83.08 GiB | 9.82 GiB |
| 10 原始 + Track1 | 183.787 / 175.887 / 169.009 / 168.036 / 171.047 | 171.05 | 3.73% | 4,990 | 72.10 GiB | 18.92 GiB |
| 01 joint | 219.736 / 220.661 / 224.028 / 226.637 / 220.326 | 220.66 | 1.33% | 16,829 | 68.60 GiB | 5.98 GiB |
| 11 joint + Track1 | 158.047 / 156.339 / 161.381 / 158.705 / 155.941 | 158.05 | 1.38% | 3,842 | 58.18 GiB | 20.54 GiB |

10 的 GET 范围是 4,972–4,999，11 是 3,828–3,854，表中取中位数。00 和 01 的 GET 与远端字节各轮相同。

按两位小数中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −86.31 s | −33.5% |
| Track1，加在 joint 上（11−01） | −62.61 s | −28.4% |
| Track2，没有 Track1（01−00） | −36.70 s | −14.3% |
| Track2，有 Track1（11−10） | −13.00 s | −7.6% |
| 两项一起（11−00） | −99.31 s | −38.6% |

分开两项的和是 −123.00 s，一起跑下来是 −99.31 s，重叠 23.70 s。

按轮配对，五项差值符号都一致：

| 轮 | 00→10 | 01→11 | 00→01 | 10→11 |
| --- | ---: | ---: | ---: | ---: |
| 1 | −90.4 s | −61.7 s | −54.4 s | −25.7 s |
| 2 | −80.0 s | −64.3 s | −35.2 s | −19.5 s |
| 3 | −88.8 s | −62.6 s | −33.8 s | −7.6 s |
| 4 | −87.5 s | −67.9 s | −28.9 s | −9.3 s |
| 5 | −86.3 s | −64.4 s | −37.0 s | −15.1 s |

### 4.3 读法

joint 的存放体积少 15.2%，远端字节少 17.4%，GET 少 22.5%，墙钟少 14.3%。文件只从 21 变成 20，行组和 scan task 都没变，所以这次布局收益来自 zstd、列序和更短的 column-chunk 读，不是并行度变化。

Track1 是更大的单项收益。原始布局上，它把远程 GET 从 21,724 降到 4,990（−77.0%），墙钟少 86.31 s。joint 上远程 GET 从 16,829 降到 3,842（−77.2%），墙钟少 62.61 s。Q2、Q4、Q7、Q12、Q15、Q16 在原始布局上加 Track1 各少约 4.7–8.3 s；Q15 是候选筛选查询，降幅最大。joint 上 Q2、Q7、Q14、Q15 仍各再少约 4.1–7.3 s。

### 4.4 缓存与布局的交互

| 指标 | 原始布局 + Track1 | joint + Track1 |
| --- | ---: | ---: |
| 无 Track1 时的逻辑 GET | 21,724 | 16,829 |
| cache hit 中位数 | 16,734 | 12,987 |
| 命中率 | 77.0% | 77.2% |
| cache useful bytes 中位数 | 10.98 GiB | 10.42 GiB |
| admitted blocks 中位数 | 6,698 | 6,111 |
| Track1 墙钟收益 | −86.31 s | −62.61 s |

这不是缓存效率下降：两个布局的命中率基本相同，useful bytes 也接近。joint 先消掉约 4,895 个逻辑 GET，留下可由缓存消掉的请求总数更少，因此 Track1 的绝对收益从 86 秒缩到 63 秒。也就是说，这是由重叠请求集合变小造成的替代关系。

联合收益仍然显著：11 相比 00 少 99.31 s、远程 GET 少 82.3%、远端字节少 30.0%。但把原始布局上的 Track1 收益与不带 Track1 的 Track2 收益相加，会高估 23.70 s；该差值量化了两者在重复 column-chunk 读取上的重叠。

---

## 5. Criteo CTR，AWS S3，2026-10-30

完成时间 2026-10-30 02:39 +08:00（2026-10-29 18:39 UTC）。20 格全部成功，没有查询错误、OOM 或 scratch ENOSPC。四格 CV 都低于 5%。Driver 是 48g；`spark.cleaner.periodicGC.interval=30s`，scratch 使用独立 NVMe。

本节是 §3 COS Criteo 的 AWS 复现。第一次 AWS 测试使用旧 scan-task floor，planner 只允许列重排，joint 没有墙钟收益；该结果不进入本节主表。这里使用修复后的 planner、`S3ARR_SCAN_TASK_FLOOR=0` 和独立的 `joint2_v2` 前缀，从零重写并重跑四格五轮。

### 5.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | Criteo 点击日志，4 个 `day=` 分区；原始文件 Snappy |
| 行数 | 747,844,676 |
| 原始布局 | 973 文件、973 行组、43.60 GiB（46,821,457,841 字节） |
| joint v2 布局 | 32 文件、973 行组、42.02 GiB（45,117,882,165 字节），仍按 `day=` 分目录 |
| 查询 | `Criteo-CTR-FE-v1`，24 条；Q20–Q22 是相同训练输入扫描的 3 个 epoch |
| 机器 | AWS EC2，Spark `local[16]`，driver 48g |
| 存储 | AWS S3，`us-east-2` |
| 原始前缀 | `s3://home-haoyue/track2/criteo_clicklogs/criteo/` |
| joint 前缀 | `s3://home-haoyue/track2/criteo_clicklogs_joint2_v2/criteo/` |
| sysconst | `measured_cross_cloud`：RTT 79.4 ms，带宽 8116.0 MiB/s，K_busy 6.22，K_data 3.20，K_metadata 4.62 |
| 查询文件 | `tools/track2/criteo_queries.json`，SHA-256 `415179647688865cfc7d5401d9daa3ff98d02ff173365aeffcdbd67a2085196e` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/criteo_2x2_n5_48g_gc30s_v2/` |

规划输入是 AWS 本次 00 的 `dataset_snapshot.json`、`access_profile.json`、`sysconst.json` 和 `layout_probe.json`，不再有 `--dataset` 名称开关。规划设置 `S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。L1 选择 `order=seriation`、`compression=joint-codec-encoding`、`page=baseline`、`file=32f`、`rg=baseline`。40 列改为 zstd；3 个类别列用 `DELTA_LENGTH_BYTE_ARRAY`，1 个整数列用 `PLAIN`；显式 encoding 使 `use_dictionary=[]`。虚拟候选预计 37,198 次 GET、108.19 GiB、`t_io=551.4 s`。

模型几何门没有通过，且失败结果保留在 `geometry_gate.json`：预测 24 文件、34.56 GB，实际 32 文件、45.12 GB；行数和 973 个行组对齐。实际 writer 关闭全局 dictionary 后，压缩率与 probe 预测不一致。这是 advisor 的几何预测问题，不能把虚拟候选的 551.4 s 当成写出布局的预测。

实际写出通过全量 footer 检查：32/32 文件、973/973 行组，ZSTD、4 个显式 encoding、page index 全部落地；行数和四个分区逐日行数一致。AWS 写出布局与提供的 COS manifest 物理等价：文件数、行组数、row-group rows、codec、encoding map、dictionary 模式相同，RG 中位大小 259.33 MB（COS 259.39 MB）。S3 上传后，四个分区对象数和字节数均与本地逐项相等。Q1 返回 1 行，Q2 按 DATE 类型的 `day` 分组返回 4 行。

### 5.2 墙钟与 IO

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 1706.7 / 1774.4 / 1794.2 / 1761.1 / 1815.5 | 1774.4 | 2.08% | 81,492 | 196.51 GiB | 11.97 GiB |
| 10 原始 + Track1 | 1521.1 / 1531.7 / 1579.3 / 1574.0 / 1541.4 | 1541.4 | 1.49% | 20,513 | 190.95 GiB | 25.44 GiB |
| 01 joint v2 | 1421.9 / 1449.8 / 1425.1 / 1457.8 / 1436.8 | 1436.8 | 0.96% | 44,242 | 187.87 GiB | 4.78 GiB |
| 11 joint v2 + Track1 | 1323.8 / 1316.0 / 1338.2 / 1339.7 / 1386.8 | 1338.2 | 1.83% | 15,357 | 180.54 GiB | 15.12 GiB |

00 和 01 的 GET/远端字节五轮逐轮相同；10 的 GET 中位数是 20,513，11 是 15,357。所有比较的五轮配对方向都一致：joint 5/5 更快，Track1 在两个布局上均 5/5 更快，组合也 5/5 更快。

按墙钟中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −233.0 s | −13.1% |
| Track1，加在 joint 上（11−01） | −98.5 s | −6.9% |
| Track2，没有 Track1（01−00） | −337.6 s | −19.0% |
| Track2，有 Track1（11−10） | −203.2 s | −13.2% |
| 两项一起（11−00） | −436.2 s | −24.6%（1.326×） |

分开两项的和是 −570.6 s，一起跑下来是 −436.2 s，重叠约 134.4 s。布局减少了缓存原本能命中的一部分小请求，因此两项是替代关系；这和 §3 COS Criteo 的结论一致。

按轮配对：

| 轮 | 00→10 | 01→11 | 00→01 | 10→11 |
| --- | ---: | ---: | ---: | ---: |
| 1 | −185.5 s | −98.1 s | −284.7 s | −197.3 s |
| 2 | −242.7 s | −133.8 s | −324.6 s | −215.7 s |
| 3 | −214.9 s | −86.8 s | −369.1 s | −241.1 s |
| 4 | −187.1 s | −118.2 s | −303.3 s | −234.3 s |
| 5 | −274.1 s | −50.0 s | −378.7 s | −154.6 s |

### 5.3 堆与 GC

`peak_heap_bytes` 是 Track1 probe 在一个 Spark 进程内观测到的最大 JVM heap used，不是 Xmx；四格 Xmx 均为 48g。表中给出五轮中位数和范围。

| 格 | 峰值堆中位数 | 五轮范围 | 4 GiB D1 峰值 | GC 时间中位数 | GC 次数中位数 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 11.97 GiB | 1.43–13.10 GiB | 0 | 77.91 s | 1,255 |
| 10 原始 + Track1 | 25.44 GiB | 24.89–26.35 GiB | 4.00 GiB | 75.20 s | 984 |
| 01 joint v2 | 4.78 GiB | 1.39–12.84 GiB | 0 | 16.64 s | 2,008 |
| 11 joint v2 + Track1 | 15.12 GiB | 14.56–16.84 GiB | 4.00 GiB | 13.13 s | 1,214 |

Track1 在原始布局上把峰值堆中位数提高 13.47 GiB（11.97→25.44），远大于 4 GiB 的缓存预算。不能把差值全部归因于缓存本体；它还包含命中路径上 SDK/Spark 对象的存活、不同查询阶段的峰值时刻以及 GC 时序。即便如此，25.44 GiB 仍比 48g Xmx 留有约 22.6 GiB 的名义余量，五轮峰值稳定在 24.89–26.35 GiB，没有 OOM。

joint v2 显著降低了堆和 GC 压力：不开 Track1 时，峰值堆中位数从 11.97 降至 4.78 GiB，GC 时间从 77.91 降至 16.64 s；开 Track1 时，峰值从 25.44 降至 15.12 GiB，GC 时间从 75.20 降至 13.13 s。原因不是更小的 D1——两格都装满 4.00 GiB——而是 joint 将文件从 973 收到 32、远程 GET 从 81,492 降到 44,242，元数据和读取对象的短期分配量随之下降。GC 次数在 joint-off 反而更多（约 2,008 次），但单次停顿更短，总 GC 时间低约 79%；周期 GC 每 30 秒会影响次数，所以应以总 GC 时间和峰值堆为主，不把次数直接解读成压力大小。

Track1 的缓存行为：

| 指标 | 原始 + Track1 | joint v2 + Track1 |
| --- | ---: | ---: |
| cache hit 中位数 | 60,979 | 28,885 |
| 命中率（hits / (hits + misses)） | 81.8% | 77.3% |
| cache useful bytes 中位数 | 5.57 GiB | 7.34 GiB |
| admitted blocks 中位数 | 12,655 | 12,316 |
| rejected blocks 中位数 | 17,152 | 8,780 |
| evicted bytes 中位数 | 0.63 GiB | 1.59 GiB |
| cached / peak cached | 4.00 / 4.00 GiB | 4.00 / 4.00 GiB |

原始布局的命中率更高、命中次数更多，因为它保留了大量可由后续查询重复利用的小 range；joint 已把其中一部分合并掉。joint 的 useful bytes 反而更多，说明留下来的命中单次更大；这也是命中次数减少但仍有 6.9% 增量收益的原因。

### 5.4 逐查询与 COS 对照

joint v2 使 24/24 条查询全部变快；Track1 在原始和 joint 上也都是 24/24 变快。布局收益最大的是 Q1–Q3、Q6、Q13：约 −31%～−36%。Q8（26 个类别列缺失率）是 −23.2%；Q20–Q22 三个训练 epoch是 −15.5%、−17.2%、−16.5%；Validation/Test 是 −15.4%/−14.8%。组合后 Q2/Q3 约 −73%，Q13 −50.8%，Q6 −49.6%。

AWS 与 COS 的 IO 基本复现：joint 都是 44,242 GET / 187.87 GiB；joint+Track1 是 AWS 15,357 GET / 180.54 GiB，对 COS 15,510 GET / 180.55 GiB。由此可以把两边墙钟差异归因于执行环境，而不是物理布局或 S3 range 访问模式。

| 效应 | AWS S3 | COS |
| --- | ---: | ---: |
| Track1 @ 原始 | −13.1% | −20.8% |
| joint @ off | −19.0% | −21.4% |
| Track1 @ joint | −6.9% | −11.2% |
| 组合 | −24.6% | −30.2% |

两个环境方向一致：joint 是主收益，Track1 是进一步增量；joint 又降低峰值堆和 GC 时间。COS 绝对墙钟更短、缓存增量更大，可能来自 CPU/字符串解码吞吐、对象存储延迟、JVM/GC 版本或宿主机噪声；仅凭这组数据不能继续拆分。

### 5.5 复现与保留的失败证据

小体积结果归档在：

```text
docs/adaptive-range-reader/results/track2/criteo_2x2_n5_48g_gc30s_v2/
```

包括 20 个轮次报告、计划、物化 manifest、candidate snapshot、分区/上传检查和准备日志。大体积 IO 轨迹保留在：

```text
/mnt/ssd-cache/criteo_local/grid/criteo_2x2_n5_48g_gc30s_v2/
```

`geometry_gate.json` 保留模型预测失败（24 文件/34.56 GB vs 实测 32 文件/45.12 GB）；`cos_reference_gate.json` 记录 AWS 写出布局与 COS 物理参考逐项等价。后续修 planner 压缩/encoding 建模时，应先让原 geometry gate 通过，而不是继续依赖参考门。

---

## 6. NovoMCP 化合物筛选，AWS S3，2026-10-30

完成时间 2026-10-30 18:07 +08:00（10:07 UTC）。20 格全部成功，没有查询错误、OOM 或 scratch ENOSPC。四格 CV 都低于 5%。Driver 是 48g；`spark.cleaner.periodicGC.interval=30s`，scratch 使用独立 NVMe。

本节参照前三次 COS 2×2 的协议，但数据放在 AWS S3，并且这次规划不再有 `--dataset` 名称开关：planner 只读本次 00 的 `dataset_snapshot.json`、`access_profile.json`、`sysconst.json` 和 `layout_probe.json`。

### 6.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | NovoMCP Open Corpus Lite，化合物表，Snappy，59 列，无 `day=` 分区 |
| 行数 | 122,454,458 |
| 原始布局 | 21 个文件，132 个行组，34.27 GiB（36,797,876,065 字节） |
| joint 布局 | 20 个文件，132 个行组，29.08 GiB（31,212,517,905 字节），写成 `compounds/part-NNNNN.parquet` |
| 查询 | `NovoMCP-Compound-Screen-v1`，20 条；Q18–Q20 是相同候选打分输入扫描的 3 个 epoch |
| 机器 | AWS EC2，Spark `local[16]`，driver 48g |
| 存储 | AWS S3，`us-east-2` |
| 原始前缀 | `s3://home-haoyue/track2/novomcp_open_corpus_lite/`（表 `compounds`） |
| joint 前缀 | `s3://home-haoyue/track2/novomcp_open_corpus_lite_joint2/`（表 `compounds`） |
| sysconst | `measured_cross_cloud`：RTT 25.5 ms，带宽 131.9 MiB/s，K_busy 6.77，K_data 5.67，K_metadata 2.28 |
| 查询文件 | `tools/track2/novomcp_queries.json`，SHA-256 `a2b4bfc30dcffb9d518bd0b2e84cfae4e9df1fda6fb3a7a3176242dce548c1dc` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/novomcp_2x2_n5_48g_gc30s_r2/` |

规划输入全部来自本次 00：`S3ARR_SCAN_TASK_FLOOR=0`，UC1 PyArrow，压缩探测打开。L1 选择 `order=seriation`、`compression=global-zstd`、`page=baseline`、`file=26f`、`rg=baseline`，3 个动作：全局 zstd、列顺序 seriation、目标文件 1,415,302,927 字节。虚拟候选预计 18,961 次 GET、62.92 GiB、`t_io=156.7 s`。

几何门通过。行组数对齐（132），行数对齐（122,454,458）。文件数预测 22、写出 20；压缩后体积预测 29.96 GiB、写出 29.08 GiB，差 3%，在容差内。这次没有出现 Criteo 那种 `-31%` 的压缩率预测偏差。

字节覆盖率门禁通过（>0.95）。关联阶段把 21 个本地文件的 footer 与 00 的 IO 轨迹按路径后缀对齐，这是本轮能直接用本地文件做规划输入的前提。

### 6.2 墙钟与 IO

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 191.0 / 197.3 / 201.7 / 197.0 / 192.0 | 197.0 | 1.99% | 21,724 | 83.08 GiB | 13.25 GiB |
| 10 原始 + Track1 | 171.6 / 175.7 / 183.4 / 176.6 / 174.1 | 175.7 | 2.25% | 4,949 | 72.10 GiB | 18.51 GiB |
| 01 joint | 191.3 / 189.9 / 189.4 / 189.3 / 189.2 | 189.4 | 0.39% | 16,829 | 68.60 GiB | 6.50 GiB |
| 11 joint + Track1 | 173.4 / 174.0 / 174.4 / 171.9 / 170.1 | 173.4 | 0.91% | 3,649 | 58.17 GiB | 21.59 GiB |

按墙钟中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −21.3 s | −10.8% |
| Track1，加在 joint 上（11−01） | −16.0 s | −8.5% |
| Track2，没有 Track1（01−00） | −7.6 s | −3.8% |
| Track2，有 Track1（11−10） | −2.3 s | −1.3% |
| 两项一起（11−00） | −23.6 s | −12.0%（1.136×） |

分开两项的和是 −28.9 s，一起跑下来是 −23.6 s，重叠约 5.3 s。两者基本可加，替代关系比 Criteo 弱很多。和 NYC Yellow、Criteo 相反，这次 Track1 是主收益，布局只贡献 −3.8%。

### 6.3 堆与 GC

`peak_heap_bytes` 是 Track1 probe 观测到的最大 JVM heap used，不是 Xmx；四格 Xmx 均为 48g。

| 格 | 峰值堆中位数 | 4 GiB D1 峰值 | GC 时间中位数 |
| --- | ---: | ---: | ---: |
| 00 原始 | 13.25 GiB | 0 | 3.9 s |
| 10 原始 + Track1 | 18.51 GiB | 4.00 GiB | 4.7 s |
| 01 joint | 6.50 GiB | 0 | 3.7 s |
| 11 joint + Track1 | 21.59 GiB | 4.00 GiB | 3.9 s |

两点值得记录。第一，joint 不开 Track1 时峰值堆从 13.25 降到 6.50 GiB，和 Criteo 的方向一致：更少、更大的文件降低了元数据和读取对象的短期分配。第二，joint + Track1 的峰值堆（21.59 GiB）反而高于原始 + Track1（18.51 GiB），尽管 joint 的 GET 更少、useful bytes 相近（10.42 vs 10.99 GiB）。原因是 joint 的单次 GET 更大（远端 58.17 GiB 用 3,649 次 GET），缓存块和工作集更集中，命中路径上同时存活的对象更多。这也说明峰值堆不能简单用 GET 数解释，必须实测。四格 GC 时间都在 3.7–4.7 s，不是瓶颈。

Track1 的逻辑请求命中率按 `cache_hits / (cache_hits + 远程 GET)` 计算：原始布局 77.2%（命中 16,775，远程 4,949），joint 78.3%（命中 13,180，远程 3,649），与 COS 的 77.0% / 77.2% 基本相同。`track1.cache_misses` 是缓存内部查找口径，若用它作分母会得到 89.9% / 85.0%，不能与“逻辑 GET 被缓存消掉多少”混用。joint 已经合并掉一部分请求，留给缓存消除的绝对请求数变少，所以墙钟收益从 21.3 s 降到 16.0 s；这不是缓存能力下降。

### 6.4 逐查询

布局并不是对所有查询都有效：

- 布局变快：Q1–Q13、Q17，共 14 条；最大是 Q10 −25.6%、Q11 −14.6%、Q2 −10.3%、Q1 −9.0%
- 布局变慢：Q14 +7.6%、Q15 +4.5%、Q16 +2.4%，以及 **Q18–Q20 三个候选打分 epoch 分别 +9.9%、+8.2%、+4.9%**

这是本轮最关键的结果。joint 把文件从 21 收到 20、把行组放大，GET 降 22.5%、远端字节降 17.4%，但三条 epoch 查询反而变慢。它们要扫全表的打分输入列，工作集远大于 4 GiB；更大的单次读让解码和聚合的峰值资源上升。布局在窄列和中等扫描上有效（Q10 −25.6%），在宽 epoch 扫描上无效。

Track1 在这张表上没有回退：canonical 和 joint 上都是 20/20 变快。最大收益是 Q15 −43.7%、Q7 −29.9%、Q4 −29.2%、Q3 −24.2%，这些查询反复读取同一批理化性质列，工作集在 4 GiB 内。

组合后仍有 3 条回退：Q18 +4.7%、Q19 +6.5%、Q20 +1.8%。也就是说，对这条 workload，`joint + Track1` 的总收益 −12.0% 全部来自非 epoch 查询；若只看 Q18–Q20，joint 是负收益，只有 Track1 有效。

### 6.5 过程中的两处偏差

**第一，基线文件需要规范化。** `pubchem_novomcp_lite_00020.parquet`（2,353 行，552,343 字节）的 `hba`/`hbd` 是 int64，其余 20 个文件是 double。这会让压缩探测直接失败：

```text
ArrowInvalid: Schema at index 20 was different
```

也会让两个 canonical 格报：

```text
[FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH]
```

处理方式与 COS 那次一致：只规范化这一个文件，`hba`/`hbd` 改为 double，Snappy、1 个行组、2,353 行、59 列、列顺序不变，原文件保留为备份。本地副本和 S3 原始前缀都做了替换，其余 20 个文件未动。记录见：

```text
docs/adaptive-range-reader/results/track2/novomcp_2x2_n5_48g_gc30s_r2/schema_normalization.json
```

首次网格的前两格因此失败（`canonical-off/final` run 1 各有 8 条查询报类型不匹配）。规范化 S3 文件后，整个网格从零重跑；失败结果没有进入本节主表。

**第二，网格运行载体不是 tmux。** 计划是在 tmux 里跑，但两次 `tmux new-session` 等待确认超时，最终用 `setsid nohup` 后台运行。效果等价于脱离当前会话，但本轮没有 tmux 会话可 attach；结果、日志和退出状态都完整保留。

### 6.6 复现

小体积结果归档在：

```text
docs/adaptive-range-reader/results/track2/novomcp_2x2_n5_48g_gc30s_r2/
```

包括 20 个轮次报告、`plan_deterministic.json`、物化 manifest、candidate snapshot、footer/关联覆盖率、上传核对和规范化记录。大体积 IO 轨迹保留在：

```text
/mnt/ssd-cache/novomcp_local/grid/novomcp_2x2_n5_48g_gc30s_r2/
```

---

## 7. NYC Yellow Taxi，AWS S3，2026-11-01

完成时间 2026-11-01 14:30 +08:00（06:30 UTC）。20 格全部成功，没有查询错误、OOM 或 scratch ENOSPC。四格 CV 都低于 5%。Driver 是 48g；`spark.cleaner.periodicGC.interval=30s`，scratch 使用独立 NVMe。

本节把 §1 的 COS NYC Yellow 搬到 AWS S3 重跑。两份布局由 COS 复制而来，本机只做传输与核对，没有重新规划或重写布局。

### 7.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | NYC TLC Yellow Taxi 月度 Parquet，收成一张表 `nyc_yellow` |
| 原始布局 | 204 个文件，42.17 GiB（42,168,461,705 字节） |
| joint 布局 | 109 个文件，41.22 GiB（41,223,818,584 字节） |
| 查询 | 4 个 epoch，同一组训练列各扫一遍；语句只在 epoch 常量上不同 |
| 机器 | AWS EC2，Spark `local[16]`，driver 48g |
| 存储 | AWS S3，`us-east-2` |
| 原始前缀 | `s3://home-haoyue/nyc_yellow/`，benchmark 根 `s3a://home-haoyue`，`--tables nyc_yellow` |
| joint 前缀 | `s3://home-haoyue/nyc_yellow_joint2/nyc_yellow/`，benchmark 根 `s3a://home-haoyue/nyc_yellow_joint2`，`--tables nyc_yellow` |
| 查询文件 | `tools/track2/nyc_yellow_epoch_queries.json` |
| 结果目录 | `docs/adaptive-range-reader/results/track2/nyc_yellow_2x2_n5_48g_gc30s/` |

协议与之前的 AWS 网格一致：四格都传 `--track1-s3a`。因此 00/01 是 Track1 passthrough factory（D1/D2/D4 全关），10/11 打开 D1；这里的 Track1 效应只隔离 D1，不包含客户端工厂切换。COS 那次 NYC（§1）在 00/01 用原生 S3A，所以两边的 Track1 百分比不能直接横比。

### 7.2 墙钟与 IO

| 格 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 峰值堆中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 00 原始 | 342.9 / 345.3 / 348.9 / 340.2 / 353.5 | 345.3 | 1.34% | 121,730 | 104.10 GiB | 1.99 GiB |
| 10 原始 + Track1 | 315.5 / 327.2 / 318.2 / 317.4 / 319.4 | 318.2 | 1.26% | 108,885 | 92.04 GiB | 8.64 GiB |
| 01 joint | 277.3 / 284.6 / 288.0 / 291.1 / 283.2 | 284.6 | 1.63% | 9,266 | 109.33 GiB | 12.89 GiB |
| 11 joint + Track1 | 275.0 / 273.1 / 279.9 / 275.7 / 271.9 | 275.0 | 1.00% | 5,351 | 100.44 GiB | 17.19 GiB |

按墙钟中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上（10−00） | −27.1 s | −7.8% |
| Track1，加在 joint 上（11−01） | −9.6 s | −3.4% |
| Track2，没有 Track1（01−00） | −60.7 s | −17.6% |
| Track2，有 Track1（11−10） | −43.2 s | −13.6% |
| 两项一起（11−00） | −70.3 s | −20.4% |

四项配对差全部 5/5 同方向：

| 比较 | 五轮配对比例 |
| --- | ---: |
| 00→10 | −8.0 / −5.2 / −8.8 / −6.7 / −9.6 % |
| 00→01 | −19.1 / −17.6 / −17.5 / −14.4 / −19.9 % |
| 01→11 | −0.8 / −4.0 / −2.8 / −5.3 / −4.0 % |
| 00→11 | −19.8 / −20.9 / −19.8 / −19.0 / −23.1 % |

分开两项的和是 −87.8 s，一起跑下来是 −70.3 s，重叠约 17.5 s。仍是替代关系，但比 Criteo 弱。

### 7.3 读法

这次布局收益主要来自请求数，不是传输量。joint 把 GET 从 121,730 降到 9,266（−92.4%），远端字节反而从 104.10 升到 109.33 GiB（+5.0%）。文件从 204 收到 109，读变大变少。AWS 的 RTT 很低（Criteo 那次拟合是 25.5 ms），但 12 万次请求仍然主导墙钟；合并成 9 千次大读后墙钟降 17.6%。

逐查询非常一致，四个 epoch 都是 −15.6% 到 −18.7%：

| 查询 | 00 | 10 | 01 | 11 | 01 相对 00 | 11 相对 00 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Q1 | 90.0 | 88.8 | 75.9 | 69.9 | −15.6% | −22.4% |
| Q2 | 85.9 | 76.8 | 69.8 | 68.2 | −18.7% | −20.6% |
| Q3 | 85.2 | 76.5 | 70.2 | 68.1 | −17.6% | −20.1% |
| Q4 | 84.9 | 76.9 | 69.4 | 68.8 | −18.3% | −18.9% |

Track1 在 joint 上只剩 −3.4%，因为 joint 已经把可复用的重复请求消掉大半：逻辑命中率在 joint+Track1 是 42.3%（命中 3,915，远程 5,351），而原始+Track1 只有 10.6%（命中 12,845，远程 108,885）。命中的绝对字节反而更少（8.89 vs 12.06 GiB）。

### 7.4 堆

`peak_heap_bytes` 是 probe 观测到的最大 JVM heap used，不是 48g Xmx。

| 格 | 峰值堆中位数 | 4 GiB D1 峰值 | GC 时间中位数 |
| --- | ---: | ---: | ---: |
| 00 原始 | 1.99 GiB | 0 | 4.5 s |
| 10 原始 + Track1 | 8.64 GiB | 4.00 GiB | 4.0 s |
| 01 joint | 12.89 GiB | 0 | 5.2 s |
| 11 joint + Track1 | 17.19 GiB | 4.00 GiB | 5.4 s |

这张表和 Criteo、NovoMCP 相反：joint 提高了峰值堆。原因是 joint 的单次读大得多（109.33 GiB 只用 9,266 次 GET），解码/聚合批次和缓存块同时存活的对象更多。四格 GC 时间都在 4.0–5.4 s，不是瓶颈；堆峰值最高 17.19 GiB，距 48g Xmx 仍有较大余量，没有 OOM。

### 7.5 过程中的两个问题

**第一，凭证文件格式导致首轮 20 格全部秒退。** `~/.aws/credentials` 的 `[aws]` 段里有两组重复的 `aws_access_key_id` / `aws_secret_access_key`，而 `s3a_session.load_creds()` 用严格模式解析：

```text
configparser.DuplicateOptionError: option 'aws_access_key_id' in section 'aws' already exists
```

20 个 slot 在每个约 1 秒内失败，`grid.exit=0` 但 0 份报告。失败目录保留为：

```text
/mnt/ssd-spare/nyc_grid/nyc_yellow_2x2_n5_48g_gc30s_failed_credparse/
```

处理方式是给 `load_creds()` 增加两个可选环境变量，不改用户凭证文件、也不把密钥写进命令历史：

```text
S3ARR_AWS_CREDENTIALS_FILE  指定单 profile 凭证文件
S3ARR_AWS_SECTION           指定读取哪个段
```

本次用一份权限 `0600`、只含 AWS `[default]` 的临时文件启动。这个改动是加性兼容的：不设环境变量时行为与之前完全相同。

**第二，AWS 与 COS 的 Track1 口径不同。** COS NYC 的 00/01 用原生 S3A；本次 AWS 四格都用 Track1 factory。因此本节的 −7.8% 与 §1 的 Track1 百分比不是同一个量，不能直接比较大小。已在 §7.7 补跑 00/01 不传 `--track1-s3a` 的对照轮，实测两版四格差异在 ±1.6% 以内。

### 7.6 复现

结果归档在：

```text
docs/adaptive-range-reader/results/track2/nyc_yellow_2x2_n5_48g_gc30s/
```

大体积 IO 轨迹保留在：

```text
/mnt/ssd-spare/nyc_grid/nyc_yellow_2x2_n5_48g_gc30s/
```

### 7.7 COS 协议对照轮（2026-11-01）

§7.5 第二点留下的口径缺口：本节补跑一次 **00/01 不传 `--track1-s3a`（原生 S3A client）、10/11 传 `--track1-s3a` 并开 D1** 的网格，与 COS NYC 的协议一致。其余配置与 §7 完全相同（48g、`periodicGC=30s`、`local[16]`、4 条 epoch 查询、五轮轮换、scratch 独占 nvme2）。20/20 有效，无查询错误。

逐格对照（五轮中位数，秒）：

| 臂 | 同 factory（四格都 `--track1-s3a`） | COS 协议（00/01 原生 S3A） | 差 |
| --- | ---: | ---: | ---: |
| `canonical-track1-off` | 345.3 | 350.7 | +1.6% |
| `canonical-track1-final` | 318.2 | 321.2 | +0.9% |
| `joint2-track1-off` | 284.6 | 282.3 | −0.8% |
| `joint2-track1-final` | 275.0 | 275.2 | +0.1% |

COS 协议版自身的因子效应（CV 均 <1.5%）：

- 布局效应（Track1 off）**−19.5%**
- Track1 在原始布局上 **−8.4%**（此口径包含 client factory 切换 + D1）
- Track1 在候选布局上 −2.5%
- 组合 **−21.5%**

结论：**passthrough factory 本身没有可观开销。** 四格差异全部在 ±1.6% 以内，且方向不一致（off 臂 +1.6%、joint2-off 臂 −0.8%），与两版 CV（0.7%–1.6%）同量级，属于噪声而非系统差。因此 §7 的 −7.8% 与 §1 COS 的 Track1 百分比**可以直接比较**：若两者仍有明显差距，原因在 D1 行为与介质，不在 client 切换。IO 侧也一致：两版 GET 数几乎逐位相同（121,730 / 108,88x / 9,266 / 5,35x），字节数完全相同，说明 client 切换不改变请求形状。

编排脚本：`run_nyc_2x2_grid_cos_protocol.py`（`run_nyc_2x2_grid.py` 的变体，仅在 `not spec.d1` 时移除 `--track1-s3a`）。结果目录：

```text
/mnt/ssd-spare/nyc_grid/nyc_yellow_2x2_n5_48g_gc30s_cos_protocol/
```

---

## 8. ClickBench SF8，AWS S3，当前协议重跑，2026-11-02

完成时间 2026-11-02 11:50 +08:00（03:50 UTC）。20 格全部成功，没有查询错误、OOM 或 scratch ENOSPC。四格 CV 均低于 1.2%。Driver 是 48g；`spark.cleaner.periodicGC.interval=30s`；scratch 使用独立 NVMe。

本节补的是五种 AWS 负载里最后一个尚未使用当前运行协议的负载。ClickBench 此前只有 2026-09-23/24 的旧网格（SF1 与 SF8，32g、无周期 GC），与 §5 至 §7 的 TPC-H / Criteo / NovoMCP / NYC 不同代次，不能并列进主表。这里按当前协议重跑 SF8，并**复用已验证的 `joint2` 物理布局，不重新规划、不重写**。

### 8.1 数据与协议

| 项 | 值 |
| --- | --- |
| 数据 | ClickBench `hits` 表，SF8 |
| 行数 | 799,979,976 |
| 原始布局 | `s3a://home-haoyue/track2/clickbench_sf8`，881 个对象，120,252,196,875 字节 |
| joint 布局 | `s3a://home-haoyue/track2/clickbench_sf8_e0_uc1_joint2`，176 个对象，87,957,897,552 字节 |
| joint 物化 | 174 个 Parquet 文件，行组 591,702 行，行组中位 424.37 MB，最大 519.47 MB，manifest 校验 `pass` |
| 查询 | `tools/track2/clickbench_queries.json`，Q1–Q43；SHA-256 `8775514e0ba2aa41230803e15afe43e753bccef72081d9fc0dd69eb550007fbb` |
| 机器 | AWS EC2，Spark `local[16]`，driver 48g |
| 存储 | AWS S3，`us-east-2` |
| Track1 | 四格都传 `--track1-s3a`；final 为固定 4 GiB W-TinyLFU、adaptive、admit 0 |
| 结果目录 | `/mnt/ssd-cache/clickbench_grid/clickbench_sf8_2x2_n5_48g_gc30s_current/` |

不重写布局的依据：本次要统一的是**运行协议**（堆、GC、scratch、JAR、Track1 配置、轮换方式），这些都不改变 Parquet 物理形态。旧 `joint2` 由 2026-09-10 的旧版 planner 产出，其目标文件约 470 MB、compression/encoding 选择也来自旧流水线；若论文要声称"五种负载全部由当前 planner 端到端生成"，才需要重跑规划与重写。主表按"当前协议 + 既有已验证布局"口径成立。

### 8.2 墙钟与 IO

| 臂 | 五轮查询时间和 (s) | 中位数 | CV | GET | 远端字节 | 命中率 | 峰值堆中位数 | GC 时间中位数 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| canonical + off | 2405 / 2351 / 2398 / 2358 / 2396 | 2395.5 | 0.93% | 262,309 | 371.9 GiB | — | 3.56 GiB | 30.9 s |
| canonical + final | 1991 / 1975 / 1936 / 1934 / 1957 | 1957.3 | 1.13% | 70,844 | 333.1 GiB | 75.7% | 22.55 GiB | 24.5 s |
| joint + off | 1865 / 1847 / 1849 / 1875 / 1837 | 1848.8 | 0.73% | 183,675 | 252.1 GiB | — | 7.01 GiB | 23.6 s |
| joint + final | 1645 / 1651 / 1637 / 1639 / 1634 | **1638.9** | 0.36% | 44,597 | 210.5 GiB | 78.0% | 23.60 GiB | 17.2 s |

按中位数：

| 比较 | 差值 | 比例 |
| --- | ---: | ---: |
| Track1，加在原始布局上 | −438.2 s | **−18.3%** |
| Track1，加在 joint 上 | −209.9 s | −11.4% |
| 布局（Track1 off） | −546.7 s | **−22.8%** |
| 布局（Track1 final） | −318.4 s | −16.3% |
| 两项一起 | −756.6 s | **−31.6%（1.46×）** |

分开两项的和是 −984.9 s，一起跑下来是 −756.6 s，重叠约 228.4 s（9.5 个百分点），仍是替代关系。

43 条查询全部变快：布局无回归，Track1 在原始布局和 joint 上也无回归。布局收益最大的是 Q37 −63%、Q38/Q39 −56%、Q43/Q42 −54%、Q41 −53%；最小的是 Q29 −5%、Q33 −12%、Q30 −13%、Q23 −13%。这是目前五种 AWS 负载里最稳的一组：四臂 CV 全部低于 1.2%，`joint + final` 只有 0.36%。

### 8.3 与旧网格的对照（旧结果不进主表）

| 臂 | 旧（2026-09，32g，无 gc30s） | 新（当前协议） | 差 |
| --- | ---: | ---: | ---: |
| canonical + off | 2309.5 | 2395.5 | +3.7% |
| canonical + final | 2038.1 | 1957.3 | −4.0% |
| joint + off | 1896.2 | 1848.8 | −2.5% |
| joint + final | 1711.8 | 1638.9 | −4.3% |

三点：

1. **off 臂慢 3.7%，带 Track1 的三格快 2.5–4.3%。** 48g 堆让 off 臂 GC 时间从 20.7 s 升到 30.9 s；带缓存的三格 GC 时间反而下降，D1 命中率也从 73.8% 升到 75.7%。因此 Track1 效应由旧 −11.8% 放大到 −18.3%，组合由 −25.9% 到 **−31.6%**。
2. **IO 形状完全没变。** 两个 off 格 GET 逐位相同（262,309 / 183,675），远端字节也相同（371.9 / 252.1 GiB）。这既说明布局复用有效，也说明差异只来自运行时，不是物理布局或访问模式变化。
3. **堆纪律仍然成立。** final 两臂峰值堆 22.55 / 23.60 GiB，接近但低于 48g Xmx，五轮没有 OOM。

**绝对墙钟不可与旧网格横比。** 协议变了（32g → 48g 并新增周期 GC），且两次相隔一个多月，机器与网络状态未必一致。旧 SF8 结果只保留为代次对照，不再进入主表；ClickBench 主表一行应采用本节的 −22.8% / −18.3% / −31.6%。

### 8.4 复现

编排脚本 `run_clickbench_sf8_2x2_grid.py` 由 §7 的同 factory 网格脚本改出，只改数据前缀、表名、查询文件和实验名，运行协议部分（48g、`local[16]`、`periodicGC=30s`、轮换、每格独立进程）保持不变。启动命令：

```bash
tmux new-session -d -s clickbench_sf8_current "bash -lc 'cd /home/ec2-user/aws-sdk-java-v2; \
  env -u CODEBUDDY_SESSION_ID -u CLAUDE_SESSION_ID \
  S3ARR_AWS_CREDENTIALS_FILE=<临时凭证文件> AWS_DEFAULT_REGION=us-east-2 \
  /home/ec2-user/aws-sdk-java-v2/.venv/bin/python \
  <grid>/run_clickbench_sf8_2x2_grid.py --out <grid> --runs 5 --driver-memory 48g \
  --scratch /mnt/ssd-spare/track2-scratch --resume > <grid>/grid.log 2>&1'"
```

结果目录（小体积报告与大体积 IO 轨迹都在数据盘）：

```text
/mnt/ssd-cache/clickbench_grid/clickbench_sf8_2x2_n5_48g_gc30s_current/
```
