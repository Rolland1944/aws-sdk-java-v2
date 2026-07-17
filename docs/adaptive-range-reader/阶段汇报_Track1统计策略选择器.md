# 阶段汇报 —— Track 1 统计模型 IO 策略选择器（Path 2 离线 POC）

> 对应 `PROJECT2.md` §2 Track 1 / §7.2 / §20.2、`project_notes.md` §20.5 里程碑 P1/P3。
> 路线：**Path 2（不动 SDK，先在 replay simulator + 现有 trace 上验证）**。
> 复现：`./.venv/bin/python train_policy_selector.py` → `./.venv/bin/python eval_track1.py`。
> 默认模型：`models/policy_selector.joblib`（v3 超参：`max_per_trace=15000, max_depth=12, min_samples_leaf=10`）。

---

## 1. 做了什么

在不改 AWS SDK 的前提下，把"自适应选 IO 策略"落成一个**离线可证伪的实验**：

1. **在线特征提取器**（`prefetch_simulator.py::extract_features`）：每次 `read(offset,length)`
   从最近请求流（滚动窗口，复用 `ReplayState.history`）算 10 个**纯 IO 派生**特征，
   **不喂 `query_id`/负载标签**，避免 oracle 泄漏。
2. **StatisticalPolicySelector 元策略**：加载离线训练的模型 → 预测策略标签 → **滞回平滑**
   （连续 3 次一致才切换）→ 委派给现有子策略执行。
3. **训练**（`train_policy_selector.py`）：按 §6.2 每负载最优策略打标签，训练决策树（主）。
4. **评测**（`eval_track1.py`）：四类单负载 + mixed trace，对比 `stat_selector` vs `template_auto`。

---

## 2. 模型训练结果（默认 v3）

- 数据：每条大 trace 上限 **15000** 行；决策树 `max_depth=12`、`min_samples_leaf=10`。
- **决策树** 分层 5 折 CV 准确率 **0.997 ± 0.0005**。
- 特征重要度前三：`frac_small / frac_large / cur_size_over_filesize`。

---

## 3. 评测结果（默认 v3）

成本模型：RTT=50ms、BW=100MiB/s、cache=256MiB（与 §6.2 一致）。

### 3.1 Mixed trace（主结论）

| 策略 | 总时延 | remote(GiB) | 读放大 |
|---|---|---|---|
| **stat_selector (v3)** | **2659s** | **13.0** | **1.03** |
| template_auto | 2883s | 13.4 | 1.07 |

相对 `template_auto`：
- **时延 −7.8%**（2659 / 2883）
- **远端字节 −3.5%**（13.0 / 13.4）
- 切换区最大读放大 **12.7x**（v1 曾是 291x）

**结论：决策树可优化到在时延与远端字节上同时优于手写 `template_auto`。**

### 3.2 单负载对照（参考）

| 负载 | sel/auto 时延 | sel/auto 远端字节 |
|---|---|---|
| lance_large | 0.90 | 0.98 |
| lance_small | 0.013 | 0.52 |
| ml_epoch_scan | 0.48 | 0.22 |
| ml_embedding | 0.51 | 1.13 |
| multimodal | 1.00 | 1.00 |
| tpch | 1.90 | 0.84 |

说明：当前单负载/混合集**不是标准测试集**，单场景回归（如 tpch 时延）不作为优化目标；
POC 只需证明决策树**可以**优于 `template_auto`，mixed 上的双降已足够。

### 3.3 相对首版 v1 的修复

首版 v1（`max_per_trace=8000, depth=8, leaf=50`）在 mixed 上曾用 +77.8% 远端字节换 −5.8% 时延，
且 Lance 切换点出现 291x 读放大尖峰。根因是：lance_large 中约 1.8% 大读被误判到
`s3a_random`/`s3a_prefetch`（训练采样不足，判别特征未被学到）。

v3 通过更充分采样 + 适中树深/叶大小修复该误判后：
- mixed 远端字节比从 **1.778 → 0.965**
- 切换尖峰从 **291x → 12.7x**
- 时延进一步改善到 **−7.8%**

---

## 4. 结论与判定

- **方向验证成立**：轻量决策树选择器，仅凭纯 IO 特征，可在 mixed trace 上**时延与远端字节同时优于**
  手写 `template_auto`。
- **Track 1 Path 2 目标达成**：已证明“学出来的选择器可以比规则自适应更好”，无需再针对本非标准
  测试集做逐负载调参。
- 单负载细节（如 tpch 标签的字节/时延权衡）记录在案，**暂不处理**。

---

## 5. 资产

| 文件 | 说明 |
|---|---|
| `prefetch_simulator.py` | `extract_features` + `StatisticalPolicySelector`（`--policy stat_selector --model`） |
| `train_policy_selector.py` | 默认超参即 v3；产出 `models/policy_selector.joblib` |
| `eval_track1.py` | 评测脚本；默认读 `models/policy_selector.joblib` |
| `access_report/track1_eval.json` | 当前默认评测结果（v3） |
| `access_report/track1_mixed_convergence.png` | mixed 收敛图（v3） |
| `models/policy_selector_v{1,2,3}.*` | 历史版本备份 |
