#!/usr/bin/env python3
"""Train the Track 1 statistical IO-policy selector (PROJECT2 20.2).

Path 2 (offline) proof: instead of touching the AWS SDK, we learn a lightweight
model that picks one of the existing simulator sub-policies per read, and check
(downstream, via eval_track1.py) whether it beats the hand-written
`template_auto`.

Pipeline:
  1. Replay each single-workload trace through the SAME feature extractor used
     by prefetch_simulator.StatisticalPolicySelector (no query_id/workload label
     as input -> no oracle leakage).
  2. Label every read with that workload's best static strategy (PROJECT2 6.2).
  3. Train a DecisionTree (primary; interpretable, us inference, KB memory) and
     an RBF-SVM (comparison), report stratified-CV accuracy + feature importance.
  4. Save a joblib bundle consumable via
     `prefetch_simulator.py --policy stat_selector --model <bundle>`.

Note on accuracy numbers: stratified CV here measures in-distribution feature
separability only (one/few traces per label). The honest generalization signal
is the simulator replay on the mixed trace in eval_track1.py.
"""
from __future__ import annotations

import argparse
import json
import os
from collections import Counter, deque

import numpy as np

from sklearn.metrics import classification_report
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

from prefetch_simulator import (
    FEATURE_NAMES,
    Request,
    extract_features,
    load_trace,
    row_to_request,
)

HISTORY_WINDOW = 64

# trace path -> best static strategy (PROJECT2 6.2) + workload tag for reporting.
DEFAULT_TRAINING_SET: list[tuple[str, str, str]] = [
    ("traces/tpch_sf1_full.csv", "s3a_random", "tpch"),
    ("traces/ml_taxi_epoch.csv", "template_locality", "ml_epoch_scan"),
    ("traces/lance_sift1m_real/lance_all.csv", "template_locality", "lance_large"),
    ("traces/lance_fmnist_real/lance_all.csv", "s3a_prefetch", "lance_small"),
    ("traces/ml_emb_real.csv", "s3a_prefetch", "ml_embedding"),
    ("traces/mm_n2000/retrieval.csv", "template_multimodal", "multimodal"),
]


def featurize_trace(path: str) -> np.ndarray:
    """Feature matrix for one trace, one row per read (window matches inference)."""
    df = load_trace([path])
    history: deque[Request] = deque(maxlen=HISTORY_WINDOW)
    rows: list[np.ndarray] = []
    for row in df.itertuples(index=False):
        req = row_to_request(row)
        rows.append(extract_features(list(history), req))
        history.append(req)
    if not rows:
        return np.empty((0, len(FEATURE_NAMES)), dtype=float)
    return np.asarray(rows, dtype=float)


def build_dataset(
    training_set: list[tuple[str, str, str]],
    max_per_trace: int,
    seed: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, int]]:
    rng = np.random.default_rng(seed)
    x_parts: list[np.ndarray] = []
    y_parts: list[np.ndarray] = []
    tag_parts: list[np.ndarray] = []
    per_trace: dict[str, int] = {}

    for path, label, tag in training_set:
        if not os.path.exists(path):
            raise FileNotFoundError(f"training trace missing: {path}")
        feats = featurize_trace(path)
        if max_per_trace and len(feats) > max_per_trace:
            idx = np.sort(rng.choice(len(feats), size=max_per_trace, replace=False))
            feats = feats[idx]
        x_parts.append(feats)
        y_parts.append(np.full(len(feats), label))
        tag_parts.append(np.full(len(feats), tag))
        per_trace[tag] = len(feats)
        print(f"  {tag:16s} <- {os.path.basename(path):28s} rows={len(feats):7d} label={label}")

    x = np.vstack(x_parts)
    y = np.concatenate(y_parts)
    tags = np.concatenate(tag_parts)
    return x, y, tags, per_trace


def train_decision_tree(x, y, max_depth, min_samples_leaf, seed) -> DecisionTreeClassifier:
    clf = DecisionTreeClassifier(
        max_depth=max_depth,
        min_samples_leaf=min_samples_leaf,
        class_weight="balanced",
        random_state=seed,
    )
    clf.fit(x, y)
    return clf


def train_svm(x, y, seed) -> Pipeline:
    clf = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("svc", SVC(kernel="rbf", C=4.0, gamma="scale", class_weight="balanced", random_state=seed)),
        ]
    )
    clf.fit(x, y)
    return clf


def cv_accuracy(clf, x, y, seed) -> dict[str, float]:
    n_min = min(Counter(y).values())
    folds = int(min(5, n_min))
    if folds < 2:
        return {"folds": folds, "mean": float("nan"), "std": float("nan")}
    skf = StratifiedKFold(n_splits=folds, shuffle=True, random_state=seed)
    scores = cross_val_score(clf, x, y, cv=skf, scoring="accuracy")
    return {"folds": folds, "mean": float(scores.mean()), "std": float(scores.std())}


def main() -> None:
    ap = argparse.ArgumentParser(description="Train Track 1 statistical IO-policy selector")
    ap.add_argument("--out", default="models/policy_selector.joblib", help="primary (decision tree) bundle path")
    ap.add_argument("--save-svm", default=None, help="optional path to also save the SVM bundle")
    ap.add_argument("--report", default="models/policy_selector_report.json")
    ap.add_argument("--max-per-trace", type=int, default=15000, help="cap feature rows per trace (0=all)")
    ap.add_argument("--max-depth", type=int, default=12)
    ap.add_argument("--min-samples-leaf", type=int, default=10)
    ap.add_argument("--skip-svm", action="store_true",
                    help="skip the RBF-SVM comparison (intractable on large datasets)")
    ap.add_argument("--svm-max-rows", type=int, default=30000,
                    help="subsample cap for RBF-SVM training (O(n^2) kernel)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    print("=== featurizing training traces ===")
    x, y, tags, per_trace = build_dataset(DEFAULT_TRAINING_SET, args.max_per_trace, args.seed)
    print(f"\ndataset: X={x.shape} labels={dict(Counter(y))}")

    x_tr, x_te, y_tr, y_te = train_test_split(
        x, y, test_size=0.25, random_state=args.seed, stratify=y
    )

    print("\n=== decision tree (primary) ===")
    tree = train_decision_tree(x, y, args.max_depth, args.min_samples_leaf, args.seed)
    tree_cv = cv_accuracy(
        DecisionTreeClassifier(
            max_depth=args.max_depth,
            min_samples_leaf=args.min_samples_leaf,
            class_weight="balanced",
            random_state=args.seed,
        ),
        x, y, args.seed,
    )
    tree_holdout = train_decision_tree(x_tr, y_tr, args.max_depth, args.min_samples_leaf, args.seed)
    tree_report = classification_report(y_te, tree_holdout.predict(x_te), output_dict=True, zero_division=0)
    print(f"stratified {tree_cv['folds']}-fold CV accuracy: {tree_cv['mean']:.4f} +/- {tree_cv['std']:.4f}")
    importances = sorted(
        zip(FEATURE_NAMES, tree.feature_importances_), key=lambda kv: kv[1], reverse=True
    )
    print("feature importance:")
    for name, imp in importances:
        print(f"  {name:24s} {imp:.4f}")

    if args.skip_svm:
        print("\n=== rbf svm (comparison) === skipped (--skip-svm)")
        svm = None
        svm_cv = {"folds": 0, "mean": float("nan"), "std": float("nan"), "skipped": True}
        svm_report = {"skipped": True}
    else:
        print("\n=== rbf svm (comparison) ===")
        # RBF SVM is O(n^2); subsample for tractability on large datasets.
        if len(x) > args.svm_max_rows:
            rng = np.random.default_rng(args.seed)
            sidx = rng.choice(len(x), size=args.svm_max_rows, replace=False)
            xs, ys = x[sidx], y[sidx]
            print(f"  (subsampled to {args.svm_max_rows} rows for SVM)")
        else:
            xs, ys = x, y
        svm = train_svm(xs, ys, args.seed)
        svm_cv = cv_accuracy(
            Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("svc", SVC(kernel="rbf", C=4.0, gamma="scale", class_weight="balanced", random_state=args.seed)),
                ]
            ),
            xs, ys, args.seed,
        )
        xs_tr, xs_te, ys_tr, ys_te = train_test_split(
            xs, ys, test_size=0.25, random_state=args.seed, stratify=ys
        )
        svm_holdout = train_svm(xs_tr, ys_tr, args.seed)
        svm_report = classification_report(ys_te, svm_holdout.predict(xs_te), output_dict=True, zero_division=0)
        print(f"stratified {svm_cv['folds']}-fold CV accuracy: {svm_cv['mean']:.4f} +/- {svm_cv['std']:.4f}")

    labels = sorted(set(y.tolist()))
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    import joblib

    tree_bundle = {
        "model": tree,
        "model_type": "decision_tree",
        "feature_names": FEATURE_NAMES,
        "labels": labels,
        "window": HISTORY_WINDOW,
        "training_set": [(p, lab, tag) for p, lab, tag in DEFAULT_TRAINING_SET],
        "cv_accuracy": tree_cv,
    }
    joblib.dump(tree_bundle, args.out)
    print(f"\nwrote {args.out}")

    if args.save_svm and svm is not None:
        os.makedirs(os.path.dirname(os.path.abspath(args.save_svm)), exist_ok=True)
        joblib.dump(
            {
                "model": svm,
                "model_type": "svm_rbf",
                "feature_names": FEATURE_NAMES,
                "labels": labels,
                "window": HISTORY_WINDOW,
                "training_set": [(p, lab, tag) for p, lab, tag in DEFAULT_TRAINING_SET],
                "cv_accuracy": svm_cv,
            },
            args.save_svm,
        )
        print(f"wrote {args.save_svm}")

    report = {
        "labels": labels,
        "per_trace_rows": per_trace,
        "label_counts": {k: int(v) for k, v in Counter(y).items()},
        "feature_names": FEATURE_NAMES,
        "decision_tree": {
            "cv_accuracy": tree_cv,
            "holdout_report": tree_report,
            "feature_importance": {name: float(imp) for name, imp in importances},
            "params": {"max_depth": args.max_depth, "min_samples_leaf": args.min_samples_leaf},
        },
        "svm_rbf": {
            "cv_accuracy": svm_cv,
            "holdout_report": svm_report,
        },
        "note": (
            "CV accuracy measures in-distribution feature separability; the honest "
            "generalization signal is the simulator replay on the mixed trace "
            "(eval_track1.py)."
        ),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(report, f, indent=2, sort_keys=True)
        f.write("\n")
    print(f"wrote {args.report}")


if __name__ == "__main__":
    main()
