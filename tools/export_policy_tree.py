#!/usr/bin/env python3
"""Export the trained Track 1 decision tree to a stable, auditable JSON.

PROJECT2 7.2/7.3 S1 step 1: the sklearn `.joblib` bundle is a Python-only
serialized object and MUST NOT be embedded in the Java SDK. This script reads
the trained `DecisionTreeClassifier` and emits a versioned JSON that a
dependency-free Java inferencer can consume:

  * feature schema (names + order + window) so Java and the model agree,
  * the exact feature constants used by prefetch_simulator.extract_features,
  * the full tree (nodes: split feature/threshold/children, leaf -> label),
  * provenance (source bundle, sklearn/numpy versions, CV accuracy),
  * a sha256 checksum over the canonical tree/schema payload for auditing.

sklearn split semantics reproduced by Java: at an internal node, go LEFT iff
`x[feature] <= threshold`, else RIGHT. A leaf's predicted label is
`classes_[argmax(value)]` (matches DecisionTreeClassifier.predict).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import warnings

import numpy as np

SCHEMA_VERSION = 1
FEATURE_VERSION = "v1"

# Must match prefetch_simulator.py (L700-703). Exported so Java reads the same
# constants instead of hard-coding a second copy that could silently drift.
FEATURE_CONSTANTS = {
    "small_read_bytes": 16 * 1024,
    "large_read_bytes": 128 * 1024,
    "seq_gap_bytes": 64 * 1024,
    "feature_page_size": 256 * 1024,
}

# TREE_LEAF / TREE_UNDEFINED sentinels used by sklearn's tree arrays.
TREE_LEAF = -1


def load_bundle(path: str) -> dict:
    import joblib

    with warnings.catch_warnings():
        # The bundle may have been pickled with a different sklearn version;
        # we only read version-independent `tree_` arrays, so silence the warning.
        warnings.simplefilter("ignore")
        return joblib.load(path)


def build_nodes(clf) -> list[dict]:
    tree = clf.tree_
    classes = [str(c) for c in clf.classes_]
    children_left = tree.children_left
    children_right = tree.children_right
    feature = tree.feature
    threshold = tree.threshold
    value = tree.value  # shape (n_nodes, n_outputs, n_classes)

    nodes: list[dict] = []
    for i in range(tree.node_count):
        is_leaf = children_left[i] == TREE_LEAF and children_right[i] == TREE_LEAF
        if is_leaf:
            class_idx = int(np.argmax(value[i][0]))
            nodes.append(
                {
                    "id": i,
                    "leaf": True,
                    "feature_index": -1,
                    "threshold": None,
                    "left": -1,
                    "right": -1,
                    "predicted_label": classes[class_idx],
                }
            )
        else:
            nodes.append(
                {
                    "id": i,
                    "leaf": False,
                    "feature_index": int(feature[i]),
                    "threshold": float(threshold[i]),
                    "left": int(children_left[i]),
                    "right": int(children_right[i]),
                    "predicted_label": None,
                }
            )
    return nodes


def canonical_checksum(payload: dict) -> str:
    """sha256 over the schema/tree parts that determine inference behaviour."""
    material = {
        "schema_version": payload["schema_version"],
        "feature_version": payload["feature_version"],
        "feature_names": payload["feature_names"],
        "window": payload["window"],
        "labels": payload["labels"],
        "feature_constants": payload["feature_constants"],
        "nodes": payload["nodes"],
    }
    blob = json.dumps(material, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser(description="Export Track 1 decision tree to versioned JSON")
    ap.add_argument("--model", default="models/policy_selector.joblib")
    ap.add_argument("--out", default="models/policy_selector_v1.json")
    args = ap.parse_args()

    import sklearn

    bundle = load_bundle(args.model)
    clf = bundle["model"]
    if bundle.get("model_type") != "decision_tree":
        raise ValueError(f"unsupported model_type: {bundle.get('model_type')}")

    feature_names = list(bundle["feature_names"])
    labels = sorted(str(c) for c in clf.classes_)
    nodes = build_nodes(clf)

    leaf_labels = {n["predicted_label"] for n in nodes if n["leaf"]}
    unknown = leaf_labels - set(labels)
    if unknown:
        raise ValueError(f"leaf labels not in classes_: {unknown}")

    payload = {
        "schema_version": SCHEMA_VERSION,
        "feature_version": FEATURE_VERSION,
        "feature_names": feature_names,
        "window": int(bundle.get("window", 64)),
        "labels": labels,
        "feature_constants": FEATURE_CONSTANTS,
        "split_semantics": "go_left_if x[feature] <= threshold, else right; leaf label = argmax(value)",
        "n_nodes": len(nodes),
        "nodes": nodes,
        "provenance": {
            "source_bundle": args.model,
            "model_type": bundle.get("model_type"),
            "loaded_with_sklearn": sklearn.__version__,
            "loaded_with_numpy": np.__version__,
            "python": platform.python_version(),
            "cv_accuracy": bundle.get("cv_accuracy"),
            "training_set": bundle.get("training_set"),
        },
    }
    payload["sha256_checksum"] = canonical_checksum(payload)

    with open(args.out, "w") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")

    n_leaves = sum(1 for n in nodes if n["leaf"])
    print(f"wrote {args.out}")
    print(f"  nodes={len(nodes)} leaves={n_leaves} depth={clf.get_depth()}")
    print(f"  feature_names={feature_names}")
    print(f"  labels={labels}")
    print(f"  checksum={payload['sha256_checksum']}")


if __name__ == "__main__":
    main()
