"""Record R082.6 golden output; pass a local agent-utilities checkout as argv[1]."""

import ast
import hashlib
import json
import subprocess
import sys
import threading
import types
from pathlib import Path

root = Path(__file__).resolve().parents[1]
source = subprocess.check_output(
    [
        "git",
        "-C",
        sys.argv[1],
        "show",
        "134576bd0f5931a9b22ed3c3859a3f4350731ad6:agent_utilities/knowledge_graph/analytics_worker.py",
    ],
    text=True,
)
tree = ast.parse(source)
# Execute unchanged mining definitions; omit worker transport/config imports.
keep = {
    "AnalyticsWorkerError",
    "KernelCancelled",
    "_Rule",
    "_check_cancel",
    "_association_rule_id",
    "_normalize_transactions",
    "_vertical_index",
    "_mine_itemsets",
    "_mask_split",
    "_rule_for_split",
    "_itemset_rules",
    "_rules_from_itemsets",
    "_rules_to_result",
    "_association_rules",
}
nodes = [
    n
    for n in tree.body
    if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in keep
]
prefix = ast.parse(
    "from __future__ import annotations\n"
    "import hashlib, math, struct, threading\n"
    "from dataclasses import dataclass\nfrom typing import Any\n"
).body
mod = types.ModuleType("historical_association")
sys.modules[mod.__name__] = mod
exec(
    compile(
        ast.Module(body=prefix + nodes, type_ignores=[]),
        "historical_analytics_worker.py",
        "exec",
    ),
    mod.__dict__,
)
tx = [
    ["eg:z", "eg:a", "eg:m"],
    ["eg:z", "eg:a"],
    ["eg:z", "eg:m"],
    ["eg:a", "eg:m"],
    ["eg:z", "eg:a", "eg:m"],
    ["eg:z", "eg:z"],
]
cases = []
for name, s, c in [
    ("ordered", 0.3, 0.0),
    ("ceil", 0.34, 0.0),
    ("minimum_one", 0.0, 0.0),
    ("confidence_at", 0.3, 0.75),
    ("confidence_tolerated", 0.3, 0.75 + 0.5e-12),
    ("confidence_rejected", 0.3, 0.75 + 2e-12),
]:
    rows = mod._association_rules(tx, s, c, threading.Event())
    cases.append(
        dict(
            name=name,
            min_support=s,
            min_confidence=c,
            rules=[
                {
                    k: r[k]
                    for k in [
                        "antecedent",
                        "consequent",
                        "support",
                        "confidence",
                        "lift",
                    ]
                }
                for r in rows
            ],
        )
    )
fixture = dict(
    source_repository="Knuckles-Team/agent-utilities",
    source_commit="134576bd0f5931a9b22ed3c3859a3f4350731ad6",
    deletion_commit="92974bae8c3ed3a270f582300b7458d4f53571c4",
    source_path="agent_utilities/knowledge_graph/analytics_worker.py",
    source_sha256=hashlib.sha256(source.encode()).hexdigest(),
    extraction=(
        "Unmodified mining definitions executed via AST; "
        "unrelated transport and config imports omitted."
    ),
    transactions=tx,
    cases=cases,
)
(root / "tests/fixtures/association_mining_order.json").write_text(
    json.dumps(fixture, indent=2) + "\n"
)
print([(c["name"], len(c["rules"])) for c in cases])
