import csv
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

HERE = Path(__file__).parent


def load(path):
    rows = {}
    with open(path) as f:
        for row in csv.DictReader(f):
            if not row["error"]:
                rows[row["query_id"]] = float(row["latency_ms"])
    return rows


asap = load(HERE / "asap_results.csv")
base = load(HERE / "baseline_results.csv")

qids = sorted(set(asap) & set(base))
x = np.arange(len(qids))
a_vals = [asap[q] for q in qids]
b_vals = [base[q] for q in qids]
speedup = [b / a for a, b in zip(a_vals, b_vals)]

fig, (ax1, ax2) = plt.subplots(
    2, 1, figsize=(14, 7), gridspec_kw={"height_ratios": [3, 1]}
)

# --- Top: per-query latency ---
w = 0.4
ax1.bar(x - w / 2, b_vals, w, label="Elastic baseline", color="#f4a460")
ax1.bar(x + w / 2, a_vals, w, label="ASAP (KLL sketch)", color="#4682b4")
ax1.set_xticks(x)
ax1.set_xticklabels(qids, rotation=90, fontsize=7)
ax1.set_ylabel("Latency (ms)")
ax1.set_title(
    "Query latency: ASAP vs Elastic baseline  "
    f"(p50: {np.median(a_vals):.1f}ms vs {np.median(b_vals):.1f}ms)"
)
ax1.legend()
ax1.set_xlim(-0.6, len(qids) - 0.4)

# --- Bottom: speedup ---
ax2.bar(x, speedup, color="#2e8b57", width=0.7)
ax2.axhline(
    np.mean(speedup),
    color="red",
    linewidth=1,
    linestyle="--",
    label=f"mean {np.mean(speedup):.1f}×",
)
ax2.set_xticks(x)
ax2.set_xticklabels(qids, rotation=90, fontsize=7)
ax2.set_ylabel("Speedup (×)")
ax2.legend(fontsize=8)
ax2.set_xlim(-0.6, len(qids) - 0.4)

plt.tight_layout()
out = HERE / "latency_comparison.png"
plt.savefig(out, dpi=150)
print(f"Saved {out}")
