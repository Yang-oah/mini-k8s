"""Generate charts from the recorded G4/G5 acceptance evidence (matplotlib required)."""

from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import numpy as np

OUT = Path(__file__).resolve().parents[2] / "docs" / "figures"
OUT.mkdir(parents=True, exist_ok=True)
font = Path("C:/Windows/Fonts/msyh.ttc")
if font.exists():
    font_manager.fontManager.addfont(str(font))
    plt.rcParams["font.family"] = font_manager.FontProperties(fname=str(font)).get_name()
plt.rcParams.update({"axes.unicode_minus": False, "font.size": 11, "figure.dpi": 160})

# Single-run wall-clock values and Gateway limits, in seconds. Values below are
# copied from g401.json, g404-window.json, g504.json, g506.json and g507.json.
labels = ["G4-01 双节点运行", "G4-04 心跳至 Unknown", "G5-04 恢复后节点 Ready", "G5-06 20 Pod Ready", "G5-07 删除后清理"]
measured = [1.699, 18.051, 14.784, 6.672, 11.814]
limits = [30, 20, 30, 30, 30]
fig, ax = plt.subplots(figsize=(10, 5.2), layout="constrained")
y = np.arange(len(labels))
ax.barh(y, limits, color="#e5edf4", label="对应时限")
ax.barh(y, measured, color="#147d92", height=.55, label="本轮测量")
for i, (value, limit) in enumerate(zip(measured, limits)):
    ax.text(value + .4, i, f"{value:.2f} s / {limit} s", va="center", fontsize=10)
ax.set_yticks(y, labels)
ax.invert_yaxis()
ax.set_xlim(0, 36)
ax.set_xlabel("耗时（秒）；不同场景的起止条件不同")
ax.set_title("G4 / G5：关键场景耗时", loc="left", fontweight="bold")
ax.legend(loc="lower right", frameon=False)
ax.spines[["top", "right", "left"]].set_visible(False)
ax.tick_params(axis="y", length=0)
fig.savefig(OUT / "acceptance-durations.png", bbox_inches="tight")
plt.close(fig)

# G4 g406.json and G5 g506.json: elapsed time reported per scale step.
labels = ["G4 目标 2", "G4 目标 4", "G4 目标 1", "G4 目标 0", "G5 20 → 4", "G5 4 → 0"]
values = [2.102851, 1.719167, .087410, .073978, .486931, .476139]
fig, ax = plt.subplots(figsize=(10, 4.7), layout="constrained")
bars = ax.bar(labels, values, color=["#147d92"] * 4 + ["#d98a35"] * 2, width=.65)
ax.bar_label(bars, labels=[f"{v:.2f} s" for v in values], padding=4)
ax.set_ylim(0, 2.8)
ax.set_ylabel("各步报告耗时（秒）")
ax.set_title("副本伸缩：各步收敛耗时\n各步 Gateway 时限均为 30 秒；坐标放大以便比较", loc="left", pad=12)
ax.spines[["top", "right"]].set_visible(False)
ax.grid(axis="y", alpha=.2)
ax.set_axisbelow(True)
fig.savefig(OUT / "acceptance-scaling.png", bbox_inches="tight")
plt.close(fig)

# G4 g405.json; G5 results.json and g503.json. Empty endpoints should refuse.
labels = ["G4 A → B", "G4 B → A", "G5 宿主机 A", "G5 宿主机 B", "G5 Pod A", "G5 Pod B", "无端点：宿主机 A", "无端点：宿主机 B", "无端点：Pod B"]
counts = [10, 10, 20, 20, 20, 20, 20, 20, 20]
fig, ax = plt.subplots(figsize=(10, 5.4), layout="constrained")
y = np.arange(len(labels))
ax.barh(y, counts, color=["#147d92"] * 6 + ["#d98a35"] * 3)
for i, count in enumerate(counts):
    ax.text(count + .25, i, f"{count}/{count}", va="center")
ax.set_yticks(y, labels)
ax.invert_yaxis()
ax.set_xlim(0, 23)
ax.set_xlabel("符合预期的全新 TCP 连接次数")
ax.set_title("跨节点与 Service 连接检查\n蓝色：成功到达；橙色：无端点时立即拒绝（并非成功连接）", loc="left", pad=12)
ax.spines[["top", "right", "left"]].set_visible(False)
ax.tick_params(axis="y", length=0)
fig.savefig(OUT / "acceptance-connections.png", bbox_inches="tight")
plt.close(fig)
print("Generated charts in", OUT)
