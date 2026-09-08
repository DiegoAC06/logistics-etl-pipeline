"""
Charts from the analysis queries, saved as PNGs in output\\.

    python src\\visualize.py

The SQL lives in sql\\analysis.sql and is read from there, not duplicated
here -- a chart that disagrees with the report it illustrates is worse than
no chart. This file only draws.
"""

import sqlite3
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")          # writing files, no window to open
import matplotlib.pyplot as plt   # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_analysis import load_queries   # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "processed" / "logistics.db"
SQL_PATH = ROOT / "sql" / "analysis.sql"
OUT_DIR = ROOT / "output"

INK = "#1b2430"        # text and axis lines
BAR = "#3d6fb4"        # ordinary bars and lines
MUTED = "#9aa5b1"      # 'unknown' and other not-really-data categories
ACCENT = "#c2543d"     # reference lines and warnings
FAINT = "#dfe4ea"      # background volume bars

DPI = 150


# ==========================================================================
# Pulling data
# ==========================================================================

def fetch(conn, queries, name):
    """Run one named query from analysis.sql, as a list of dicts."""
    query = queries.get(name)
    if query is None:
        raise SystemExit(
            f"{SQL_PATH.name} has no query named {name!r}. "
            f"Available: {', '.join(sorted(queries))}")
    cur = conn.execute(query["sql"])
    cols = [d[0] for d in cur.description]
    return query, [dict(zip(cols, row)) for row in cur.fetchall()]


def titles(ax, title, subtitle):
    """
    Title above a smaller explanatory line.

    The pad has to clear the subtitle -- both are anchored to the top of the
    axes, so too small a pad prints one on top of the other.
    """
    ax.set_title(title, fontsize=13, color=INK, pad=32, loc="left")
    ax.text(0, 1.015, subtitle, transform=ax.transAxes,
            fontsize=9, color="#5b6672", va="bottom")


def style(ax, grid_axis="y"):
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color("#c8cdd4")
    ax.tick_params(colors=INK, labelsize=9)
    ax.grid(axis=grid_axis, color=FAINT, linewidth=0.9)
    ax.set_axisbelow(True)


def save(fig, name):
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / name
    fig.savefig(path, dpi=DPI, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"   wrote {path.relative_to(ROOT)}  ({path.stat().st_size / 1024:.0f} KB)")
    return path


# ==========================================================================
# 1. On-time rate by warehouse
# ==========================================================================

def chart_warehouse_rates(conn, queries):
    _, rows = fetch(conn, queries, "on_time_by_warehouse")
    # A warehouse with nothing delivered has no rate to plot; the table
    # reports it as 'no data' and it simply has no bar here.
    rows = [r for r in rows if r["on_time_pct"] is not None]
    rows.sort(key=lambda r: r["on_time_pct"], reverse=True)

    names = [r["warehouse"] for r in rows]
    rates = [r["on_time_pct"] for r in rows]
    delivered = [r["delivered"] for r in rows]

    # Weighted by volume, so it's the network's real rate rather than the
    # average of eight percentages.
    network = sum(p * d for p, d in zip(rates, delivered)) / sum(delivered)

    fig, ax = plt.subplots(figsize=(10, 5.6))
    bars = ax.bar(names, rates, color=BAR, width=0.62)

    # Anything under the network rate is what "dragging" actually means.
    for bar_, rate in zip(bars, rates):
        if rate < network:
            bar_.set_color(ACCENT)

    ax.axhline(network, color=INK, linestyle="--", linewidth=1.1, zorder=3)
    ax.annotate(f"network average  {network:.1f}%",
                xy=(len(names) - 0.4, network), xytext=(0, 6),
                textcoords="offset points", ha="right", va="bottom",
                fontsize=9, color=INK)

    for bar_, rate, n in zip(bars, rates, delivered):
        ax.annotate(f"{rate:.1f}%", xy=(bar_.get_x() + bar_.get_width() / 2, rate),
                    xytext=(0, 4), textcoords="offset points",
                    ha="center", fontsize=9, color=INK)
        ax.annotate(f"n={n:,}", xy=(bar_.get_x() + bar_.get_width() / 2, 2),
                    ha="center", fontsize=8, color="white")

    ax.set_ylim(0, 100)
    ax.set_ylabel("On-time delivery rate (% of delivered shipments)")
    titles(ax, "On-time delivery rate by warehouse",
           "Bars below the network average in red. n = shipments delivered.")
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    style(ax)
    return save(fig, "on_time_by_warehouse.png")


# ==========================================================================
# 2. Monthly on-time trend
# ==========================================================================

def chart_monthly_trend(conn, queries):
    _, rows = fetch(conn, queries, "monthly_trend")
    months = [r["month"] for r in rows]
    rates = [r["on_time_pct"] for r in rows]
    orders = [r["orders"] for r in rows]
    in_transit = [r["in_transit"] for r in rows]

    fig, ax = plt.subplots(figsize=(10, 5.6))

    # Order volume behind the line: the dip and the peak are the same story,
    # and the rate alone doesn't show why it moves.
    volume = ax.twinx()
    volume.bar(months, orders, color=FAINT, width=0.6, zorder=1)
    volume.set_ylabel("Orders placed", color="#8a939e", fontsize=10)
    volume.tick_params(colors="#8a939e", labelsize=9)
    # Cap the tallest bar at 22% of the plot height. The rate line bottoms
    # out around 73% on a 60-100 axis, so anything taller collides with it.
    volume.set_ylim(0, max(orders) / 0.22)
    for side in ("top", "left"):
        volume.spines[side].set_visible(False)
    volume.spines["right"].set_color("#c8cdd4")

    ax.set_zorder(volume.get_zorder() + 1)
    ax.patch.set_visible(False)
    ax.plot(months, rates, color=BAR, linewidth=2.2, marker="o",
            markersize=5, zorder=4)

    for x, rate in zip(months, rates):
        ax.annotate(f"{rate:.0f}%", xy=(x, rate), xytext=(0, 8),
                    textcoords="offset points", ha="center",
                    fontsize=8.5, color=INK)

    # The last month is still filling in, so its rate covers fewer shipments.
    if in_transit and in_transit[-1] > 0:
        ax.annotate(f"{in_transit[-1]} shipments still in transit,\n"
                    f"so this month is incomplete",
                    xy=(len(months) - 1, rates[-1]),
                    xytext=(-12, -46), textcoords="offset points",
                    ha="right", fontsize=8.5, color=ACCENT,
                    arrowprops=dict(arrowstyle="->", color=ACCENT, linewidth=1))

    ax.set_ylim(60, 100)
    ax.set_ylabel("On-time rate (% of delivered shipments)")
    ax.set_xlabel("Month of order")
    titles(ax, "On-time rate falls as volume rises",
           "Line: on-time rate. Grey bars: order volume, right axis.")
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    style(ax)
    return save(fig, "monthly_on_time_trend.png")


# ==========================================================================
# 3. Delay reasons by frequency
# ==========================================================================

def chart_delay_reasons(conn, queries):
    _, rows = fetch(conn, queries, "delay_reason_frequency")
    # barh draws the first row at the bottom, so ascending puts the biggest
    # bar at the top where it's read first.
    rows.sort(key=lambda r: r["shipments"])

    labels = [r["delay_reason"].replace("_", " ") for r in rows]
    counts = [r["shipments"] for r in rows]
    shares = [r["share_pct"] for r in rows]
    # 'unknown' is a gap in the data, not a cause, so it isn't coloured
    # like one.
    colors = [MUTED if r["delay_reason"] == "unknown" else BAR for r in rows]

    fig, ax = plt.subplots(figsize=(10, 5.2))
    bars = ax.barh(labels, counts, color=colors, height=0.62)

    for bar_, n, share in zip(bars, counts, shares):
        ax.annotate(f"{n:,}   ({share:.1f}%)",
                    xy=(bar_.get_width(), bar_.get_y() + bar_.get_height() / 2),
                    xytext=(6, 0), textcoords="offset points",
                    va="center", fontsize=9, color=INK)

    total = sum(counts)
    ax.set_xlim(0, max(counts) * 1.22)
    ax.set_xlabel("Late shipments")
    titles(ax, "Why shipments arrive late",
           f"All {total:,} late shipments, so the shares total 100%. "
           f"Grey 'unknown' = no cause recorded.")
    style(ax, grid_axis="x")
    return save(fig, "delay_reasons.png")


# ==========================================================================

def main():
    if not DB_PATH.exists():
        print(f"No database at {DB_PATH}")
        print("Build it first:  venv\\Scripts\\python.exe src\\etl.py")
        raise SystemExit(1)

    # load_queries returns them in file order; index by name to pick three.
    queries = {q["name"]: q for q in load_queries(SQL_PATH)}
    conn = sqlite3.connect(DB_PATH)
    try:
        print(f"\nCharts from {SQL_PATH.name} -> {OUT_DIR.name}\\")
        chart_warehouse_rates(conn, queries)
        chart_monthly_trend(conn, queries)
        chart_delay_reasons(conn, queries)
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
