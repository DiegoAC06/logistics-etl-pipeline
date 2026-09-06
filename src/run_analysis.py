"""
Run the queries in sql\\analysis.sql and print the results.

    python src\\run_analysis.py              -- everything
    python src\\run_analysis.py carrier      -- only queries whose name matches
    python src\\run_analysis.py --list       -- just the names

Queries live in the .sql file, not in here, so they can be run by hand in
any SQLite client. This script only splits, executes and formats.
"""

import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "processed" / "logistics.db"
SQL_PATH = ROOT / "sql" / "analysis.sql"

BAR_WIDTH = 22


# ==========================================================================
# Reading the .sql file
# ==========================================================================

def load_queries(path):
    """
    Split analysis.sql into its named blocks.

    A block starts at a `-- name:` line and runs to the next one. `-- title:`
    and `-- question:` lines before the SQL become the printed header; any
    other comment stays with the SQL.
    """
    queries = []
    current = None

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()

        if stripped.startswith("-- name:"):
            if current:
                queries.append(current)
            current = {"name": stripped.split(":", 1)[1].strip(),
                       "title": "", "question": [], "sql": []}
        elif current is None:
            continue                                   # file header, skip
        elif stripped.startswith("-- title:"):
            current["title"] = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("-- question:"):
            current["question"].append(stripped.split(":", 1)[1].strip())
        elif current["question"] and stripped.startswith("--") and not current["sql"]:
            current["question"].append(stripped[2:].strip())   # continuation
        else:
            current["sql"].append(line)

    if current:
        queries.append(current)

    for q in queries:
        q["sql"] = "\n".join(q["sql"]).strip().rstrip(";")
        q["question"] = " ".join(q["question"])
    return queries


# ==========================================================================
# Formatting
# ==========================================================================

def fmt_value(v):
    if v is None:
        return "-"
    if isinstance(v, bool):
        return "yes" if v else "no"
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}".rstrip("0").rstrip(".")
    return str(v)


def is_numeric(values):
    seen = [v for v in values if v is not None]
    return bool(seen) and all(isinstance(v, (int, float)) and not isinstance(v, bool)
                              for v in seen)


def bar(pct):
    if pct is None:
        return ""
    filled = max(0, min(BAR_WIDTH, round(BAR_WIDTH * pct / 100)))
    return "#" * filled


def render(headers, rows):
    """Plain text table. Numbers right-aligned, text left."""
    if not rows:
        return "    (no rows)"

    # A bar next to the first percentage column makes rankings scannable.
    pct_idx = next((i for i, h in enumerate(headers) if h.endswith("_pct")), None)
    bar_idx = None
    if pct_idx is not None:
        bar_idx = pct_idx + 1
        headers = headers[:bar_idx] + [""] + headers[bar_idx:]
        rows = [r[:bar_idx] + (bar(r[pct_idx]),) + r[bar_idx:] for r in rows]

    numeric = [is_numeric([r[i] for r in rows]) for i in range(len(headers))]
    cells = [[fmt_value(v) for v in row] for row in rows]
    widths = [max(len(h), *(len(row[i]) for row in cells))
              for i, h in enumerate(headers)]

    def line(values, pad=" "):
        return "    " + "  ".join(
            v.rjust(w) if numeric[i] else v.ljust(w)
            for i, (v, w) in enumerate(zip(values, widths))).rstrip()

    # The bar column has no header, so it gets no rule either.
    rule = "  ".join((" " if i == bar_idx else "-") * w
                     for i, w in enumerate(widths))
    out = [line(headers), ("    " + rule).rstrip()]
    out += [line(c) for c in cells]
    return "\n".join(out)


def wrap(text, width=72, indent="    "):
    words, lines, line = text.split(), [], ""
    for w in words:
        if len(line) + len(w) + 1 > width:
            lines.append(line)
            line = w
        else:
            line = f"{line} {w}".strip()
    if line:
        lines.append(line)
    return "\n".join(indent + l for l in lines)


# ==========================================================================

def main():
    args = [a for a in sys.argv[1:]]

    if not DB_PATH.exists():
        print(f"No database at {DB_PATH}")
        print("Build it first:  python src\\etl.py")
        raise SystemExit(1)

    queries = load_queries(SQL_PATH)

    if "--list" in args:
        for q in queries:
            print(f"  {q['name']:<32} {q['title']}")
        return

    if args:
        needle = args[0].lower()
        queries = [q for q in queries
                   if needle in q["name"].lower() or needle in q["title"].lower()]
        if not queries:
            print(f"Nothing matches {args[0]!r}. Try --list.")
            raise SystemExit(1)

    conn = sqlite3.connect(DB_PATH)
    try:
        total = conn.execute("SELECT count(*) FROM shipments").fetchone()[0]
        print()
        print("=" * 78)
        print(f"  Logistics analysis  --  {total:,} shipments in {DB_PATH.name}")
        print("=" * 78)

        for i, q in enumerate(queries, start=1):
            print(f"\n[{i}] {q['title'] or q['name']}")
            if q["question"]:
                print(wrap(q["question"]))
            print()
            cur = conn.execute(q["sql"])
            headers = [d[0] for d in cur.description]
            print(render(headers, cur.fetchall()))
        print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
