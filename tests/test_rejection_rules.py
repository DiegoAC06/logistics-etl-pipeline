"""
Tests for the ETL's rejection rules.

None of these can fire on the generated data -- generate_data.py doesn't
produce non-numeric ids, conflicting primary keys, zero quantities or
backwards delivery windows. So each case here builds a small frame that
does, and checks the row lands in rejected_rows.csv with a reason instead
of aborting the insert.

Plain asserts, same style as test_schema_drift.py, no test framework.

    venv\\Scripts\\python.exe tests\\test_rejection_rules.py
"""

import sqlite3
import sys
import tempfile
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import etl  # noqa: E402

passed = failed = 0


def check(label, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"   PASS  {label}")
    else:
        failed += 1
        print(f"   FAIL  {label}  {detail}")


# --------------------------------------------------------------------------
# Frames shaped like the real ones, small enough to reason about.
# --------------------------------------------------------------------------

def warehouses(rows=None):
    return pd.DataFrame(rows or [("1", "Alpha DC", "Northeast", "Newark", "NJ")],
                        columns=["warehouse_id", "name", "region", "city", "state"])


def orders(rows):
    return pd.DataFrame(rows, columns=["order_id", "warehouse_id", "order_date",
                                       "customer_id", "quantity", "product_category"])


def shipments(rows):
    return pd.DataFrame(rows, columns=["shipment_id", "order_id", "carrier",
                                       "ship_date", "expected_delivery_date",
                                       "actual_delivery_date", "delay_reason"])


def run_pipeline(tmp, frames):
    """Transform + load the given frames; return (rejects_df, db_path)."""
    etl.DB_PATH = tmp / "t.db"
    etl.REJECTS_PATH = tmp / "rejects.csv"
    etl.DB_PATH.unlink(missing_ok=True)
    rejects = etl.Rejects()
    out = etl.transform({k: v.copy() for k, v in frames.items()}, rejects)
    etl.load(out, rejects)
    return pd.read_csv(etl.REJECTS_PATH), etl.DB_PATH


def loaded(db, table, col):
    c = sqlite3.connect(db)
    vals = [r[0] for r in c.execute(f"SELECT {col} FROM {table} ORDER BY 1")]
    c.close()
    return vals


def run():
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)

        # ------------------------------------------------------------------
        print("\n1. a non-numeric primary key is rejected, not auto-assigned")
        # Without the check, 'abc' coerces to NaN, inserts as NULL, and
        # SQLite hands the row a rowid of its own choosing.
        frames = {
            "warehouses": warehouses(),
            "orders": orders([
                ("10", "1", "2026-03-01", "500", "2", "toys"),
                ("abc", "1", "2026-03-02", "501", "1", "toys"),   # bad order_id
            ]),
            "shipments": shipments([
                ("20", "10", "UPS", "2026-03-02", "2026-03-05", "2026-03-05", ""),
            ]),
        }
        rej, db = run_pipeline(tmp, frames)
        reasons = set(rej["reason"])
        check("rejected with the numeric reason",
              "required numeric column unparseable" in reasons, reasons)
        check("detail names the offending column",
              any("order_id" in str(d) for d in rej["detail"]), list(rej["detail"]))
        ids = loaded(db, "orders", "order_id")
        check("only the good order loaded", ids == [10], ids)
        check("no fabricated rowid appeared", len(ids) == 1, ids)

        print("\n   1b. a non-numeric quantity is rejected too")
        frames["orders"] = orders([
            ("10", "1", "2026-03-01", "500", "2", "toys"),
            ("11", "1", "2026-03-02", "501", "lots", "toys"),      # bad quantity
        ])
        rej, db = run_pipeline(tmp, frames)
        check("rejected", "required numeric column unparseable" in set(rej["reason"]))
        check("only the good order loaded", loaded(db, "orders", "order_id") == [10])

        print("\n   1c. a non-numeric customer_id is NOT rejected (it has a sentinel)")
        frames["orders"] = orders([
            ("10", "1", "2026-03-01", "oops", "2", "toys"),
        ])
        rej, db = run_pipeline(tmp, frames)
        check("no rejection", len(rej) == 0, list(rej.get("reason", [])))
        cust = loaded(db, "orders", "customer_id")
        check("filled with the sentinel instead",
              cust == [etl.UNKNOWN_CUSTOMER_ID], cust)

        # ------------------------------------------------------------------
        print("\n2. two rows sharing a primary key, first wins")
        frames = {
            "warehouses": warehouses(),
            "orders": orders([
                ("10", "1", "2026-03-01", "500", "2", "toys"),
                ("10", "1", "2026-03-01", "999", "7", "grocery"),   # same id, differs
                ("11", "1", "2026-03-03", "502", "1", "toys"),
            ]),
            "shipments": shipments([
                ("20", "10", "UPS", "2026-03-02", "2026-03-05", "2026-03-05", ""),
            ]),
        }
        rej, db = run_pipeline(tmp, frames)
        check("rejected with the key-conflict reason",
              "duplicate primary key with conflicting values" in set(rej["reason"]),
              set(rej["reason"]))
        check("exactly one row rejected",
              (rej["reason"] == "duplicate primary key with conflicting values").sum() == 1)
        c = sqlite3.connect(db)
        kept = c.execute("SELECT customer_id, quantity FROM orders WHERE order_id=10").fetchall()
        c.close()
        check("the FIRST row is the one kept", kept == [(500, 2)], kept)
        check("the other order is untouched", 11 in loaded(db, "orders", "order_id"))
        check("its shipment still loads (key survived, so no orphan)",
              loaded(db, "shipments", "shipment_id") == [20])

        print("\n   2b. identical duplicates are deduped, not rejected")
        frames["orders"] = orders([
            ("10", "1", "2026-03-01", "500", "2", "toys"),
            ("10", "1", "2026-03-01", "500", "2", "toys"),        # byte-identical
        ])
        rej, db = run_pipeline(tmp, frames)
        check("nothing rejected", len(rej) == 0, list(rej.get("reason", [])))
        check("one row loaded", loaded(db, "orders", "order_id") == [10])

        # ------------------------------------------------------------------
        print("\n4. quantity must be positive")
        for label, qty in (("zero", "0"), ("negative", "-3")):
            frames = {
                "warehouses": warehouses(),
                "orders": orders([
                    ("10", "1", "2026-03-01", "500", "2", "toys"),
                    ("11", "1", "2026-03-02", "501", qty, "toys"),
                ]),
                "shipments": shipments([]),
            }
            rej, db = run_pipeline(tmp, frames)
            check(f"{label} quantity rejected",
                  "quantity is not positive" in set(rej["reason"]), set(rej["reason"]))
            check(f"{label}: only the good order loaded",
                  loaded(db, "orders", "order_id") == [10])

        print("\n   4b. expected_delivery_date before ship_date")
        frames = {
            "warehouses": warehouses(),
            "orders": orders([("10", "1", "2026-03-01", "500", "2", "toys")]),
            "shipments": shipments([
                ("20", "10", "UPS", "2026-03-02", "2026-03-05", "2026-03-05", ""),
                ("21", "10", "UPS", "2026-03-10", "2026-03-04", "", ""),   # backwards
            ]),
        }
        rej, db = run_pipeline(tmp, frames)
        check("rejected with the timeline reason",
              "expected_delivery_date before ship_date" in set(rej["reason"]),
              set(rej["reason"]))
        check("detail shows both dates",
              any("expected 2026-03-04" in str(d) and "ship 2026-03-10" in str(d)
                  for d in rej["detail"]), list(rej["detail"]))
        check("only the good shipment loaded",
              loaded(db, "shipments", "shipment_id") == [20])

        # ------------------------------------------------------------------
        print("\n5. every rejected row still reaches the CSV with its columns")
        frames = {
            "warehouses": warehouses(),
            "orders": orders([
                ("10", "1", "2026-03-01", "500", "2", "toys"),
                ("abc", "1", "2026-03-02", "501", "1", "toys"),
                ("12", "1", "2026-03-03", "502", "0", "toys"),
                ("13", "1", "2026-03-04", "503", "1", "toys"),
                ("13", "1", "2026-03-04", "999", "9", "grocery"),
            ]),
            "shipments": shipments([]),
        }
        rej, db = run_pipeline(tmp, frames)
        print(rej[["source_table", "reason", "detail"]].to_string(index=False))
        check("three distinct reasons present", len(set(rej["reason"])) == 3, set(rej["reason"]))
        check("original columns preserved on rejected rows",
              {"order_id", "warehouse_id", "quantity"} <= set(rej.columns), list(rej.columns))
        check("survivors loaded", loaded(db, "orders", "order_id") == [10, 13])
        check("nothing silently vanished", len(rej) + 2 == 5, len(rej))


if __name__ == "__main__":
    print("=" * 62)
    print("  ETL rejection rules")
    print("=" * 62)
    run()
    print("\n" + "=" * 62)
    print(f"  {passed} passed, {failed} failed")
    print("=" * 62)
    raise SystemExit(1 if failed else 0)
