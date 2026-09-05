"""
ETL: data\\raw\\*.csv  ->  data\\processed\\logistics.db

Reads the raw CSVs, cleans them up, adds the delivery-performance columns,
and loads everything into SQLite.

The load is a full refresh: the CSVs are the complete source of truth, so
each run clears the tables and reloads them inside a single transaction.
Running it any number of times leaves the database in the same state.

    python src\\etl.py
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "data" / "processed" / "logistics.db"
REJECTS_PATH = ROOT / "data" / "processed" / "rejected_rows.csv"
SCHEMA_PATH = ROOT / "sql" / "schema.sql"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

# Parents first. Deletes walk this backwards.
LOAD_ORDER = ("warehouses", "orders", "shipments")

TEXT_COLUMNS = {
    "warehouses": ["name", "region", "city", "state"],
    "orders": ["product_category"],
    "shipments": ["carrier"],
}

DATE_COLUMNS = {
    "orders": ["order_date"],
    "shipments": ["ship_date", "expected_delivery_date", "actual_delivery_date"],
}


# ==========================================================================
# EXTRACT
# ==========================================================================

def extract():
    log.info("EXTRACT")
    frames = {}
    for name in LOAD_ORDER:
        df = pd.read_csv(RAW_DIR / f"{name}.csv")
        frames[name] = df
        log.info("  read %-14s %5d rows", f"{name}.csv", len(df))
    return frames


# ==========================================================================
# TRANSFORM
# ==========================================================================

def transform(frames):
    log.info("TRANSFORM")

    for name, df in frames.items():
        # The files mix several date formats, so infer per value.
        for col in DATE_COLUMNS.get(name, []):
            df[col] = pd.to_datetime(df[col], format="mixed")

        before = len(df)
        df = df.drop_duplicates()
        log.info("  %-12s dropped %d duplicate rows -> %d",
                 name, before - len(df), len(df))

        for col in TEXT_COLUMNS[name]:
            df[col] = df[col].str.strip().str.title()

        frames[name] = df

    orders = frames["orders"]
    shipments = frames["shipments"]

    # delay_reason is a fixed vocabulary the schema checks against, so it
    # gets lowercased rather than title-cased.
    shipments["delay_reason"] = shipments["delay_reason"].str.strip().str.lower()

    # Fill in what's missing.
    orders["product_category"] = orders["product_category"].fillna("Unknown")
    orders["customer_id"] = orders["customer_id"].fillna(0)
    shipments["carrier"] = shipments["carrier"].fillna("Unknown")
    log.info("  filled missing product_category, customer_id and carrier")

    # Delivery performance. Negative days_late means it arrived early, so
    # on_time is days_late <= 0. Both stay unknown while a shipment is in
    # transit -- the schema rejects a row that knows one but not the other.
    days = (shipments["actual_delivery_date"]
            - shipments["expected_delivery_date"]).dt.days
    shipments["days_late"] = days
    shipments["on_time"] = (days <= 0).astype("boolean").where(days.notna(), pd.NA)
    log.info("  added days_late and on_time")

    for name, df in frames.items():
        log.info("  %-12s %5d rows after transform", name, len(df))

    return frames


# ==========================================================================
# LOAD
# ==========================================================================

def connect():
    """
    Open a connection with foreign keys on and transactions under our control.

    The pragma is per-connection and off by default, so schema.sql's foreign
    keys are inert on any connection that doesn't set it. It's also ignored
    inside a transaction, hence isolation_level=None (no implicit BEGIN) and
    the read-back -- better a loud failure than a check that isn't checking.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.isolation_level = None
    conn.execute("PRAGMA foreign_keys = ON")
    (enabled,) = conn.execute("PRAGMA foreign_keys").fetchone()
    if not enabled:
        raise RuntimeError(
            "PRAGMA foreign_keys did not take -- this build of SQLite was "
            "compiled with SQLITE_OMIT_FOREIGN_KEY")
    return conn


def ensure_schema(conn):
    """Build the tables the first time; leave them alone after that."""
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in LOAD_ORDER if t not in existing]
    if missing:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        log.info("  created schema from %s", SCHEMA_PATH.name)
    else:
        log.info("  schema already present, reusing it")


def row_counts(conn):
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in LOAD_ORDER}


def fmt_counts(counts):
    return "  ".join(f"{t}={n}" for t, n in counts.items())


def split_on_fk(child, child_col, parent_keys):
    """Split a child table into rows whose parent exists and rows whose doesn't."""
    orphaned = ~child[child_col].isin(parent_keys)
    return child[~orphaned].copy(), child[orphaned].copy()


def write_rejects(rejected):
    """
    Write every row that failed a foreign key check, with the reason.

    Always writes the file, even when nothing was rejected, so a stale copy
    from an earlier run can't be mistaken for this run's result.
    """
    parts = []
    for table, df, reason, col in rejected:
        if df.empty:
            continue
        out = df.copy()
        out.insert(0, "source_table", table)
        out.insert(1, "reason", reason)
        out.insert(2, "violating_value", f"{col}=" + df[col].astype(str))
        parts.append(out)

    combined = (pd.concat(parts, ignore_index=True) if parts
                else pd.DataFrame(columns=["source_table", "reason", "violating_value"]))
    combined.to_csv(REJECTS_PATH, index=False)

    if combined.empty:
        log.info("  0 rows rejected; wrote empty %s", REJECTS_PATH.name)
    else:
        log.warning("  %d rows rejected -> %s", len(combined), REJECTS_PATH.name)
        for (table, reason), n in combined.groupby(
                ["source_table", "reason"]).size().items():
            log.warning("      %5d  %s: %s", n, table, reason)
    return combined


def to_records(df):
    """
    DataFrame -> plain Python tuples for executemany.

    Dates go in as 'YYYY-MM-DD' strings: SQLite has no date type, and the
    schema's `x IS date(x)` checks reject the '2026-01-05 00:00:00' that
    a raw datetime would land as. Every flavour of NA becomes None.
    """
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].dt.strftime("%Y-%m-%d")

    out = out.astype(object).where(pd.notna(out), None)
    return [tuple(v.item() if hasattr(v, "item") else v for v in row)
            for row in out.itertuples(index=False, name=None)]


def insert_frame(conn, table, df):
    cols = list(df.columns)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) "
           f"VALUES ({', '.join('?' * len(cols))})")
    conn.executemany(sql, to_records(df))


def load(frames):
    log.info("LOAD")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    warehouses = frames["warehouses"]
    orders = frames["orders"]
    shipments = frames["shipments"]

    # Bulk inserts abort as a whole, with no way to tell which row was bad.
    # Find the orphans here, where the offending value can still be named.
    orders, bad_orders = split_on_fk(
        orders, "warehouse_id", set(warehouses["warehouse_id"]))

    # Checked against the orders that survived, not the ones we started with
    # -- a shipment whose order was just rejected is orphaned too.
    shipments, bad_shipments = split_on_fk(
        shipments, "order_id", set(orders["order_id"]))

    write_rejects([
        ("orders", bad_orders, "warehouse_id not found in warehouses", "warehouse_id"),
        ("shipments", bad_shipments, "order_id not found in orders", "order_id"),
    ])

    staged = {"warehouses": warehouses, "orders": orders, "shipments": shipments}

    conn = connect()
    try:
        ensure_schema(conn)

        before = row_counts(conn)
        log.info("  rows before load:  %s", fmt_counts(before))

        # One transaction around the whole clear-and-reload. A failure
        # anywhere rolls back to the previous contents rather than leaving
        # the database empty or half-filled.
        conn.execute("BEGIN")
        try:
            # Children first -- the parents are still referenced.
            for table in reversed(LOAD_ORDER):
                n = conn.execute(f"DELETE FROM {table}").rowcount
                log.info("  cleared  %-12s %5d rows", table, n)

            for table in LOAD_ORDER:
                insert_frame(conn, table, staged[table])
                log.info("  inserted %-12s %5d rows", table, len(staged[table]))

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            log.error("  load failed and was rolled back; database unchanged")
            raise

        after = row_counts(conn)
        log.info("  rows after load:   %s", fmt_counts(after))

        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        if violations:
            log.error("  %d foreign key violations after load", len(violations))
        else:
            log.info("  foreign key check clean")
    finally:
        conn.close()

    log.info("  database written to %s", DB_PATH)


# ==========================================================================

def main():
    frames = extract()
    frames = transform(frames)
    load(frames)
    log.info("done")


if __name__ == "__main__":
    main()
