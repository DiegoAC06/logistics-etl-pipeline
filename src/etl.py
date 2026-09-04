"""
ETL: data\\raw\\*.csv  ->  data\\processed\\logistics.db

First pass at the pipeline. Reads the raw CSVs, cleans them up a bit,
adds the delivery-performance columns, and loads everything into SQLite.

    python src\\etl.py
"""

import logging
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
DB_PATH = ROOT / "data" / "processed" / "logistics.db"
SCHEMA_PATH = ROOT / "sql" / "schema.sql"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("etl")

TEXT_COLUMNS = {
    "warehouses": ["name", "region", "city", "state"],
    "orders": ["product_category"],
    "shipments": ["carrier", "delay_reason"],
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
    for name in ("warehouses", "orders", "shipments"):
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
        # Let pandas work out the date formats.
        for col in DATE_COLUMNS.get(name, []):
            df[col] = pd.to_datetime(df[col])

        before = len(df)
        df = df.drop_duplicates()
        log.info("  %-12s dropped %d duplicate rows -> %d",
                 name, before - len(df), len(df))

        for col in TEXT_COLUMNS[name]:
            df[col] = df[col].str.strip().str.title()

        frames[name] = df

    orders = frames["orders"]
    shipments = frames["shipments"]

    # Fill in what's missing.
    orders["product_category"] = orders["product_category"].fillna("Unknown")
    orders["customer_id"] = orders["customer_id"].fillna(0)
    shipments["carrier"] = shipments["carrier"].fillna("Unknown")
    log.info("  filled missing product_category, customer_id and carrier")

    # Delivery performance. Negative days_late means it arrived early.
    shipments["days_late"] = (
        shipments["actual_delivery_date"] - shipments["expected_delivery_date"]
    ).dt.days
    shipments["on_time"] = shipments["days_late"] <= 0
    log.info("  added days_late and on_time")

    for name, df in frames.items():
        log.info("  %-12s %5d rows after transform", name, len(df))

    return frames


# ==========================================================================
# LOAD
# ==========================================================================

def load(frames):
    log.info("LOAD")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
    log.info("  schema applied from %s", SCHEMA_PATH.name)

    for name in ("warehouses", "orders", "shipments"):
        frames[name].to_sql(name, conn, if_exists="append", index=False)
        log.info("  loaded %-12s %5d rows", name, len(frames[name]))

    conn.commit()
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
