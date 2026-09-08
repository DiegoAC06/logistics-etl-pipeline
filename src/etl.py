"""
ETL: data\\raw\\*.csv  ->  data\\processed\\logistics.db

Reads the raw CSVs, cleans them up, adds the delivery-performance columns,
and loads everything into SQLite.

The load is a full refresh: the CSVs are the complete source of truth, so
each run clears the tables and reloads them inside a single transaction.
Running it any number of times leaves the database in the same state.

Rows the pipeline refuses -- bad dates, impossible timelines, broken
foreign keys -- go to data\\processed\\rejected_rows.csv with a reason
rather than being dropped quietly.

    python src\\etl.py
"""

import hashlib
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

# Casing is a per-column decision, not a per-table one. 'AZ' is a code,
# 'Newark DC' is a name, 'home_goods' is a key and 'Northeast' is a word --
# one .str.title() over all four mangles three of them.
#
#   asis   trim only, leave the characters alone
#   title  ordinary words
#   upper  short codes
#   lower  controlled vocabularies and keys
#
# carrier isn't here: no casing rule produces UPS, FedEx and OnTrac, so it
# goes through CARRIER_NAMES instead.
TEXT_RULES = {
    "warehouses": {
        "name": "asis",     # proper nouns from the source; "DC" is not "Dc"
        "region": "title",  # words, and the source really does hold 'NORTHEAST'
        "city": "title",
        "state": "upper",   # two-letter postal codes
    },
    "orders": {
        "product_category": "lower",   # 'home_goods' is a key, not a label
    },
    "shipments": {
        "delay_reason": "lower",       # the schema's CHECK expects lowercase
    },
}

# Carrier names are brands, not sentences: .str.title() turns UPS into 'Ups'
# and FedEx into 'Fedex'. No casing rule gets these right, so the cleaned
# lowercase form is looked up instead. Keys must be lowercase and trimmed.
CARRIER_NAMES = {
    "ups": "UPS",
    "usps": "USPS",
    "fedex": "FedEx",
    "dhl": "DHL",
    "ontrac": "OnTrac",
}

NUMERIC_COLUMNS = {
    "warehouses": ["warehouse_id"],
    "orders": ["order_id", "warehouse_id", "customer_id", "quantity"],
    "shipments": ["shipment_id", "order_id"],
}

DATE_COLUMNS = {
    "orders": ["order_date"],
    "shipments": ["ship_date", "expected_delivery_date", "actual_delivery_date"],
}

# Sentinels for values the source left blank. Real customer_ids run
# 1000-2199, so 0 can't collide with one. These rows are still real demand
# and are kept rather than dropped -- dropping an order would orphan its
# shipment -- but they are NOT anonymous customers, so filter customer_id
# on this value before counting distinct customers.
UNKNOWN_CUSTOMER_ID = 0
# Each sentinel matches its own column's convention: carriers are brand
# names, categories are lowercase keys.
#
# COUPLED TO sql\analysis.sql: query 6 filters on the literal 'Unknown' and
# query 4 sorts on it. Change this value and those queries stop matching --
# silently, since a filter that matches nothing is not an error.
UNKNOWN_CARRIER = "Unknown"
UNKNOWN_CATEGORY = "unknown"

FILL_VALUES = {
    "warehouses": {},
    "orders": {"product_category": UNKNOWN_CATEGORY,
               "customer_id": UNKNOWN_CUSTOMER_ID},
    "shipments": {"carrier": UNKNOWN_CARRIER},
}

# Not in FILL_VALUES: this one depends on on_time, which doesn't exist until
# after the derived columns are built. Filled at the end of transform().
UNKNOWN_DELAY_REASON = "unknown"

# The four formats actually present in the raw files, confirmed by counting
# shapes across all 20,000 date values. Tried in this order; first hit wins.
#
# %m/%d/%Y is stated, not inferred. 40% of the slash-formatted values have
# both components <= 12, so nothing in those values says which half is the
# month -- only the other 60% (day > 12) settles it for the column. Leaving
# that to inference means a coin flip on 425 rows.
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y"]

# Columns the schema declares NOT NULL. A row missing one can't be loaded.
REQUIRED_DATES = {
    "orders": ["order_date"],
    "shipments": ["ship_date", "expected_delivery_date"],
}

# Numeric columns that must actually be numbers. customer_id is absent on
# purpose -- it has a sentinel, so an unparseable one becomes UNKNOWN_CUSTOMER_ID
# rather than a rejected row.
REQUIRED_NUMERICS = {
    "warehouses": ["warehouse_id"],
    "orders": ["order_id", "warehouse_id", "quantity"],
    "shipments": ["shipment_id", "order_id"],
}

PRIMARY_KEYS = {
    "warehouses": "warehouse_id",
    "orders": "order_id",
    "shipments": "shipment_id",
}


# ==========================================================================
# Rejected rows
# ==========================================================================

class Rejects:
    """
    Collects every row the pipeline refuses, for one CSV at the end.

    Written even when empty, so a stale file from an earlier run can't be
    mistaken for this run's result.
    """

    def __init__(self):
        self._parts = []

    def add(self, table, df, reason, detail=""):
        """detail may be a plain string or a Series aligned to df."""
        if df.empty:
            return
        out = df.copy()
        out.insert(0, "source_table", table)
        out.insert(1, "reason", reason)
        out.insert(2, "detail", detail)
        self._parts.append(out)
        log.warning("  rejected %d %s rows: %s", len(out), table, reason)

    def write(self):
        combined = (pd.concat(self._parts, ignore_index=True) if self._parts
                    else pd.DataFrame(columns=["source_table", "reason", "detail"]))
        combined.to_csv(REJECTS_PATH, index=False)

        if combined.empty:
            log.info("  0 rows rejected; wrote empty %s", REJECTS_PATH.name)
        else:
            log.warning("  %d rows rejected in total -> %s",
                        len(combined), REJECTS_PATH.name)
            for (table, reason), n in combined.groupby(
                    ["source_table", "reason"]).size().items():
                log.warning("      %5d  %s: %s", n, table, reason)
        return combined


# ==========================================================================
# EXTRACT
# ==========================================================================

def extract():
    log.info("EXTRACT")
    frames = {}
    for name in LOAD_ORDER:
        df = pd.read_csv(RAW_DIR / f"{name}.csv", dtype=str)
        frames[name] = df
        log.info("  read %-14s %5d rows", f"{name}.csv", len(df))
    return frames


# ==========================================================================
# TRANSFORM
#
# Step order matters. Everything that can change a value runs before
# drop_duplicates, because dedup compares whole rows: two rows that mean
# the same thing only match once they look the same. Anything that removes
# rows runs after, so the counts below stay readable.
# ==========================================================================

def log_step(table, label, before, after):
    delta = after - before
    log.info("  %-11s %-26s %5d -> %-5d %s", table, label, before, after,
             f"({delta:+d})" if delta else "")


def parse_dates(series, label):
    """
    Parse a date column by trying each known format explicitly.

    Each format is applied only to what's still unparsed, so a column mixing
    ISO with '03-Aug-2026' comes out whole. errors='coerce' means a value
    matching no format becomes NaT and gets counted, rather than killing the
    run on row 4,000.

    Returns (parsed, n_failed) -- n_failed counts values that were present
    but unparseable, not values that were blank to begin with.
    """
    raw = series.astype("string").str.strip().replace("", pd.NA)
    parsed = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    hits = {}
    for fmt in DATE_FORMATS:
        todo = parsed.isna() & raw.notna()
        if not todo.any():
            break
        attempt = pd.to_datetime(raw[todo], format=fmt, errors="coerce")
        parsed.loc[todo] = attempt
        n = int(attempt.notna().sum())
        if n:
            hits[fmt] = n

    blank = int(raw.isna().sum())
    failed = int((raw.notna() & parsed.isna()).sum())

    log.debug("    %-30s %s", label,
              "  ".join(f"{f}={n}" for f, n in hits.items()) or "(none parsed)")
    if blank:
        log.debug("    %-30s %d blank", label, blank)
    if failed:
        log.warning("    %-30s %d FAILED TO PARSE -> NaT", label, failed)

    return parsed, failed


def parse_date_columns(df, table):
    """Turn every date column into real datetimes. Normalizes format, so it
    must run before dedup -- '03/19/2026' and '2026-03-19' are the same day
    but different strings."""
    out = df.copy()
    failed = 0
    for col in DATE_COLUMNS.get(table, []):
        out[col], n = parse_dates(out[col], f"{table}.{col}")
        failed += n
    return out, failed


def tidy(series):
    """Collapse whitespace runs and tabs, trim, blanks to NA."""
    return (series.str.replace(r"\s+", " ", regex=True)
                  .str.strip()
                  .replace("", pd.NA))


def canonical_carrier(series):
    """
    Map every spelling of a carrier onto its real brand name.

    A lookup rather than a casing rule, because no rule produces UPS, FedEx
    and OnTrac from the same input. Anything not in the table keeps a
    title-cased version of itself -- an unrecognised carrier should look
    odd in the output, not vanish into the 'Unknown' sentinel.
    """
    cleaned = tidy(series).str.lower()
    mapped = cleaned.map(CARRIER_NAMES).astype("string")

    unmapped = cleaned.notna() & mapped.isna()
    if unmapped.any():
        names = sorted(cleaned[unmapped].unique())
        log.warning("  carrier: %d rows in %d unrecognized spellings kept as-is: %s",
                    int(unmapped.sum()), len(names), ", ".join(names[:5]))
        mapped = mapped.where(~unmapped, cleaned.str.title())
    return mapped


def normalize_text(df, table):
    """
    Trim, collapse internal runs of whitespace and tabs, and fold case.

    This is the step the dedup depends on: ' FEDEX' and 'FedEx ' are one
    carrier, and rows differing only by that should collapse together.

    Casing follows TEXT_RULES, one rule per column; carrier gets the brand
    lookup. Nothing runs after these, so nothing re-mangles them.
    """
    out = df.copy()
    for col, rule in TEXT_RULES[table].items():
        cleaned = tidy(out[col])
        if rule == "title":
            cleaned = cleaned.str.title()
        elif rule == "upper":
            cleaned = cleaned.str.upper()
        elif rule == "lower":
            cleaned = cleaned.str.lower()
        elif rule != "asis":
            raise ValueError(f"unknown casing rule {rule!r} for {table}.{col}")
        out[col] = cleaned

    if table == "shipments":
        out["carrier"] = canonical_carrier(out["carrier"])
    return out


def coerce_numerics(df, table):
    """'0007' and '7' are the same id; make them the same value."""
    out = df.copy()
    for col in NUMERIC_COLUMNS[table]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out


def fill_missing(df, table):
    out = df.copy()
    for col, value in FILL_VALUES[table].items():
        out[col] = out[col].fillna(value)
    return out


def key_text(series):
    """
    Render an id for a reject reason.

    to_numeric gives float64 as soon as one value in the column is NaN, so
    a plain astype(str) writes 'order_id=13.0' into the reason column.
    """
    # astype at the end matters: map over an empty selection keeps the
    # numeric dtype, and "id=" + <empty int64 series> raises. These details
    # are built before rejects.add sees the frame is empty, so the common
    # case -- nothing rejected -- goes through here too.
    return series.map(
        lambda v: "" if pd.isna(v)
        else str(int(v)) if float(v).is_integer() else str(v)
    ).astype("string")


def validate_numerics(frames, rejects):
    """
    Reject rows whose required numeric columns didn't parse.

    coerce_numerics uses errors='coerce', so 'abc' in order_id becomes NaN
    and then None on insert. SQLite reads NULL in an INTEGER PRIMARY KEY as
    "pick a rowid for me", so the row would load under a fabricated key
    instead of failing -- silent corruption rather than a loud abort.

    customer_id is deliberately not required: it has a sentinel, and
    fill_missing turns its NaN into UNKNOWN_CUSTOMER_ID a step later.
    """
    for name in LOAD_ORDER:
        df = frames[name]
        before = len(df)
        cols = REQUIRED_NUMERICS[name]
        bad = df[cols].isna().any(axis=1)
        if bad.any():
            which = df.loc[bad, cols].isna().apply(
                lambda r: "not numeric: " + ", ".join(r.index[r]), axis=1)
            rejects.add(name, df[bad], "required numeric column unparseable", which)
            df = df[~bad].copy()
        log_step(name, "reject unparseable numbers", before, len(df))
        frames[name] = df
    return frames


def validate_primary_keys(frames, rejects):
    """
    Reject rows that repeat a primary key with different values.

    drop_duplicates has already removed identical rows, so anything left
    sharing a key genuinely disagrees with its twin. Both would insert and
    the second would abort the transaction with "UNIQUE constraint failed"
    naming no row at all.

    THE FIRST ROW WINS. Source order is the only ordering available -- there
    is no updated_at to prefer -- so first-seen is at least deterministic.
    Rejecting every copy instead would be more cautious about which one is
    true, but it would also delete the key entirely and orphan any child
    rows pointing at it, turning one bad row into several.
    """
    for name, key in PRIMARY_KEYS.items():
        df = frames[name]
        before = len(df)
        conflicting = df.duplicated(subset=[key], keep="first")
        if conflicting.any():
            detail = (f"{key}=" + key_text(df.loc[conflicting, key])
                      + " already seen with different values")
            rejects.add(name, df[conflicting],
                        "duplicate primary key with conflicting values", detail)
            df = df[~conflicting].copy()
        log_step(name, "reject conflicting keys", before, len(df))
        frames[name] = df
    return frames


def validate_ranges(frames, rejects):
    """
    Reject rows that break the schema's CHECK constraints on values.

    Both rules are already declared in schema.sql. Catching them here is the
    difference between a labelled row in rejected_rows.csv and a transaction
    that aborts naming a constraint but no row.
    """
    orders = frames["orders"]
    shipments = frames["shipments"]

    before = len(orders)
    bad = orders["quantity"] <= 0
    if bad.any():
        rejects.add("orders", orders[bad], "quantity is not positive",
                    "quantity=" + key_text(orders.loc[bad, "quantity"]))
        orders = orders[~bad].copy()
    log_step("orders", "reject non-positive qty", before, len(orders))

    before = len(shipments)
    bad = shipments["expected_delivery_date"] < shipments["ship_date"]
    if bad.any():
        detail = ("expected "
                  + shipments.loc[bad, "expected_delivery_date"].dt.strftime("%Y-%m-%d")
                  + " < ship "
                  + shipments.loc[bad, "ship_date"].dt.strftime("%Y-%m-%d"))
        rejects.add("shipments", shipments[bad],
                    "expected_delivery_date before ship_date", detail)
        shipments = shipments[~bad].copy()
    log_step("shipments", "reject expected<ship", before, len(shipments))

    frames["orders"] = orders
    frames["shipments"] = shipments
    return frames


def validate_dates(frames, rejects):
    """
    Pull out rows whose dates can't be trusted.

    Two rules: a required date that didn't parse, and a delivery that
    happened before its own shipment.
    """
    orders = frames["orders"]
    shipments = frames["shipments"]

    for name, df in (("orders", orders), ("shipments", shipments)):
        before = len(df)
        missing = df[REQUIRED_DATES[name]].isna().any(axis=1)
        if missing.any():
            which = df.loc[missing, REQUIRED_DATES[name]].isna().apply(
                lambda r: "NaT: " + ", ".join(r.index[r]), axis=1)
            rejects.add(name, df[missing], "required date missing or unparseable", which)
            df = df[~missing].copy()
        log_step(name, "reject unparseable dates", before, len(df))
        if name == "orders":
            orders = df
        else:
            shipments = df

    # A parcel cannot arrive before it was shipped. The schema rejects these
    # too, so catching them here is the difference between a reject row with
    # a reason and an aborted transaction.
    before = len(shipments)
    impossible = (shipments["actual_delivery_date"].notna()
                  & (shipments["actual_delivery_date"] < shipments["ship_date"]))
    if impossible.any():
        detail = ("actual "
                  + shipments.loc[impossible, "actual_delivery_date"].dt.strftime("%Y-%m-%d")
                  + " < ship "
                  + shipments.loc[impossible, "ship_date"].dt.strftime("%Y-%m-%d"))
        rejects.add("shipments", shipments[impossible],
                    "actual_delivery_date before ship_date", detail)
        shipments = shipments[~impossible].copy()
    log_step("shipments", "reject impossible timeline", before, len(shipments))

    frames["orders"] = orders
    frames["shipments"] = shipments
    return frames


def transform(frames, rejects):
    log.info("TRANSFORM")
    total_failed = 0

    for name in LOAD_ORDER:
        df = frames[name]
        log.info("  -- %s --", name)

        # --- value-shaping steps: all of these must precede dedup ---
        before = len(df)
        df, failed = parse_date_columns(df, name)
        total_failed += failed
        log_step(name, "parse dates", before, len(df))

        before = len(df)
        df = normalize_text(df, name)
        log_step(name, "normalize text", before, len(df))

        before = len(df)
        df = coerce_numerics(df, name)
        log_step(name, "coerce numerics", before, len(df))

        before = len(df)
        df = fill_missing(df, name)
        log_step(name, "fill missing values", before, len(df))

        # --- row-removing steps: only now that values are final ---
        before = len(df)
        df = df.drop_duplicates()
        log_step(name, "drop duplicate rows", before, len(df))

        frames[name] = df

    log.info("  %d date values failed to parse across all columns", total_failed)

    log.info("  -- validation --")
    # Numbers first: everything after this depends on the ids being real.
    # Then keys, then dates, then value ranges.
    frames = validate_numerics(frames, rejects)
    frames = validate_primary_keys(frames, rejects)
    frames = validate_dates(frames, rejects)
    frames = validate_ranges(frames, rejects)
    shipments = frames["shipments"]

    # Delivery performance, computed only on rows whose dates survived
    # validation. Negative days_late means it arrived early, so on_time is
    # days_late <= 0. Both stay unknown while a shipment is in transit --
    # the schema rejects a row that knows one but not the other.
    days = (shipments["actual_delivery_date"]
            - shipments["expected_delivery_date"]).dt.days
    shipments["days_late"] = days
    shipments["on_time"] = (days <= 0).astype("boolean").where(days.notna(), pd.NA)
    log.info("  derived days_late/on_time: %d early, %d on the day, %d late, "
             "%d in transit",
             int((days < 0).sum()), int((days == 0).sum()),
             int((days > 0).sum()), int(days.isna().sum()))

    # A late shipment nobody explained is not the same fact as one that
    # arrived on time, but a blank delay_reason says both. Give the first
    # its own value so NULL is left meaning only "not late".
    #
    # This has to run after on_time exists, which is why it isn't in
    # fill_missing with the other sentinels -- those only need the raw row.
    #
    # on_time is nullable, so fillna(False) keeps in-transit rows (NA) out
    # of the mask rather than letting them fall through as late.
    unexplained = ((shipments["on_time"] == 0).fillna(False)
                   & shipments["delay_reason"].isna())
    n = int(unexplained.sum())
    if n:
        shipments.loc[unexplained, "delay_reason"] = UNKNOWN_DELAY_REASON
        log.info("  filled %d late shipments with no recorded reason -> '%s'",
                 n, UNKNOWN_DELAY_REASON)
    log.info("  delay_reason: %d set, %d null (on time or in transit)",
             int(shipments["delay_reason"].notna().sum()),
             int(shipments["delay_reason"].isna().sum()))

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


class SchemaDriftError(RuntimeError):
    """schema.sql no longer matches the schema the database was built with."""


class ForeignKeyViolation(RuntimeError):
    """The loaded rows break a foreign key. Fails the run; nothing commits."""


def schema_fingerprint():
    """
    A number identifying the current contents of schema.sql.

    Hashed rather than hand-maintained: a version number you have to
    remember to bump fails in exactly the situation this guards against.
    Truncated to 31 bits because that is what PRAGMA user_version holds,
    and shifted off 0 because 0 is how SQLite says "never set".
    """
    digest = hashlib.sha256(SCHEMA_PATH.read_bytes()).digest()
    return (int.from_bytes(digest[:4], "big") & 0x7FFFFFFF) or 1


def ensure_schema(conn):
    """
    Build the tables the first time, and refuse to load into a stale one.

    The database carries a fingerprint of the schema.sql that built it, in
    PRAGMA user_version. If the file has changed since, the tables in the
    database are the old ones -- constraints added to the file wouldn't be
    enforced and columns added to it wouldn't exist -- so this stops rather
    than loading into a definition nobody meant to use.

    It never deletes anything. Rebuilding is the caller's call.
    """
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    missing = [t for t in LOAD_ORDER if t not in existing]
    fingerprint = schema_fingerprint()

    if missing:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        # PRAGMA won't take a bound parameter, and this is an int we just
        # computed, so interpolating it is safe.
        conn.execute(f"PRAGMA user_version = {fingerprint}")
        log.info("  created schema from %s (fingerprint %d)",
                 SCHEMA_PATH.name, fingerprint)
        return

    stored = conn.execute("PRAGMA user_version").fetchone()[0]

    if stored == 0:
        raise SchemaDriftError(
            f"{DB_PATH} was built before schema drift detection existed, so "
            f"there is no way to tell whether it matches {SCHEMA_PATH.name}.\n\n"
            + _rebuild_instructions())

    if stored != fingerprint:
        raise SchemaDriftError(
            f"{SCHEMA_PATH.name} has changed since {DB_PATH.name} was built "
            f"(database fingerprint {stored}, schema.sql now {fingerprint}).\n\n"
            f"The database still holds the old table definitions. Loading now "
            f"would write against a schema you have already edited: new "
            f"constraints would go unenforced, new columns would be missing.\n\n"
            + _rebuild_instructions())

    log.info("  schema matches %s (fingerprint %d)", SCHEMA_PATH.name, fingerprint)


def _rebuild_instructions():
    return (
        "This pipeline is a full refresh from data\\raw\\, so rebuilding "
        "loses nothing. Delete the database and run again:\n\n"
        "    del data\\processed\\logistics.db\n"
        "    venv\\Scripts\\python.exe src\\etl.py\n\n"
        "Nothing has been deleted for you -- that call is yours.")


def row_counts(conn):
    return {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            for t in LOAD_ORDER}


def fmt_counts(counts):
    return "  ".join(f"{t}={n}" for t, n in counts.items())


def split_on_fk(child, child_col, parent_keys):
    """Split a child table into rows whose parent exists and rows whose doesn't."""
    orphaned = ~child[child_col].isin(parent_keys)
    return child[~orphaned].copy(), child[orphaned].copy()


def to_records(df):
    """
    DataFrame -> plain Python tuples for executemany.

    Dates go in as 'YYYY-MM-DD' strings: SQLite has no date type, and the
    schema's `x IS date(x)` checks reject the '2026-01-05 00:00:00' that a
    raw datetime would land as. Every flavour of NA becomes None.
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


def load(frames, rejects):
    log.info("LOAD")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    warehouses = frames["warehouses"]
    orders = frames["orders"]
    shipments = frames["shipments"]

    # Bulk inserts abort as a whole, with no way to tell which row was bad.
    # Find the orphans here, where the offending value can still be named.
    before = len(orders)
    orders, bad_orders = split_on_fk(
        orders, "warehouse_id", set(warehouses["warehouse_id"]))
    rejects.add("orders", bad_orders, "warehouse_id not found in warehouses",
                "warehouse_id=" + key_text(bad_orders["warehouse_id"]))
    log_step("orders", "reject orphan warehouse_id", before, len(orders))

    # Checked against the orders that survived, not the ones we started with
    # -- a shipment whose order was rejected for any reason is orphaned too.
    before = len(shipments)
    shipments, bad_shipments = split_on_fk(
        shipments, "order_id", set(orders["order_id"]))
    rejects.add("shipments", bad_shipments, "order_id not found in orders",
                "order_id=" + key_text(bad_shipments["order_id"]))
    log_step("shipments", "reject orphan order_id", before, len(shipments))

    rejects.write()

    staged = {"warehouses": warehouses, "orders": orders, "shipments": shipments}

    conn = connect()
    try:
        ensure_schema(conn)

        before_counts = row_counts(conn)
        log.info("  rows before load:  %s", fmt_counts(before_counts))

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

            # Checked before COMMIT, not after. Run after and a violation is
            # already durable, so failing the run would just leave a broken
            # database behind; here it rolls back with everything else.
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise ForeignKeyViolation(
                    f"{len(violations)} foreign key violation(s) after load, "
                    f"first few: {violations[:5]}")
            log.info("  foreign key check clean")

            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            log.error("  load failed and was rolled back; database unchanged")
            raise

        log.info("  rows after load:   %s", fmt_counts(row_counts(conn)))
    finally:
        conn.close()

    log.info("  database written to %s", DB_PATH)


# ==========================================================================

def main():
    rejects = Rejects()
    frames = extract()
    frames = transform(frames, rejects)
    try:
        load(frames, rejects)
    except SchemaDriftError as e:
        # A traceback would bury the instructions. This one is a decision
        # for the operator, not a crash to debug.
        log.error("SCHEMA DRIFT -- nothing was loaded\n\n%s\n", e)
        raise SystemExit(2)
    log.info("done")


if __name__ == "__main__":
    main()
