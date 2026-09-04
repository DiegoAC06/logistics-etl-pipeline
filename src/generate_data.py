"""
Generate synthetic logistics data as CSVs in data/raw/.

The data is deliberately imperfect -- mixed date formats, blank fields,
sloppy casing, duplicate rows -- so the ETL step has real work to do.
Stdlib only, on purpose: pandas would tidy up the mess on write.

Everything is driven by one seed, so two runs produce identical files.

    python src\\generate_data.py
"""

import csv
import random
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

SEED = 42
N_ORDERS = 5000

# A trailing 12 months, pulled as if the extract ran on EXTRACT_DATE.
# Shipments that would land after that date are still in transit.
START_DATE = date(2025, 9, 1)
END_DATE = date(2026, 8, 31)
EXTRACT_DATE = date(2026, 9, 1)

OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"

# Tuned so the overall late rate lands near 20% once the multipliers below
# are applied. Change the multipliers and this needs re-tuning.
BASE_DELAY_RATE = 0.161

# Fractions of rows to corrupt.
DUPLICATE_RATE = 0.02
MISSING_RATE = 0.03
ODD_DATE_RATE = 0.12
SLOPPY_TEXT_RATE = 0.25


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

# share  = fraction of total order volume
# delay  = multiplier on the base delay rate (Phoenix and Newark are the
#          problem sites; Harrisburg and Dallas run clean)
# handle = days between order and ship
WAREHOUSES = [
    # id, name,             region,        city,          state, share, delay, handle
    (1, "Newark DC",        "Northeast",   "Newark",      "NJ",  0.16,  1.50, (0, 2)),
    (2, "Harrisburg DC",    "Mid-Atlantic","Harrisburg",  "PA",  0.11,  0.65, (0, 1)),
    (3, "Atlanta DC",       "Southeast",   "Atlanta",     "GA",  0.14,  0.90, (0, 2)),
    (4, "Chicago DC",       "Midwest",     "Chicago",     "IL",  0.15,  1.10, (0, 2)),
    (5, "Dallas DC",        "South Central","Dallas",     "TX",  0.13,  0.75, (0, 1)),
    (6, "Phoenix DC",       "Southwest",   "Phoenix",     "AZ",  0.09,  1.85, (1, 3)),
    (7, "Los Angeles DC",   "West",        "Los Angeles", "CA",  0.14,  1.30, (0, 2)),
    (8, "Seattle DC",       "Northwest",   "Seattle",     "WA",  0.08,  0.85, (0, 2)),
]

# Ports handle international freight, so they're the only ones that can
# plausibly get stuck in customs.
PORT_WAREHOUSES = {1, 7, 8}
WESTERN_REGIONS = {"West", "Northwest", "Southwest"}

# name: (delay multiplier, transit days)
CARRIERS = {
    "FedEx":  (0.70, (2, 4)),
    "UPS":    (0.75, (2, 4)),
    "DHL":    (1.00, (3, 5)),
    "USPS":   (1.35, (3, 6)),
    "OnTrac": (1.85, (1, 3)),   # fast when it works, unreliable when it doesn't
}

# OnTrac is a west-coast regional carrier, so it only appears out there.
CARRIER_MIX_NATIONAL = {"FedEx": 0.30, "UPS": 0.32, "DHL": 0.12, "USPS": 0.26}
CARRIER_MIX_WEST = {"FedEx": 0.24, "UPS": 0.26, "DHL": 0.08, "USPS": 0.18, "OnTrac": 0.24}

# Peak season in Nov/Dec, winter hangover in Jan/Feb, summer lull.
SEASON_VOLUME = {1: 0.75, 2: 0.70, 3: 0.85, 4: 0.85, 5: 0.90, 6: 0.88,
                 7: 0.92, 8: 0.95, 9: 0.85, 10: 1.00, 11: 1.65, 12: 1.95}

# Volume and reliability move together -- the busy months are the late ones.
SEASON_DELAY = {1: 1.30, 2: 1.20, 3: 0.95, 4: 0.85, 5: 0.85, 6: 0.90,
                7: 0.90, 8: 0.90, 9: 0.90, 10: 1.00, 11: 1.35, 12: 1.55}

PRODUCT_CATEGORIES = ["electronics", "apparel", "home_goods", "grocery",
                      "sporting_goods", "toys", "automotive", "health_beauty"]

DELAY_REASONS = ["weather", "carrier_delay", "inventory_shortage",
                 "address_issue", "customs"]


# --------------------------------------------------------------------------
# Date helpers
# --------------------------------------------------------------------------

# Four formats one system or another might export. None of them are
# ambiguous with each other -- DD/MM vs MM/DD would make the mess
# unsolvable rather than just annoying.
DATE_STYLES = ["iso", "us_slash", "iso_slash", "abbrev_month"]
ODD_STYLE_WEIGHTS = [0.45, 0.30, 0.25]  # among the three non-ISO styles


def fmt_date(d, style="iso"):
    if d is None:
        return ""
    if style == "us_slash":
        return d.strftime("%m/%d/%Y")
    if style == "iso_slash":
        return d.strftime("%Y/%m/%d")
    if style == "abbrev_month":
        return d.strftime("%d-%b-%Y")
    return d.isoformat()


def build_calendar(rng):
    """Every day in the window, weighted by season and day of week."""
    days, weights = [], []
    d = START_DATE
    while d <= END_DATE:
        w = SEASON_VOLUME[d.month]
        if d.weekday() == 5:
            w *= 0.55
        elif d.weekday() == 6:
            w *= 0.30
        days.append(d)
        weights.append(w)
        d += timedelta(days=1)
    return days, weights


# --------------------------------------------------------------------------
# Generation
# --------------------------------------------------------------------------

def pick_weighted(rng, mapping):
    keys = list(mapping)
    return rng.choices(keys, weights=[mapping[k] for k in keys], k=1)[0]


def pick_delay_reason(rng, wh_id, month):
    """Weather in winter, shortages at peak, customs only at the ports."""
    w = {"weather": 1.0, "carrier_delay": 3.0, "inventory_shortage": 1.2,
         "address_issue": 1.0, "customs": 0.0}
    if month in (11, 12, 1, 2):
        w["weather"] = 3.5
    if month in (11, 12):
        w["inventory_shortage"] = 3.0
    if wh_id in PORT_WAREHOUSES:
        w["customs"] = 1.4
    return pick_weighted(rng, w)


def generate(rng):
    warehouses = [
        {"warehouse_id": wid, "name": name, "region": region,
         "city": city, "state": state}
        for wid, name, region, city, state, _, _, _ in WAREHOUSES
    ]
    meta = {w[0]: {"share": w[5], "delay": w[6], "handle": w[7], "region": w[2]}
            for w in WAREHOUSES}

    days, day_weights = build_calendar(rng)
    order_dates = rng.choices(days, weights=day_weights, k=N_ORDERS)
    order_dates.sort()

    wh_ids = list(meta)
    wh_weights = [meta[i]["share"] for i in wh_ids]

    orders, shipments = [], []

    for i, order_date in enumerate(order_dates, start=1):
        wh_id = rng.choices(wh_ids, weights=wh_weights, k=1)[0]
        info = meta[wh_id]

        orders.append({
            "order_id": i,
            "warehouse_id": wh_id,
            "order_date": order_date,
            "customer_id": rng.randint(1000, 2199),
            # Most orders are small; a few are bulk.
            "quantity": rng.choices([1, 2, 3, 4, 5, 8, 12],
                                    weights=[34, 24, 16, 10, 8, 5, 3], k=1)[0],
            "product_category": rng.choice(PRODUCT_CATEGORIES),
        })

        mix = (CARRIER_MIX_WEST if info["region"] in WESTERN_REGIONS
               else CARRIER_MIX_NATIONAL)
        carrier = pick_weighted(rng, mix)
        carrier_delay, transit = CARRIERS[carrier]

        ship_date = order_date + timedelta(days=rng.randint(*info["handle"]))
        expected = ship_date + timedelta(days=rng.randint(*transit))

        p_late = BASE_DELAY_RATE * info["delay"] * carrier_delay \
            * SEASON_DELAY[ship_date.month]
        is_late = rng.random() < min(p_late, 0.85)

        if is_late:
            # Most delays are a day or two; the tail is long.
            slip = rng.choices([1, 2, 3, 4, 6, 9, 14],
                               weights=[30, 25, 16, 12, 9, 5, 3], k=1)[0]
            actual = expected + timedelta(days=slip)
            reason = pick_delay_reason(rng, wh_id, ship_date.month)
        else:
            # On time, and sometimes a day early.
            actual = expected - timedelta(days=rng.choices([0, 1], weights=[80, 20], k=1)[0])
            actual = max(actual, ship_date)
            reason = None

        # Anything not delivered by the extract date is still moving.
        if actual > EXTRACT_DATE:
            actual, reason = None, None

        shipments.append({
            "shipment_id": i,
            "order_id": i,
            "carrier": carrier,
            "ship_date": ship_date,
            "expected_delivery_date": expected,
            "actual_delivery_date": actual,
            "delay_reason": reason,
        })

    return warehouses, orders, shipments


# --------------------------------------------------------------------------
# Mess injection
#
# Runs on a copy of the clean records, so the summary can report the truth
# while the CSVs get the corrupted version.
# --------------------------------------------------------------------------

def sloppy(rng, value):
    """Same string, different sloppiness."""
    return rng.choice([
        value.upper(),
        value.lower(),
        " " + value,
        value + " ",
        "  " + value + "  ",
        value.replace(" ", "  ") if " " in value else value + "\t",
    ])


def messify(rng, warehouses, orders, shipments):
    stats = Counter()

    # Regions are dirtied on a fixed pattern rather than a dice roll. With
    # only 8 warehouses a random draw usually misses whole variants -- and
    # then nothing tests whether the ETL folds case as well as trims space.
    # 5 of 8 dirty, one of each kind, 3 left clean.
    variants = [str.upper, str.lower,
                lambda s: " " + s, lambda s: s + "  ",
                lambda s: s.upper() + " "]
    for slot, idx in enumerate([0, 2, 3, 5, 6]):
        warehouses[idx]["region"] = variants[slot](warehouses[idx]["region"])
        stats["sloppy_region"] += 1

    for o in orders:
        o["_style"] = "iso"
        if rng.random() < ODD_DATE_RATE:
            o["_style"] = rng.choices(DATE_STYLES[1:], weights=ODD_STYLE_WEIGHTS, k=1)[0]
            stats["odd_date_orders"] += 1
        # product_category and customer_id are safe to lose; the ETL can
        # fill or drop them without breaking the schema.
        if rng.random() < MISSING_RATE:
            o["product_category"] = None
            stats["missing_category"] += 1
        if rng.random() < MISSING_RATE / 3:
            o["customer_id"] = None
            stats["missing_customer"] += 1

    for s in shipments:
        s["_style"] = "iso"
        if rng.random() < ODD_DATE_RATE:
            s["_style"] = rng.choices(DATE_STYLES[1:], weights=ODD_STYLE_WEIGHTS, k=1)[0]
            stats["odd_date_shipments"] += 1
        if rng.random() < SLOPPY_TEXT_RATE:
            s["carrier"] = sloppy(rng, s["carrier"])
            stats["sloppy_carrier"] += 1
        # A late shipment whose reason nobody wrote down.
        if s["delay_reason"] and rng.random() < MISSING_RATE:
            s["delay_reason"] = None
            stats["missing_delay_reason"] += 1
        if rng.random() < MISSING_RATE / 4:
            s["carrier"] = None
            stats["missing_carrier"] += 1

    # Duplicate whole order rows, then shuffle so they aren't adjacent --
    # a dedupe that only checks neighbouring rows shouldn't pass.
    n_dupes = round(len(orders) * DUPLICATE_RATE)
    dupes = [dict(o) for o in rng.sample(orders, n_dupes)]
    orders = orders + dupes
    rng.shuffle(orders)
    stats["duplicate_orders"] = n_dupes

    return warehouses, orders, shipments, stats


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row[k])
                             for k in fieldnames})


def write_all(warehouses, orders, shipments):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    write_csv(OUT_DIR / "warehouses.csv",
              ["warehouse_id", "name", "region", "city", "state"],
              warehouses)

    write_csv(OUT_DIR / "orders.csv",
              ["order_id", "warehouse_id", "order_date", "customer_id",
               "quantity", "product_category"],
              [{**o, "order_date": fmt_date(o["order_date"], o["_style"])}
               for o in orders])

    write_csv(OUT_DIR / "shipments.csv",
              ["shipment_id", "order_id", "carrier", "ship_date",
               "expected_delivery_date", "actual_delivery_date", "delay_reason"],
              [{**s,
                "ship_date": fmt_date(s["ship_date"], s["_style"]),
                "expected_delivery_date": fmt_date(s["expected_delivery_date"], s["_style"]),
                "actual_delivery_date": fmt_date(s["actual_delivery_date"], s["_style"])}
               for s in shipments])


def bar(value, scale):
    return "#" * max(1, round(value / scale))


def summarize(warehouses, orders, shipments, stats, n_written_orders):
    wh_names = {w["warehouse_id"]: w["name"] for w in warehouses}
    wh_of_order = {o["order_id"]: o["warehouse_id"] for o in orders}

    delivered = [s for s in shipments if s["actual_delivery_date"] is not None]
    late = [s for s in shipments if s["delay_reason"] is not None]
    in_transit = len(shipments) - len(delivered)

    by_month = Counter(o["order_date"].strftime("%Y-%m") for o in orders)

    def rate_table(key_fn):
        total, bad = Counter(), Counter()
        for s in shipments:
            k = key_fn(s)
            total[k] += 1
            if s["delay_reason"] is not None:
                bad[k] += 1
        return sorted(((k, bad[k] / total[k], total[k]) for k in total),
                      key=lambda r: -r[1])

    print()
    print("=" * 62)
    print("  Synthetic logistics data written to data\\raw\\")
    print("=" * 62)
    print(f"  seed={SEED}   window {START_DATE} .. {END_DATE}   "
          f"extract {EXTRACT_DATE}")
    print()
    print(f"  warehouses.csv   {len(warehouses):>5} rows")
    print(f"  orders.csv       {n_written_orders:>5} rows "
          f"({len(orders)} unique + {stats['duplicate_orders']} duplicates)")
    print(f"  shipments.csv    {len(shipments):>5} rows (one per order)")

    print("\n  Orders per month")
    scale = max(by_month.values()) / 34
    for month in sorted(by_month):
        print(f"    {month}  {by_month[month]:>4}  {bar(by_month[month], scale)}")

    print("\n  Delivery performance")
    print(f"    delivered      {len(delivered):>5}")
    print(f"    still in transit {in_transit:>3}")
    print(f"    late           {len(late):>5}  "
          f"({len(late) / len(shipments):.1%} of all shipments)")

    print("\n  Late rate by carrier")
    for name, rate, n in rate_table(lambda s: s["carrier"] or "(missing)"):
        print(f"    {name:<12} {rate:>6.1%}   n={n:<5} {bar(rate * 100, 1.2)}")

    print("\n  Late rate by warehouse")
    for wid, rate, n in rate_table(lambda s: wh_of_order[s["order_id"]]):
        print(f"    {wh_names[wid]:<16} {rate:>6.1%}   n={n:<5} {bar(rate * 100, 1.2)}")

    print("\n  Delay reasons")
    reasons = Counter(s["delay_reason"] for s in late)
    for reason, n in reasons.most_common():
        print(f"    {reason:<20} {n:>4}  ({n / len(late):.1%})")

    print("\n  Mess injected for the ETL to clean")
    labels = [
        ("duplicate_orders",     "duplicate order rows"),
        ("missing_category",     "blank product_category"),
        ("missing_customer",     "blank customer_id"),
        ("missing_carrier",      "blank carrier"),
        ("missing_delay_reason", "late, but no reason given"),
        ("odd_date_orders",      "orders w/ non-ISO dates"),
        ("odd_date_shipments",   "shipments w/ non-ISO dates"),
        ("sloppy_carrier",       "carrier casing/whitespace"),
        ("sloppy_region",        "region casing/whitespace"),
    ]
    for key, label in labels:
        print(f"    {label:<30} {stats[key]:>5}")
    print("=" * 62)
    print()


def main():
    rng = random.Random(SEED)
    warehouses, orders, shipments = generate(rng)

    # Summarize the clean data, write the dirty data -- so the numbers below
    # describe what was actually generated, not what survived the mess.
    summary_orders = [dict(o) for o in orders]
    summary_shipments = [dict(s) for s in shipments]

    warehouses, orders, shipments, stats = messify(rng, warehouses, orders, shipments)
    write_all(warehouses, orders, shipments)
    summarize(warehouses, summary_orders, summary_shipments, stats, len(orders))


if __name__ == "__main__":
    main()
