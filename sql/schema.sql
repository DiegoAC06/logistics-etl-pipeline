/* ============================================================================
   logistics-etl-pipeline — SQLite schema
   ----------------------------------------------------------------------------
   Three tables in a simple star-ish shape:

       warehouses  (dimension)  1 --< orders  1 --< shipments
                                          (fact)      (fact)

   orders is the grain of "what a customer asked for"; shipments is the grain
   of "what physically moved". They are kept separate rather than flattened
   into one table so that an order split across multiple shipments stays
   representable without duplicating order-level columns (quantity in
   particular would double-count under a flattened model).

   Conventions used throughout:

   - Dates are TEXT in ISO-8601 'YYYY-MM-DD'. SQLite has no DATE type; ISO
     strings sort and compare correctly as text, and every date() / julianday()
     function accepts them. Each date column carries a CHECK using the
     `x IS date(x)` idiom -- date() returns NULL on unparseable input, so the
     comparison fails for anything that is not a real calendar date.
   - Surrogate INTEGER PRIMARY KEY on every table (an alias for SQLite's rowid,
     so lookups by id need no secondary index).
   - Foreign keys are declared inline and indexed explicitly: SQLite does NOT
     auto-create an index on a referencing column the way MySQL does, and
     without one every parent-side delete degrades to a full child scan.

   !! Foreign keys are OFF by default in SQLite and the pragma is per-connection
      -- it is NOT stored in the database file. The PRAGMA below only applies to
      the session that runs this script. Every application connection must issue
      `PRAGMA foreign_keys = ON;` for itself or the FK clauses are inert.
   ============================================================================ */

PRAGMA foreign_keys = ON;


/* ---------------------------------------------------------------------------
   warehouses -- dimension table, one row per physical facility.
   Every column is NOT NULL: a warehouse with no city or state cannot be
   placed on a map or rolled up by geography, which is most of its purpose.
   --------------------------------------------------------------------------- */
CREATE TABLE warehouses (
    warehouse_id  INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    region        TEXT NOT NULL,
    city          TEXT NOT NULL,
    state         TEXT NOT NULL
);


/* ---------------------------------------------------------------------------
   orders -- one row per customer order line.

   ON DELETE RESTRICT: deleting a warehouse that still has orders would orphan
   historical demand, so the delete is refused. Close a warehouse by flagging
   it, not by removing the row.
   --------------------------------------------------------------------------- */
CREATE TABLE orders (
    order_id          INTEGER PRIMARY KEY,

    warehouse_id      INTEGER NOT NULL
                      REFERENCES warehouses (warehouse_id)
                          ON UPDATE CASCADE
                          ON DELETE RESTRICT,

    order_date        TEXT    NOT NULL
                      CHECK (order_date IS date(order_date)),

    customer_id       INTEGER NOT NULL,

    quantity          INTEGER NOT NULL
                      CHECK (quantity > 0),

    product_category  TEXT    NOT NULL
);


/* ---------------------------------------------------------------------------
   shipments -- one row per physical shipment against an order.

   ON DELETE CASCADE: a shipment has no meaning without its order, so removing
   the order removes its shipments. This is the opposite choice from
   orders.warehouse_id above, because the relationship is ownership rather
   than reference.

   No UNIQUE on order_id: an order may ship in several parcels. If this
   business genuinely ships each order exactly once, change the column to
   `order_id INTEGER NOT NULL UNIQUE REFERENCES ...` and the database will
   enforce it.

   actual_delivery_date is nullable -- NULL means still in transit.
   delay_reason is nullable and restricted to a fixed vocabulary.
   --------------------------------------------------------------------------- */
CREATE TABLE shipments (
    shipment_id             INTEGER PRIMARY KEY,

    order_id                INTEGER NOT NULL
                            REFERENCES orders (order_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,

    carrier                 TEXT NOT NULL,

    ship_date               TEXT NOT NULL
                            CHECK (ship_date IS date(ship_date)),

    expected_delivery_date  TEXT NOT NULL
                            CHECK (expected_delivery_date IS date(expected_delivery_date)),

    actual_delivery_date    TEXT
                            CHECK (actual_delivery_date IS NULL
                                   OR actual_delivery_date IS date(actual_delivery_date)),

    /* NULL = no delay recorded (delivered on time, or not yet delivered). */
    delay_reason            TEXT
                            CHECK (delay_reason IS NULL
                                   OR delay_reason IN (
                                          'weather',
                                          'carrier_delay',
                                          'inventory_shortage',
                                          'address_issue',
                                          'customs'
                                      )),

    /* Timeline sanity: nothing may be promised or delivered before it ships. */
    CHECK (expected_delivery_date >= ship_date),
    CHECK (actual_delivery_date IS NULL OR actual_delivery_date >= ship_date)
);


/* ---------------------------------------------------------------------------
   Indexes.

   The two FK columns are indexed because SQLite does not do it automatically
   and both are join keys on every analytical query. The remaining three cover
   the filters this pipeline is expected to run: date-range scans on orders,
   and on-time-performance slices grouped by carrier or by delay reason.
   --------------------------------------------------------------------------- */
CREATE INDEX idx_orders_warehouse_id   ON orders    (warehouse_id);
CREATE INDEX idx_orders_order_date     ON orders    (order_date);

CREATE INDEX idx_shipments_order_id    ON shipments (order_id);
CREATE INDEX idx_shipments_ship_date   ON shipments (ship_date);
CREATE INDEX idx_shipments_carrier     ON shipments (carrier);

/* Partial index -- only delayed shipments carry a reason, so this stays small
   even as the shipments table grows. */
CREATE INDEX idx_shipments_delay_reason
    ON shipments (delay_reason)
    WHERE delay_reason IS NOT NULL;
