/*
   logistics-etl-pipeline schema.

   warehouses -> orders -> shipments. Orders and shipments are separate
   because one order can ship in several parcels.

   Dates are ISO 'YYYY-MM-DD' text (SQLite has no date type). The
   `x IS date(x)` checks reject anything that isn't a real date.

   Heads up: the pragma below is per-connection and isn't saved in the db
   file. Your app has to set it too or the foreign keys do nothing.
*/

PRAGMA foreign_keys = ON;


CREATE TABLE warehouses (
    warehouse_id  INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    region        TEXT NOT NULL,
    city          TEXT NOT NULL,
    state         TEXT NOT NULL
);


CREATE TABLE orders (
    order_id          INTEGER PRIMARY KEY,

    -- RESTRICT: don't let anyone delete a warehouse that still has orders.
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


CREATE TABLE shipments (
    shipment_id             INTEGER PRIMARY KEY,

    -- CASCADE here, unlike orders above: a shipment is meaningless without
    -- its order. Not UNIQUE -- add that if you never split an order.
    order_id                INTEGER NOT NULL
                            REFERENCES orders (order_id)
                                ON UPDATE CASCADE
                                ON DELETE CASCADE,

    carrier                 TEXT NOT NULL,

    ship_date               TEXT NOT NULL
                            CHECK (ship_date IS date(ship_date)),

    expected_delivery_date  TEXT NOT NULL
                            CHECK (expected_delivery_date IS date(expected_delivery_date)),

    -- null = still in transit
    actual_delivery_date    TEXT
                            CHECK (actual_delivery_date IS NULL
                                   OR actual_delivery_date IS date(actual_delivery_date)),

    -- null = on time, or not delivered yet. Check actual_delivery_date to
    -- tell those apart.
    delay_reason            TEXT
                            CHECK (delay_reason IS NULL
                                   OR delay_reason IN (
                                          'weather',
                                          'carrier_delay',
                                          'inventory_shortage',
                                          'address_issue',
                                          'customs'
                                      )),

    -- Filled in by the ETL. Stored rather than worked out at query time
    -- because nearly every report filters on them. Both are NULL while a
    -- shipment is still in transit. days_late goes negative for early
    -- deliveries, so on_time is days_late <= 0, not days_late == 0.
    days_late               INTEGER,
    on_time                 INTEGER
                            CHECK (on_time IN (0, 1)),

    -- nothing arrives before it ships
    CHECK (expected_delivery_date >= ship_date),
    CHECK (actual_delivery_date IS NULL OR actual_delivery_date >= ship_date),

    -- both derived columns are known, or neither is
    CHECK ((days_late IS NULL) = (on_time IS NULL))
);


-- SQLite won't index foreign keys for you, and these are joined constantly.
CREATE INDEX idx_orders_warehouse_id   ON orders    (warehouse_id);
CREATE INDEX idx_shipments_order_id    ON shipments (order_id);

-- for the usual date-range and carrier-performance queries
CREATE INDEX idx_orders_order_date     ON orders    (order_date);
CREATE INDEX idx_shipments_ship_date   ON shipments (ship_date);
CREATE INDEX idx_shipments_carrier     ON shipments (carrier);

-- partial, so it only covers the rows that actually went wrong
CREATE INDEX idx_shipments_delay_reason
    ON shipments (delay_reason)
    WHERE delay_reason IS NOT NULL;