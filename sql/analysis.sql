/*
   Analysis queries for logistics.db.  Run: python src\run_analysis.py

   Three things to know before reading these:
     - on_time IS NULL means not delivered yet, not late. Rates are a
       percentage of delivered.
     - days_late goes negative when something arrives early.
     - carrier 'Unknown' is an ETL placeholder, not a carrier.

   run_analysis.py splits this file on the `-- name:` lines, so keep that
   format if you add a query.
*/


-- name: on_time_by_warehouse
-- title: On-time delivery rate by warehouse
-- question: Which sites deliver on time? Shipments and delivered both shown, since the rate only counts delivered.
SELECT
    w.name                            AS warehouse,
    w.region                          AS region,
    count(*)                          AS shipments,
    sum(s.on_time IS NOT NULL)        AS delivered,
    sum(s.on_time IS NULL)            AS in_transit,
    round(100.0 * avg(s.on_time), 1)  AS on_time_pct
FROM shipments s
JOIN orders     o ON o.order_id     = s.order_id
JOIN warehouses w ON w.warehouse_id = o.warehouse_id
GROUP BY w.warehouse_id, w.name, w.region
ORDER BY on_time_pct DESC;


-- name: days_late_by_region
-- title: Average days late by region
-- question: When a region runs behind, how far behind? avg_net counts early deliveries against it; avg_when_late is what a waiting customer sees.
SELECT
    w.region                                                      AS region,
    count(*)                                                      AS delivered,
    sum(s.days_late > 0)                                          AS late,
    round(100.0 * avg(s.on_time = 0), 1)                          AS late_pct,
    round(avg(s.days_late), 2)                                    AS avg_net_days,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late,
    max(s.days_late)                                              AS worst_days
FROM shipments s
JOIN orders     o ON o.order_id     = s.order_id
JOIN warehouses w ON w.warehouse_id = o.warehouse_id
WHERE s.days_late IS NOT NULL
GROUP BY w.region
ORDER BY avg_when_late DESC;


-- name: top_delay_reason_by_warehouse
-- title: Most common delay reason per warehouse
-- question: What goes wrong most often at each site? A shortage problem needs a different fix than a weather one.
WITH reason_counts AS (
    SELECT
        w.warehouse_id                AS warehouse_id,
        w.name                        AS warehouse,
        s.delay_reason                AS delay_reason,
        count(*)                      AS occurrences
    FROM shipments s
    JOIN orders     o ON o.order_id     = s.order_id
    JOIN warehouses w ON w.warehouse_id = o.warehouse_id
    WHERE s.delay_reason IS NOT NULL
    GROUP BY w.warehouse_id, w.name, s.delay_reason
),
ranked AS (
    SELECT
        *,
        -- delay_reason breaks ties, so the result doesn't shuffle between runs
        row_number() OVER (PARTITION BY warehouse_id
                           ORDER BY occurrences DESC, delay_reason) AS rank_in_warehouse,
        sum(occurrences) OVER (PARTITION BY warehouse_id)           AS all_delays
    FROM reason_counts
)
SELECT
    warehouse                                     AS warehouse,
    delay_reason                                  AS top_reason,
    occurrences                                   AS occurrences,
    all_delays                                    AS total_delays,
    round(100.0 * occurrences / all_delays, 1)    AS share_pct
FROM ranked
WHERE rank_in_warehouse = 1
ORDER BY occurrences DESC;


-- name: carrier_ranking
-- title: Carrier performance ranking
-- question: Which carriers earn their money? Often-late-by-a-day is a different problem from rarely-late-by-a-week, so both are here.
SELECT
    s.carrier                                                     AS carrier,
    count(*)                                                      AS delivered,
    round(100.0 * avg(s.on_time), 1)                              AS on_time_pct,
    sum(s.on_time = 0)                                            AS late,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late,
    max(s.days_late)                                              AS worst_days
FROM shipments s
WHERE s.on_time IS NOT NULL
  AND s.carrier <> 'Unknown'   -- placeholder, not a carrier
GROUP BY s.carrier
ORDER BY on_time_pct DESC;


-- name: monthly_trend
-- title: Monthly order volume and on-time rate
-- question: Does service hold up under peak season? Volume next to reliability, so a dip can be read against the load causing it.
SELECT
    strftime('%Y-%m', o.order_date)   AS month,
    count(*)                          AS orders,
    sum(o.quantity)                   AS units,
    sum(s.on_time IS NOT NULL)        AS delivered,
    sum(s.on_time IS NULL)            AS in_transit,
    round(100.0 * avg(s.on_time), 1)  AS on_time_pct
FROM orders o
-- LEFT JOIN: an order with no shipment is still demand
LEFT JOIN shipments s ON s.order_id = o.order_id
GROUP BY month
ORDER BY month;


-- name: worst_warehouse_carrier_pairs
-- title: Worst warehouse / carrier combinations
-- question: Where should attention go first? A carrier can look fine nationally and be poor out of one site.
SELECT
    w.name                                                        AS warehouse,
    s.carrier                                                     AS carrier,
    count(*)                                                      AS delivered,
    sum(s.on_time = 0)                                            AS late,
    round(100.0 * avg(s.on_time = 0), 1)                          AS late_pct,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late
FROM shipments s
JOIN orders     o ON o.order_id     = s.order_id
JOIN warehouses w ON w.warehouse_id = o.warehouse_id
WHERE s.on_time IS NOT NULL
  AND s.carrier <> 'Unknown'
GROUP BY w.warehouse_id, w.name, s.carrier
-- below 30 shipments it's luck, not performance
HAVING count(*) >= 30
ORDER BY late_pct DESC
LIMIT 15;
