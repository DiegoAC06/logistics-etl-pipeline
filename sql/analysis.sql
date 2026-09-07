/*
   Analysis queries for logistics.db.  Run: python src\run_analysis.py

   Populations. Almost every mistake in this file would be an aggregate run
   over the wrong one, so each query's comment names its own:

     all shipments      5000
       delivered        4945   days_late IS NOT NULL
         early           757   days_late < 0
         on the day     3179   days_late = 0
         late           1009   days_late > 0
       in transit         55   days_late IS NULL -- not late, just not here yet
     with a delay_reason 1009  every late shipment has one; 26 say 'unknown'
     carrier 'Unknown'    36   blank at source, not a real carrier

   Query 4 keeps the 'Unknown' carrier so its totals add up, and pins it
   last rather than ranking it. Query 6 drops it. Each says which and why.

   Averaging days_late over everything delivered gives 0.48; over the late
   ones only it's 3.09. Both are true and they answer different questions,
   so where it matters both are reported.

   Every percentage is written as:

       round(100.0 * CAST(<numerator> AS REAL) / NULLIF(<denominator>, 0), 1)

   The CAST is what stops integer division -- `sum(x) / count(*)` on two
   INTEGER columns truncates to 0. `100.0 *` at the front works too, but
   only because evaluation runs left to right; move it to the end and the
   query silently returns 0.0. NULLIF turns a zero denominator into NULL.

   Counts of nothing are 0. Rates over nothing stay NULL, because 0% and
   "nothing to measure" are different claims. run_analysis.py prints those
   as 'no data'.

   Every join says whether it's inner or outer and why.

   run_analysis.py splits this file on the `-- name:` lines, so keep that
   format if you add a query.
*/


-- name: on_time_by_warehouse
-- title: On-time delivery rate by warehouse
-- question: Which sites deliver on time? shipments and in_transit count every shipment; delivered and on_time_pct cover only what has actually arrived.
SELECT
    w.name                                        AS warehouse,
    w.region                                      AS region,
    count(s.shipment_id)                          AS shipments,
    count(s.on_time)                              AS delivered,
    count(s.shipment_id) - count(s.on_time)       AS in_transit,
    round(100.0 * CAST(sum(s.on_time) AS REAL)
          / NULLIF(count(s.on_time), 0), 1)       AS on_time_pct
FROM warehouses w
-- OUTER: the question is about every warehouse, so a site that shipped
-- nothing is an answer ("zero"), not a row to drop. Counting
-- s.shipment_id rather than * matters here -- count(*) would score the
-- empty outer row as 1.
LEFT JOIN orders    o ON o.warehouse_id = w.warehouse_id
LEFT JOIN shipments s ON s.order_id     = o.order_id
GROUP BY w.warehouse_id, w.name, w.region
ORDER BY on_time_pct DESC;


-- name: days_late_by_region
-- title: Average days late by region
-- question: How far behind does a region run? avg_net_days is over everything delivered, so early arrivals pull it down; avg_when_late is over late shipments only, which is what a waiting customer feels. late_pct is late as a share of delivered.
SELECT
    w.region                                                      AS region,
    count(s.days_late)                                            AS delivered,
    -- NULL > 0 is NULL, so undelivered rows drop out of the sum on their own
    coalesce(sum(s.days_late > 0), 0)                             AS late,
    round(100.0 * CAST(sum(s.days_late > 0) AS REAL)
          / NULLIF(count(s.days_late), 0), 1)                     AS late_pct,
    round(avg(s.days_late), 2)                                    AS avg_net_days,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late,
    max(s.days_late)                                              AS worst_days
FROM warehouses w
-- OUTER: same reasoning as above. A region whose warehouses have delivered
-- nothing yet should read as empty rather than disappear from the report.
LEFT JOIN orders    o ON o.warehouse_id = w.warehouse_id
LEFT JOIN shipments s ON s.order_id     = o.order_id
GROUP BY w.region
ORDER BY avg_when_late DESC;


-- name: top_delay_reason_by_warehouse
-- title: Most common delay reason per warehouse
-- question: What goes wrong most often at each site? Every late shipment carries a reason now, so occurrences and share_pct are over all of a site's late shipments and the shares sum to 100%. unknown_cause is how many of those were logged with no actual cause.
WITH delay_counts AS (
    SELECT
        o.warehouse_id  AS warehouse_id,
        s.delay_reason  AS delay_reason,
        count(*)        AS occurrences
    FROM shipments s
    -- INNER: this block tallies delays that actually happened and were
    -- explained. A shipment with no delay_reason contributes nothing here.
    JOIN orders o ON o.order_id = s.order_id
    WHERE s.delay_reason IS NOT NULL
    GROUP BY o.warehouse_id, s.delay_reason
),
late_totals AS (
    -- every late shipment, plus how many of them say 'unknown'
    SELECT
        o.warehouse_id                        AS warehouse_id,
        sum(s.days_late > 0)                  AS late_total,
        sum(s.delay_reason = 'unknown')       AS unknown_cause
    FROM shipments s
    JOIN orders o ON o.order_id = s.order_id
    GROUP BY o.warehouse_id
),
ranked AS (
    SELECT
        *,
        -- delay_reason breaks ties, so the result doesn't shuffle between runs
        row_number() OVER (PARTITION BY warehouse_id
                           ORDER BY occurrences DESC, delay_reason) AS rank_in_warehouse,
        sum(occurrences) OVER (PARTITION BY warehouse_id)           AS explained
    FROM delay_counts
)
SELECT
    w.name                                          AS warehouse,
    coalesce(r.delay_reason, 'no delays recorded')  AS top_reason,
    coalesce(r.occurrences, 0)                      AS occurrences,
    coalesce(t.late_total, 0)                       AS late_total,
    coalesce(t.unknown_cause, 0)                    AS unknown_cause,
    -- every late shipment has a reason now, so late_total is the honest
    -- denominator and the shares across reasons add up to 100%
    round(100.0 * CAST(r.occurrences AS REAL)
          / NULLIF(t.late_total, 0), 1)             AS share_pct
FROM warehouses w
-- OUTER: a warehouse with a clean record is a result worth seeing. Inner
-- would silently hide the best-performing sites, which is the opposite of
-- what this report is for.
LEFT JOIN ranked      r ON r.warehouse_id = w.warehouse_id
                       AND r.rank_in_warehouse = 1
LEFT JOIN late_totals t ON t.warehouse_id = w.warehouse_id
ORDER BY occurrences DESC, warehouse;


-- name: carrier_ranking
-- title: Carrier performance ranking
-- question: Which carriers earn their money? delivered, on_time_pct and avg_net_days are over that carrier's delivered shipments; avg_when_late is over its late ones only. The gap between the two averages is how concentrated the pain is. RECONCILES: delivered sums to every delivered shipment (4,909 across the five real carriers + 36 in the 'Unknown' bucket = 4,945).
SELECT
    s.carrier                                                     AS carrier,
    count(s.on_time)                                              AS delivered,
    round(100.0 * CAST(sum(s.on_time) AS REAL)
          / NULLIF(count(s.on_time), 0), 1)                       AS on_time_pct,
    coalesce(sum(s.on_time = 0), 0)                               AS late,
    round(avg(s.days_late), 2)                                    AS avg_net_days,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late,
    max(s.days_late)                                              AS worst_days
-- NO JOIN: carrier is a column on shipments, not a table. There is no
-- carrier dimension to outer-join against, so a carrier with zero shipments
-- cannot exist in this schema. Add a carriers table and this becomes a LEFT
-- JOIN like the others.
FROM shipments s
-- 'Unknown' is NOT filtered out here, on purpose. It used to be, and the
-- delivered column then summed to 4,909 with nothing on screen explaining
-- the missing 36 -- a silent hole is worse than a labelled bucket. It is
-- still not a carrier (it's shipments whose carrier was blank at source),
-- so it's pinned to the bottom instead of being ranked among the real ones.
-- Exclude it explicitly when comparing carriers against each other.
GROUP BY s.carrier
HAVING count(s.on_time) > 0
ORDER BY s.carrier = 'Unknown', on_time_pct DESC;


-- name: monthly_trend
-- title: Monthly order volume and on-time rate
-- question: Does service hold up under peak season? Two populations in one row: orders and units count every order placed that month, while on_time_pct covers only those whose shipment has arrived. Recent months carry unshipped orders, so watch in_transit before reading the rate.
SELECT
    strftime('%Y-%m', o.order_date)             AS month,
    count(*)                                    AS orders,
    sum(o.quantity)                             AS units,
    count(s.on_time)                            AS delivered,
    count(o.order_id) - count(s.on_time)        AS in_transit,
    round(100.0 * CAST(sum(s.on_time) AS REAL)
          / NULLIF(count(s.on_time), 0), 1)     AS on_time_pct
FROM orders o
-- OUTER: an order that hasn't been shipped yet is still demand and still
-- belongs in the month's volume. Inner would quietly understate recent
-- months, which is exactly where the unshipped orders are.
LEFT JOIN shipments s ON s.order_id = o.order_id
GROUP BY month
ORDER BY month;


-- name: worst_warehouse_carrier_pairs
-- title: Worst warehouse / carrier combinations
-- question: Where should attention go first? delivered, late_pct and avg_net_days are over that pairing's delivered shipments; avg_when_late is over its late ones. DOES NOT RECONCILE, by design: this is a worst-15 list, so HAVING and LIMIT drop most pairings, and the 'Unknown' carrier is excluded outright. Use query 4 or 1 for totals.
SELECT
    w.name                                                        AS warehouse,
    s.carrier                                                     AS carrier,
    count(s.on_time)                                              AS delivered,
    sum(s.on_time = 0)                                            AS late,
    round(100.0 * CAST(sum(s.on_time = 0) AS REAL)
          / NULLIF(count(s.on_time), 0), 1)                       AS late_pct,
    round(avg(s.days_late), 2)                                    AS avg_net_days,
    round(avg(CASE WHEN s.days_late > 0 THEN s.days_late END), 2) AS avg_when_late
FROM shipments s
-- INNER, deliberately: the row here is a warehouse/carrier pairing that
-- actually shipped. Outer would invent rows for the 40-odd pairings that
-- never happened, and a ranking of combinations with no shipments is noise.
-- Dropping unmatched rows IS the filter.
JOIN orders     o ON o.order_id     = s.order_id
JOIN warehouses w ON w.warehouse_id = o.warehouse_id
-- excluded here but NOT in query 4: a warehouse/'Unknown' pairing isn't a
-- real relationship to rank, and this list never reconciles anyway
WHERE s.carrier <> 'Unknown'
GROUP BY w.warehouse_id, w.name, s.carrier
-- below 30 delivered it's luck, not performance
HAVING count(s.on_time) >= 30
ORDER BY late_pct DESC
LIMIT 15;
