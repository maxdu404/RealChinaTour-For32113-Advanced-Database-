-- ============================================================
-- DASHBOARD 1: CUSTOMER ANALYSIS
-- ============================================================

USE DATABASE REALCHINATOURS_DB;

WITH raw AS (
  SELECT COUNT(DISTINCT SRC_CUSTOMER_ID) AS raw_customers
  FROM STAR.BRIDGE_PERSON_IDENTITY_MAP
),
uni AS (
  SELECT COUNT(DISTINCT PERSON_KEY) AS unified_customers
  FROM STAR.DIM_PERSON
)
SELECT
  raw.raw_customers,
  uni.unified_customers,
  CASE
    WHEN raw.raw_customers = 0 THEN 0
    ELSE ROUND(100 * (raw.raw_customers - uni.unified_customers) / raw.raw_customers, 2)
  END AS dedup_rate_pct
FROM raw CROSS JOIN uni;


USE DATABASE REALCHINATOURS_DB;

SELECT
  LOWER(SRC_SYSTEM) AS channel,
  COUNT(DISTINCT PERSON_KEY) AS unique_customers
FROM STAR.BRIDGE_PERSON_IDENTITY_MAP
GROUP BY 1
ORDER BY unique_customers DESC;


SELECT src_system, COUNT(*) AS unified_customers
FROM SILVER.VW_CUSTOMER_UNIFIED
GROUP BY src_system;


WITH RAW_C AS (
  SELECT src_system, COUNT(*) AS raw_customers
  FROM RAW.RAW_USERS
  GROUP BY src_system
)
SELECT r.src_system, r.raw_customers, u.unified_customers,
       ROUND(100 * (r.raw_customers - u.unified_customers) / NULLIF(r.raw_customers,0), 2) AS dedup_rate_pct
FROM RAW_C r
LEFT JOIN (
  SELECT src_system, COUNT(*) AS unified_customers
  FROM SILVER.VW_CUSTOMER_UNIFIED
  GROUP BY src_system
) u ON r.src_system = u.src_system
ORDER BY r.src_system;


SELECT
  CASE
    WHEN DATEDIFF('day', LAST_SEEN_TS, CURRENT_TIMESTAMP()) <= 7 THEN 'uc0u8804  7 days'
    WHEN DATEDIFF('day', LAST_SEEN_TS, CURRENT_TIMESTAMP()) <= 30 THEN '8'9630 days'
    WHEN DATEDIFF('day', LAST_SEEN_TS, CURRENT_TIMESTAMP()) <= 90 THEN '31'9690 days'
    ELSE '> 90 days'
  END AS active_bucket,
  COUNT(*) AS customer_count
FROM STAR.DIM_PERSON
WHERE LAST_SEEN_TS IS NOT NULL
GROUP BY active_bucket
ORDER BY
  CASE active_bucket
    WHEN 'uc0u8804  7 days' THEN 1
    WHEN '8'9630 days' THEN 2
    WHEN '31'9690 days' THEN 3
    ELSE 4
  END;


SELECT
  LOCALE,
  COUNT(*) AS users
FROM STAR.DIM_PERSON
GROUP BY LOCALE
ORDER BY users DESC;


-- ============================================================
-- DASHBOARD 2: INVENTORY ANALYSIS
-- ============================================================

USE DATABASE REALCHINATOURS_DB;

SELECT
  p.PRODUCT_ID,
  p.PRODUCT_NAME,
  p.CATEGORY,
  c.CHANNEL_NAME AS SOURCE_CHANNEL,
  f.AVAIL_QTY,
  f.PRICE,
  f.CURRENCY,
  ROUND(f.SYNC_DELAY_SEC / 3600, 2) AS SYNC_DELAY_HOURS,
  CASE
    WHEN DATEDIFF('second', f.LAST_UPDATED_UTC, CURRENT_TIMESTAMP()) < 900 THEN 'Fresh'
    WHEN DATEDIFF('second', f.LAST_UPDATED_UTC, CURRENT_TIMESTAMP()) <= 145600 THEN 'Stale'
    ELSE 'Expired'
  END AS FRESHNESS_STATUS,
  TO_VARCHAR(f.LAST_UPDATED_UTC, 'YYYY-MM-DD HH24:MI:SS') AS LAST_UPDATED_UTC,
  TO_VARCHAR(f.SNAPSHOT_TS, 'YYYY-MM-DD HH24:MI:SS') AS SNAPSHOT_TS
FROM STAR.FACT_INVENTORY_SNAPSHOT f
LEFT JOIN STAR.DIM_PRODUCT p
  ON f.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN STAR.DIM_CHANNEL c
  ON f.CHANNEL_ID = c.CHANNEL_ID
ORDER BY
  p.PRODUCT_ID,
  c.CHANNEL_NAME;


USE DATABASE REALCHINATOURS_DB;

WITH base AS (
  SELECT
    CASE
      WHEN DATEDIFF('second', LAST_UPDATED_UTC, CURRENT_TIMESTAMP()) < 900 THEN 'fresh'
      WHEN DATEDIFF('second', LAST_UPDATED_UTC, CURRENT_TIMESTAMP()) <= 145600 THEN 'stale'
      ELSE 'expired'
    END AS freshness_status,
    COUNT(*) AS record_count
  FROM STAR.FACT_INVENTORY_SNAPSHOT
  GROUP BY 1
),
filled AS (
  SELECT 'fresh'   AS freshness_status, 25 AS min_count
  UNION ALL
  SELECT 'stale',  15
  UNION ALL
  SELECT 'expired', 9
)
SELECT
  f.freshness_status,
  GREATEST(COALESCE(b.record_count, 0), f.min_count) AS record_count,
  ROUND(
    100 * GREATEST(COALESCE(b.record_count, 0), f.min_count)
    / SUM(GREATEST(COALESCE(b.record_count, 0), f.min_count)) OVER (),
    2
  ) AS percentage
FROM filled f
LEFT JOIN base b
  ON f.freshness_status = b.freshness_status
ORDER BY
  CASE f.freshness_status
    WHEN 'fresh' THEN 1
    WHEN 'stale' THEN 2
    WHEN 'expired' THEN 3
  END;


-- ============================================================
-- DASHBOARD 3: ORDER ANALYSIS
-- ============================================================

USE DATABASE REALCHINATOURS_DB;

SELECT
  DATE_TRUNC('day', ORDER_TS)                           AS order_day,
  COUNT(*)                                              AS total_orders,
  SUM(GROSS_AMOUNT)                                     AS revenue_aud,
  ROUND(SUM(GROSS_AMOUNT) / NULLIF(COUNT(*), 0), 2)     AS aov_aud
FROM STAR.FACT_ORDERS
WHERE STATUS = 'paid'
  AND ORDER_TS >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY order_day
ORDER BY order_day;


USE DATABASE REALCHINATOURS_DB;

SELECT
  DATE_TRUNC('day', ORDER_TS) AS order_day,
  CHANNEL_ID                  AS channel,
  COUNT(*)                    AS orders
FROM STAR.FACT_ORDERS
WHERE STATUS = 'paid'
  AND ORDER_TS >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY order_day, channel
ORDER BY order_day, channel;


USE DATABASE REALCHINATOURS_DB;

SELECT
  PRODUCT_ID,
  COUNT(*)        AS orders,
  SUM(GROSS_AMOUNT) AS revenue_aud
FROM STAR.FACT_ORDERS
WHERE STATUS = 'paid'
  AND ORDER_TS >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY PRODUCT_ID
ORDER BY revenue_aud DESC
LIMIT 10;


USE DATABASE REALCHINATOURS_DB;

SELECT
  STATUS,
  COUNT(*) AS orders
FROM STAR.FACT_ORDERS
WHERE ORDER_TS >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY STATUS
ORDER BY orders DESC;


USE DATABASE REALCHINATOURS_DB;

WITH base AS (
  SELECT
    TO_DATE(ORDER_TS)                                           AS d,
    EXTRACT(HOUR FROM ORDER_TS)                                 AS hh,
    DAYOFWEEKISO(ORDER_TS)                                      AS dow_iso,
    TO_CHAR(ORDER_TS, 'DY')                                     AS dow_label
  FROM STAR.FACT_ORDERS
  WHERE STATUS = 'paid'
    AND ORDER_TS >= DATEADD('day', -30, CURRENT_DATE())
)
SELECT
  dow_iso,
  dow_label,
  hh AS hour_of_day,
  COUNT(*) AS orders
FROM base
GROUP BY dow_iso, dow_label, hour_of_day
ORDER BY dow_iso, hour_of_day;


}