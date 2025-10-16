-- ============================================================
-- DIM_PERSON (Customer Dimension Table)
-- ============================================================

-- Ensure database and schema exist
USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

-- ============================================================
-- ① Create the dimension table using CTAS (no INSERT used)
-- ============================================================
CREATE OR REPLACE TABLE STAR.DIM_PERSON AS
WITH SRC AS (
  SELECT
    PERSON_KEY,
    FULL_NAME,
    EMAIL,
    PHONE,
    LOCALE,
    SRC_SYSTEM,
    SRC_USER_ID,
    LAST_SEEN_UTC,
    MATCH_CONFIDENCE
  FROM SILVER.VW_CUSTOMER_UNIFIED
),

-- === Determine the first channel where each person appeared ===
FIRST_SEEN AS (
  SELECT
    PERSON_KEY,
    SRC_SYSTEM AS FIRST_SEEN_CHANNEL
  FROM (
    SELECT
      PERSON_KEY,
      SRC_SYSTEM,
      LAST_SEEN_UTC,
      ROW_NUMBER() OVER (
        PARTITION BY PERSON_KEY
        ORDER BY LAST_SEEN_UTC ASC NULLS LAST
      ) AS rn_first
    FROM SRC
  )
  WHERE rn_first = 1
),

-- === Capture the latest record per person ===
LATEST AS (
  SELECT
    PERSON_KEY,
    FULL_NAME,
    LOCALE,
    SRC_SYSTEM AS SRC_SYSTEM_LATEST,
    SRC_USER_ID AS SRC_USER_ID_LATEST,
    LAST_SEEN_UTC AS LAST_SEEN_TS,
    MATCH_CONFIDENCE
  FROM (
    SELECT
      PERSON_KEY,
      FULL_NAME,
      LOCALE,
      SRC_SYSTEM,
      SRC_USER_ID,
      LAST_SEEN_UTC,
      MATCH_CONFIDENCE,
      ROW_NUMBER() OVER (
        PARTITION BY PERSON_KEY
        ORDER BY LAST_SEEN_UTC DESC NULLS LAST
      ) AS rn_latest
    FROM SRC
  )
  WHERE rn_latest = 1
)

-- === Final output ===
SELECT
  s.PERSON_KEY,
  LOWER(NULLIF(TRIM(s.EMAIL), ''))                         AS EMAIL_NORM,
  REGEXP_REPLACE(NULLIF(TRIM(s.PHONE), ''), '[^0-9+]', '') AS PHONE_NORM,
  l.FULL_NAME,
  l.LOCALE,
  LOWER(f.FIRST_SEEN_CHANNEL)                              AS FIRST_SEEN_CHANNEL,
  l.LAST_SEEN_TS,
  LOWER(l.SRC_SYSTEM_LATEST)                               AS SRC_SYSTEM_LATEST,
  l.SRC_USER_ID_LATEST,
  l.MATCH_CONFIDENCE
FROM (
  SELECT
    PERSON_KEY,
    MAX(EMAIL) AS EMAIL,
    MAX(PHONE) AS PHONE
  FROM SRC
  GROUP BY PERSON_KEY
) s
JOIN LATEST l
  ON s.PERSON_KEY = l.PERSON_KEY
JOIN FIRST_SEEN f
  ON s.PERSON_KEY = f.PERSON_KEY;

-- ============================================================
-- ② Add surrogate key and constraints (IDENTITY cannot be created in CTAS)
-- ============================================================

ALTER TABLE STAR.DIM_PERSON ADD COLUMN PERSON_ID NUMBER;

UPDATE STAR.DIM_PERSON t
SET PERSON_ID = s.rn
FROM (
  SELECT PERSON_KEY, ROW_NUMBER() OVER (ORDER BY PERSON_KEY) AS rn
  FROM STAR.DIM_PERSON
) s
WHERE t.PERSON_KEY = s.PERSON_KEY;

ALTER TABLE STAR.DIM_PERSON ADD CONSTRAINT PK_DIM_PERSON PRIMARY KEY (PERSON_ID);
ALTER TABLE STAR.DIM_PERSON ADD CONSTRAINT UQ_DIM_PERSON_PERSON_KEY UNIQUE (PERSON_KEY);


-- ============================================================
-- PRODUCT DIMENSION TABLE
-- ============================================================

-- Ensure database and schema exist
USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

-- 1) Create product dimension using CTAS from orders and inventory
CREATE OR REPLACE TABLE STAR.DIM_PRODUCT AS
WITH SRC AS (
  SELECT DISTINCT UPPER(TRIM(PRODUCT_ID)) AS PRODUCT_ID
  FROM SILVER.VW_ORDERS_CLEAN
  UNION
  SELECT DISTINCT UPPER(TRIM(PRODUCT_ID)) AS PRODUCT_ID
  FROM SILVER.VW_INVENTORY_UNIFIED
)
SELECT
  PRODUCT_ID,
  INITCAP(REPLACE(PRODUCT_ID, '-', ' ')) AS PRODUCT_NAME,
  CASE
    WHEN PRODUCT_ID LIKE 'SYD%' THEN 'City Tour'
    WHEN PRODUCT_ID LIKE 'TAS%' THEN 'Island Tour'
    ELSE 'General'
  END AS CATEGORY
FROM SRC;

-- 2) Add surrogate key (primary key) and unique constraint
ALTER TABLE STAR.DIM_PRODUCT ADD COLUMN PRODUCT_ID_KEY NUMBER;

UPDATE STAR.DIM_PRODUCT t
SET PRODUCT_ID_KEY = s.rn
FROM (
  SELECT PRODUCT_ID, ROW_NUMBER() OVER (ORDER BY PRODUCT_ID) AS rn
  FROM STAR.DIM_PRODUCT
) s
WHERE t.PRODUCT_ID = s.PRODUCT_ID;

ALTER TABLE STAR.DIM_PRODUCT ADD CONSTRAINT PK_DIM_PRODUCT PRIMARY KEY (PRODUCT_ID_KEY);
ALTER TABLE STAR.DIM_PRODUCT ADD CONSTRAINT UQ_DIM_PRODUCT_PRODUCT_ID UNIQUE (PRODUCT_ID);


-- ============================================================
-- CHANNEL DIMENSION TABLE
-- ============================================================

-- Ensure database and schema exist
USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

-- 1) Create channel dimension from all cleaned source views
CREATE OR REPLACE TABLE STAR.DIM_CHANNEL AS
WITH SRC AS (
  SELECT LOWER(NULLIF(TRIM(SOURCE), '')) AS CHANNEL_SRC
  FROM SILVER.VW_ORDERS_CLEAN
  UNION
  SELECT LOWER(NULLIF(TRIM(SRC_SYSTEM), '')) AS CHANNEL_SRC
  FROM SILVER.VW_INVENTORY_UNIFIED
  UNION
  SELECT LOWER(NULLIF(TRIM(SRC_SYSTEM), '')) AS CHANNEL_SRC
  FROM SILVER.VW_CUSTOMER_UNIFIED
)
SELECT
  CHANNEL_SRC AS CHANNEL_ID,      -- Business key (lowercase)
  INITCAP(CHANNEL_SRC) AS CHANNEL_NAME,  -- Display name
  CASE
    WHEN CHANNEL_SRC IN ('website', 'ota') THEN 'Online'
    WHEN CHANNEL_SRC IN ('store') THEN 'Offline'
    ELSE 'Partner'
  END AS CHANNEL_TYPE
FROM SRC
WHERE CHANNEL_SRC IS NOT NULL;

-- 2) Add surrogate key (primary key) and unique constraint
ALTER TABLE STAR.DIM_CHANNEL ADD COLUMN CHANNEL_KEY NUMBER;

UPDATE STAR.DIM_CHANNEL t
SET CHANNEL_KEY = s.rn
FROM (
  SELECT CHANNEL_ID, ROW_NUMBER() OVER (ORDER BY CHANNEL_ID) AS rn
  FROM STAR.DIM_CHANNEL
) s
WHERE t.CHANNEL_ID = s.CHANNEL_ID;

ALTER TABLE STAR.DIM_CHANNEL ADD CONSTRAINT PK_DIM_CHANNEL PRIMARY KEY (CHANNEL_KEY);
ALTER TABLE STAR.DIM_CHANNEL ADD CONSTRAINT UQ_DIM_CHANNEL_CHANNEL_ID UNIQUE (CHANNEL_ID);

-- 3) Remove potential null rows (safety cleanup)
DELETE FROM STAR.DIM_CHANNEL WHERE CHANNEL_ID IS NULL;

-- ============================================================
-- FACT ORDERS TABLE
-- ============================================================

-- Ensure database and schema exist
USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

--  Create fact orders table using CTAS
CREATE OR REPLACE TABLE STAR.FACT_ORDERS AS
WITH V AS (
  SELECT
    ORDER_ID_SRC,
    SOURCE,
    ORDER_TS,
    PERSON_KEY,
    PRODUCT_ID,
    QTY,
    GROSS_AMOUNT,
    CURRENCY,
    STATUS
  FROM SILVER.VW_ORDERS_CLEAN
)
SELECT
  -- Surrogate key (string-based sequential ID for readability)
  CONCAT('ORD_', LPAD(ROW_NUMBER() OVER (ORDER BY v.ORDER_TS, v.ORDER_ID_SRC), 6, '0')) AS ORDER_KEY,

  -- Source system order ID
  v.ORDER_ID_SRC,

  -- Foreign keys aligned with business keys
  d_p.PERSON_KEY  AS PERSON_KEY,   -- Customer key
  d_pr.PRODUCT_ID AS PRODUCT_ID,   -- Product key
  d_c.CHANNEL_ID  AS CHANNEL_ID,   -- Channel key (lowercase)

  -- Fact attributes
  v.ORDER_TS,
  v.QTY::NUMBER AS QTY,
  v.GROSS_AMOUNT::NUMBER AS GROSS_AMOUNT,
  v.CURRENCY,
  v.STATUS
FROM V v
LEFT JOIN STAR.DIM_PERSON  d_p ON v.PERSON_KEY = d_p.PERSON_KEY
LEFT JOIN STAR.DIM_PRODUCT d_pr ON v.PRODUCT_ID = d_pr.PRODUCT_ID
LEFT JOIN STAR.DIM_CHANNEL d_c ON LOWER(v.SOURCE) = d_c.CHANNEL_ID;


-- ============================================================
-- FACT INVENTORY SNAPSHOT TABLE
-- ============================================================

-- Ensure database and schema exist
USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

-- 1) Create inventory snapshot fact table
CREATE OR REPLACE TABLE STAR.FACT_INVENTORY_SNAPSHOT AS
WITH V AS (
  SELECT
    UPPER(TRIM(PRODUCT_ID)) AS PRODUCT_ID,
    AVAILABLE_QTY,
    PRICE,
    CURRENCY,
    LAST_UPDATED_UTC,
    LOWER(TRIM(SRC_SYSTEM)) AS SRC_SYSTEM,
    -- Sync delay in seconds
    DATEDIFF('second', LAST_UPDATED_UTC, CURRENT_TIMESTAMP()) AS SYNC_DELAY_SEC
  FROM SILVER.VW_INVENTORY_UNIFIED
)
SELECT
  CURRENT_TIMESTAMP() AS SNAPSHOT_TS,     -- Snapshot timestamp
  p.PRODUCT_ID AS PRODUCT_ID,             -- Product business key
  c.CHANNEL_ID AS CHANNEL_ID,             -- Channel business key (lowercase)
  v.AVAILABLE_QTY AS AVAIL_QTY,
  v.PRICE::NUMBER(10,2) AS PRICE,
  v.CURRENCY AS CURRENCY,
  v.SYNC_DELAY_SEC::NUMBER AS SYNC_DELAY_SEC,
  v.LAST_UPDATED_UTC AS LAST_UPDATED_UTC  -- Original source timestamp
FROM V v
LEFT JOIN STAR.DIM_PRODUCT p ON v.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN STAR.DIM_CHANNEL c ON v.SRC_SYSTEM = c.CHANNEL_ID;

-- ============================================================
-- PERSON IDENTITY BRIDGE TABLE
-- ============================================================

USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS STAR;

CREATE OR REPLACE TABLE STAR.BRIDGE_PERSON_IDENTITY_MAP AS
SELECT
  PERSON_KEY,
  SRC_SYSTEM AS SRC_SYSTEM,
  SRC_USER_ID AS SRC_CUSTOMER_ID,
  MATCH_CONFIDENCE,
  CURRENT_TIMESTAMP() AS LOADED_AT
FROM SILVER.VW_CUSTOMER_UNIFIED;

