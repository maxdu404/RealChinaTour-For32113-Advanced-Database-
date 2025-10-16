USE DATABASE REALCHINATOURS_DB;
CREATE SCHEMA IF NOT EXISTS SILVER;

/* ============================================================
   FINAL VW_CUSTOMER_UNIFIED (Simplified & Optimized)
   - For cleaned datasets (already standardized phone/email)
   - Maintains all logic: dedup, confidence, priority, key hashing
============================================================ */

CREATE OR REPLACE VIEW SILVER.VW_CUSTOMER_UNIFIED AS
WITH
/* ============================================================
   1) SOURCE EXTRACTION
   Each data source (WhatsApp / Store / Website) is read from
   RAW.RAW_USERS where src_system identifies the origin.
   JSON paths and timestamp conversions are adjusted per source.
============================================================ */

-- === WhatsApp users (JSON with nested canonical_person) ===
WHATSAPP AS (
  SELECT
    data:VARIANT_COL:wa_id::string                          AS user_id,
    TRIM(data:VARIANT_COL:name::string)                     AS full_name_raw,
    LOWER(data:VARIANT_COL:email::string)                   AS email_raw,
    data:VARIANT_COL:phone::string                          AS phone_raw,
    data:VARIANT_COL:locale::string                         AS locale_raw,
    data:VARIANT_COL:consent::string                        AS consent_raw,

    /* Time string with time zone → convert to UTC then NTZ */
    CONVERT_TIMEZONE('UTC',
      TRY_TO_TIMESTAMP_TZ(data:VARIANT_COL:last_message_ts::string)
    )::timestamp_ntz                                         AS last_seen_utc,

    NULL::timestamp_ntz                                     AS created_utc,
    NULL::timestamp_ntz                                     AS updated_utc,

    'WHATSAPP'                                              AS src_system,
    'users_whatsapp.json'                                   AS src_file
  FROM RAW.RAW_USERS
  WHERE src_system = 'WHATSAPP'
),


-- === Physical Store CRM users (CSV with C1–C5 columns) ===
STORE AS (
  SELECT
    data:CRM_ID::string              AS user_id,        
    TRIM(data:NAME::string)          AS full_name_raw,  
    LOWER(TRIM(data:EMAIL_ADDRESS::string)) AS email_raw, 
    data:TEL::string                 AS phone_raw,
    'en-AU'                          AS locale_raw,
    NULL::string                     AS consent_raw,
    CONVERT_TIMEZONE('Australia/Sydney','UTC',
      COALESCE(
        TRY_TO_TIMESTAMP_NTZ(data:CREATED_LOCAL::string, 'DD/MM/YYYY HH24:MI:SS'),
        TRY_TO_TIMESTAMP_NTZ(data:CREATED_LOCAL::string) 
      )
    )::timestamp_ntz                 AS created_utc,
    NULL::timestamp_ntz              AS updated_utc,
    NULL::timestamp_ntz              AS last_seen_utc,
    'STORE'                          AS src_system,
    'users_store.csv'                AS src_file
  FROM RAW.RAW_USERS
  WHERE src_system = 'STORE'
),

-- === Website registered users (CSV with descriptive headers) ===
WEBSITE AS (
  SELECT
    data:USER_ID::string                                   AS user_id,
    TRIM(data:FULL_NAME::string)                           AS full_name_raw,
    LOWER(TRIM(data:EMAIL::string))                        AS email_raw,
    TRIM(data:PHONE_NUMBER::string)                        AS phone_raw,
    'en-AU'                                                AS locale_raw,
    NULL::string                                           AS consent_raw,
    TRY_TO_TIMESTAMP_NTZ(data:CREATED_AT::string)          AS created_utc,
    TRY_TO_TIMESTAMP_NTZ(data:UPDATED_AT::string)          AS updated_utc,
    NULL::timestamp_ntz                                    AS last_seen_utc,
    'WEBSITE'                                              AS src_system,
    'users_website.csv'                                    AS src_file
  FROM RAW.RAW_USERS
  WHERE src_system = 'WEBSITE'
),

/* ============================================================
   2) UNION ALL SOURCES
   Combine all user records from the three origins.
============================================================ */
UNIONED AS (
  SELECT * FROM WHATSAPP
  UNION ALL
  SELECT * FROM STORE
  UNION ALL
  SELECT * FROM WEBSITE
),

/* ============================================================
   3) STANDARDIZATION
   Normalize email, phone, locale, and consent fields.
   Convert phone numbers to a consistent international format.
============================================================ */
CLEANED AS (
  SELECT
    user_id,
    full_name_raw,

    -- Normalize and clean email address
    NULLIF(LOWER(TRIM(email_raw)), '') AS email_norm,

    /* ------------------------------------------------------------
       Normalize phone numbers (International + Australian logic)
       ------------------------------------------------------------
       1. If number already starts with '+', keep it (international).
       2. If AU local format (starts with 04...), convert to +61.
       3. If AU landline (starts with 0...), convert to +61.
       4. Otherwise, strip spaces/symbols as fallback.
    ------------------------------------------------------------- */
    CASE
      -- Case 1: already international (+countrycode)
      WHEN REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9+]','') LIKE '+%' THEN
        '+' || REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9+]',''), '[^0-9]', '')

      -- Case 2: Australian mobile (04...) — only if locale/source implies AU
      WHEN (COALESCE(locale_raw, '') = 'en-AU' OR src_system IN ('STORE','WEBSITE'))
           AND REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '^04[0-9]{8}$') THEN
        '+61' || SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''), 2)

      -- Case 3: Australian landline (0...) — only if locale/source implies AU
      WHEN (COALESCE(locale_raw, '') = 'en-AU' OR src_system IN ('STORE','WEBSITE'))
           AND REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '^0[0-9]{8,9}$') THEN
        '+61' || SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''), 2)

      -- Case 4: fallback, return cleaned digits (no country code)
      ELSE NULLIF(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '')
    END AS phone_norm,

    -- Standardize consent values (yes/no)
    CASE
      WHEN LOWER(consent_raw) IN ('yes','y','true','1') THEN 'yes'
      WHEN LOWER(consent_raw) IN ('no','n','false','0')  THEN 'no'
      ELSE NULL
    END AS consent_clean,

    -- Default locale if missing
    COALESCE(locale_raw, 'en-AU') AS locale_clean,

    -- Determine last active time: prefer updated > created > last_seen
    COALESCE(updated_utc, created_utc, last_seen_utc) AS last_seen_utc,

    src_system,
    src_file
FROM UNIONED
),

/* ============================================================
   4) ENRICHMENT & DEDUPLICATION KEYS
   Generate unique person_key and confidence score.
   SHA2 ensures uniqueness across combined sources.
============================================================ */
ENRICHED AS (
  SELECT
    -- same person_key_raw generate logic
    CASE 
      WHEN email_norm IS NOT NULL THEN 'E:' || email_norm
      WHEN phone_norm IS NOT NULL THEN 'P:' || phone_norm
      ELSE 'ID:' || src_system || ':' || NVL(user_id, '')
    END AS person_key_raw,

    -- make the length same
    TO_VARCHAR(SHA2(
      CASE 
        WHEN email_norm IS NOT NULL THEN 'E:' || email_norm
        WHEN phone_norm IS NOT NULL THEN 'P:' || phone_norm
        ELSE 'ID:' || src_system || ':' || NVL(user_id, '')
      END
    , 256)) AS person_key,

    full_name_raw AS full_name,
    email_norm AS email,
    phone_norm AS phone,
    locale_clean AS locale,
    consent_clean AS consent,
    src_system,
    user_id AS src_user_id,
    last_seen_utc,

    -- Confidence level for matching
    CASE
      WHEN email_norm IS NOT NULL AND phone_norm IS NOT NULL THEN 'High'
      WHEN email_norm IS NOT NULL OR  phone_norm IS NOT NULL THEN 'Medium'
      ELSE 'Low'
    END AS match_confidence,

    -- Source ranking priority: Website (1) > Store (2) > WhatsApp (3)
    CASE src_system WHEN 'WEBSITE' THEN 1 WHEN 'STORE' THEN 2 ELSE 3 END AS src_rank
  FROM CLEANED
)

/* ============================================================
   5) FINAL DEDUPLICATION
   Keep only one record per person_key with highest confidence,
   most recent activity, and preferred source ranking.
============================================================ */
SELECT
  person_key,
  full_name,
  email,
  phone,
  locale,
  consent,
  src_system,
  src_user_id,
  last_seen_utc,
  match_confidence
FROM ENRICHED
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY person_key
  ORDER BY
    CASE match_confidence WHEN 'High' THEN 3 WHEN 'Medium' THEN 2 ELSE 1 END DESC,
    last_seen_utc DESC NULLS LAST,
    src_rank ASC
) = 1;
------------------------------------------------------------------------------------------------------------------------------------
-- INVENTORY DOMAIN CLEANING (Unified Inventory View)
-- Consolidates stock data from all sources, standardizes formats, 
-- and computes freshness status relative to latest source update.
------------------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW SILVER.VW_INVENTORY_UNIFIED AS
WITH RAWV AS (
  SELECT
    f.value::VARIANT AS d,
    NULLIF(UPPER(TRIM(r.SRC_SYSTEM)), '') AS src_system_tbl,
    r.SRC_FILE AS src_file,
    r.LOAD_TS
  FROM RAW.RAW_INVENTORY r,
       LATERAL FLATTEN(
         INPUT => IFF(
           IS_ARRAY(COALESCE(TRY_PARSE_JSON(TO_VARCHAR(r.DATA)), r.DATA)),
           COALESCE(TRY_PARSE_JSON(TO_VARCHAR(r.DATA)), r.DATA),
           ARRAY_CONSTRUCT(COALESCE(TRY_PARSE_JSON(TO_VARCHAR(r.DATA)), r.DATA))
         )
       ) f
),

BASE AS (
  SELECT
    /* === Unified Product ID === */
    REGEXP_REPLACE(
      UPPER(
        COALESCE(
          d:"productCode"::string, d:"PRODUCT_CODE"::string,
          d:"PRODUCT_ID"::string, d:"product_id"::string,
          d:"sku"::string, d:"SKU"::string,
          d:"code"::string, d:"CODE"::string
        )
      ),
      '[^A-Z0-9]', ''
    ) AS product_id,

    /* === Location (fallback to GLOBAL) === */
    UPPER(COALESCE(NULLIF(d:"location"::string, ''), 'GLOBAL')) AS location,

    /* === Available Quantity (unified field across all sources) === */
    TRY_TO_NUMBER(
      COALESCE(
        d:"AVAILABLE_QTY"::string, d:"available_qty"::string,
        d:"AVAILABILITY"::string, d:"availability"::string,
        d:"AVAILABLE"::string, d:"available"::string,
        d:"QTY_AVAILABLE"::string, d:"qty_available"::string,
        d:"STOCK_LEVEL"::string, d:"stock_level"::string,
        d:"STOCK_QTY"::string, d:"stock_qty"::string,
        d:"stock"::string
      )
    )::NUMBER(38,0) AS available_qty,

    /* === Price normalization === */
    TRY_TO_DECIMAL(
      REGEXP_SUBSTR(
        TO_VARCHAR(COALESCE(
          d:"price", d:"PRICE",
          d:"PRICE_AUD", d:"price_aud"
        )),
        '([0-9]+(\.[0-9]+)?)', 1, 1, 'e'
      ),
      10, 2
    ) AS price,

    /* === Currency normalization === */
    COALESCE(
      NULLIF(UPPER(TRIM(COALESCE(d:"currency"::string, d:"CURRENCY"::string))), ''),
      'AUD'
    ) AS currency,

    /* === Timestamp normalization === */
    COALESCE(
      TRY_TO_TIMESTAMP_NTZ(d:"lastUpdate"::string),
      TRY_TO_TIMESTAMP_NTZ(d:"LAST_UPDATE"::string),
      TRY_TO_TIMESTAMP_NTZ(d:"LAST_PUSH_EPOCH"::string),
      TO_TIMESTAMP_NTZ(TRY_TO_NUMBER(d:"LAST_PUSH_EPOCH"::string))
    ) AS last_updated_utc,

    COALESCE(
      src_system_tbl,
      NULLIF(UPPER(COALESCE(d:"SOURCE"::string, d:"source"::string)), ''),
      'UNKNOWN'
    ) AS source,

    src_file,
    LOAD_TS
  FROM RAWV
  WHERE NULLIF(
    COALESCE(
      d:"productCode"::string, d:"PRODUCT_CODE"::string,
      d:"product_id"::string, d:"PRODUCT_ID"::string,
      d:"code"::string, d:"CODE"::string
    ), ''
  ) IS NOT NULL
),

PER_SOURCE_LATEST AS (
  SELECT *
  FROM BASE
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY product_id, location, source
    ORDER BY last_updated_utc DESC NULLS LAST, LOAD_TS DESC
  ) = 1
),

AGG AS (
  SELECT
    product_id,
    location,
    COUNT(DISTINCT source) AS source_coverage,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    MIN(available_qty) AS min_avail,
    MAX(available_qty) AS max_avail
  FROM PER_SOURCE_LATEST
  GROUP BY product_id, location
),

ELT_TIME AS (
  SELECT MAX(last_updated_utc) AS elt_time FROM PER_SOURCE_LATEST
)

SELECT
  p.product_id,
  p.location,
  p.available_qty,
  p.price,
  p.currency,
  p.last_updated_utc,
  p.source AS src_system,
  p.src_file,
  e.elt_time,
  DATEDIFF('second', COALESCE(p.last_updated_utc, e.elt_time), e.elt_time) AS sync_delay_sec,
  CASE
    WHEN DATEDIFF('second', COALESCE(p.last_updated_utc, e.elt_time), e.elt_time) < 900 THEN 'fresh'
    WHEN DATEDIFF('second', COALESCE(p.last_updated_utc, e.elt_time), e.elt_time) <= 3600 THEN 'stale'
    ELSE 'expired'
  END AS status,
  a.source_coverage,
  CASE
    WHEN a.source_coverage >= 2
         AND (a.min_price <> a.max_price OR a.min_avail <> a.max_avail)
      THEN 1 ELSE 0
  END AS conflict_flag
FROM PER_SOURCE_LATEST p
JOIN AGG a USING (product_id, location)
CROSS JOIN ELT_TIME e;

---

------------------------------------------------------------------------------------------------------------------------------------
-- Orders Domain Cleaning
-- Purpose: Normalize heterogeneous order data from STORE / WEBSITE / OTA sources.
-- Converts timestamps to UTC, standardizes IDs, amounts, currencies, and status fields.
------------------------------------------------------------------------------------------------------------------------------------
    
CREATE OR REPLACE VIEW SILVER.VW_ORDERS_CLEAN AS
WITH SRC AS (
  SELECT
    COALESCE(TRY_PARSE_JSON(TO_VARCHAR(DATA)), OBJECT_CONSTRUCT()) AS d,
    NULLIF(UPPER(TRIM(SRC_SYSTEM)), '')                             AS src_system,
    SRC_FILE,
    TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(LOAD_TS))                       AS load_ts
  FROM RAW.RAW_ORDERS
),

ORDERS_RAW AS (
  SELECT
    /* === Order ID Extraction (support OTA, Website, Store, Partner) === */
    /* === Order ID (add OTA BOOKINGREF) === */
    COALESCE(
      d:"TXN_ID"::string, d:"CRM_ID"::string,
      d:"ORDER_ID"::string, d:"BOOKING_ID"::string,
      d:"STORE_TXN_ID"::string, d:"ID"::string,
      d:"BOOKINGREF"::string, d:"bookingRef"::string
    ) AS order_id_src,



    /* === Source Normalization === */
    LOWER(COALESCE(d:"SOURCE"::string, d:source::string, src_system)) AS source,

     /* === Order Timestamp ===
       1) Convert Sydney local → UTC when applicable
       2) Handle OTA's double offset (+00:00+11:00) by replacing "+00:00+" with "+"
    */
    COALESCE(
      CONVERT_TIMEZONE('Australia/Sydney','UTC',
        TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(COALESCE(d:"TIME_LOCAL", d:time_local)), 'DD/MM/YYYY HH24:MI')
      )::timestamp_ntz,
      TRY_TO_TIMESTAMP_TZ(
        REGEXP_REPLACE(
          TO_VARCHAR(COALESCE(
            d:"ORDER_TS", d:"BOOKED_AT", d:"CREATED_AT", d:"TXN_DATE",
            d:"BOOKINGDATE", d:"bookingDate"
          )),
          '\\+00:00\\+',
          '+'
        )
      ),
      TRY_TO_TIMESTAMP_NTZ(
        REGEXP_REPLACE(
          TO_VARCHAR(COALESCE(
            d:"ORDER_TS", d:"BOOKED_AT", d:"CREATED_AT", d:"TXN_DATE",
            d:"BOOKINGDATE", d:"bookingDate"
          )),
          '\\+00:00\\+',
          '+'
        )
      )
    ) AS order_ts,
    
    /* === Person identity fields for matching === */
    COALESCE(
      d:"USER_ID"::string, d:"user_id"::string,
      d:"CRM_ID"::string,
      d:"CUSTOMEREMAIL"::string, d:"customerEmail"::string
    ) AS person_id_raw,
    
    -- Extract email if present
    LOWER(TRIM(COALESCE(
      d:"EMAIL"::string, d:"email"::string,
      d:"CUSTOMEREMAIL"::string, d:"customerEmail"::string,
      d:"CUSTOMER_EMAIL"::string
    ))) AS email_raw,
    
    -- Extract phone if present
    TRIM(COALESCE(
      d:"PHONE"::string, d:"phone"::string,
      d:"CUSTOMERPHONE"::string, d:"customerPhone"::string,
      d:"CUSTOMER_PHONE"::string
    )) AS phone_raw,

    /* === Product Identifier (include OTA productCode) === */
    TRIM(UPPER(COALESCE(
      d:"SKU"::string, d:"PRODUCT_CODE"::string, d:"PRODUCT_ID"::string,
      d:"PRODUCTCODE"::string, d:"productCode"::string
    ))) AS product_id,

    /* === Quantity (Default = 1) === */
    COALESCE(
      d:"UNITS"::number, d:"QTY"::number, d:"QUANTITY"::number,
      d:"PAX"::number, 1
    ) AS qty,


    /* === Gross Amount Cleaning: remove non-numeric symbols, keep digits/.- === */
    ROUND(
      TRY_TO_NUMBER(
        REGEXP_REPLACE(
          TO_VARCHAR(
            COALESCE(
              d:"GROSS_AMOUNT_AUD", d:"PRICE_TOTAL_AUD",
              d:"AMOUNT_AUD", d:"PAY_AMOUNT_AUD",
              d:"TOTAL", d:"AMOUNT", d:"PRICE",
              d:gross_amount_aud, d:price_total_aud,
              d:amount_aud, d:pay_amount_aud,
              d:total, d:amount, d:price
            )
          ),
          '[^0-9\\.-]', ''
        )
      ),
      2
    ) AS gross_amount,

    /* === Currency Normalization === */
    CASE
    WHEN
      REGEXP_LIKE(OBJECT_KEYS(d)::STRING, 'GROSS_AMOUNT_AUD|AMOUNT_AUD|PRICE_TOTAL_AUD|PAY_AMOUNT_AUD')
      OR d:"GROSS_AMOUNT_AUD" IS NOT NULL
      OR d:"AMOUNT_AUD" IS NOT NULL
      OR d:"PRICE_TOTAL_AUD" IS NOT NULL
      OR d:"PAY_AMOUNT_AUD" IS NOT NULL
    THEN 'AUD'
    ELSE COALESCE(UPPER(TRIM(d:"CURRENCY"::string)), 'AUD')
  END AS currency,

    /* === Status Standardization === */
    CASE UPPER(TRIM(COALESCE(
      d:"STATUS"::string, d:"ORDER_STATUS"::string,
      d:"PAYMENT_STATUS"::string, d:"BOOKINGSTATUS"::string
    )))
      WHEN 'PAID' THEN 'paid'
      WHEN 'COMPLETED' THEN 'paid'
      WHEN 'SUCCESS' THEN 'paid'
      WHEN 'SUCCESSFUL' THEN 'paid'
      WHEN 'CONFIRMED' THEN 'paid'
      WHEN 'CANCELLED' THEN 'cancelled'
      WHEN 'CANCELED' THEN 'cancelled'
      WHEN 'VOID' THEN 'cancelled'
      WHEN 'REFUNDED' THEN 'refunded'
      WHEN 'FAILED' THEN 'failed'
      WHEN 'PENDING' THEN 'pending'
      ELSE 'unknown'
    END AS status,


    SRC_FILE,
    load_ts,
    src_system

  FROM SRC
),

/* ============================================================
   NORMALIZE PHONE NUMBERS (Same logic as VW_CUSTOMER_UNIFIED)
============================================================ */
ORDERS_WITH_NORMALIZED_PHONE AS (
  SELECT
    *,
    -- Normalize phone to match customer view logic
    CASE
      -- Case 1: already international (+countrycode)
      WHEN REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9+]','') LIKE '+%' THEN
        '+' || REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9+]',''), '[^0-9]', '')

      -- Case 2: Australian mobile (04...) 
      WHEN REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '^04[0-9]{8}$') THEN
        '+61' || SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''), 2)

      -- Case 3: Australian landline (0...)
      WHEN REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '^0[0-9]{8,9}$') THEN
        '+61' || SUBSTR(REGEXP_REPLACE(phone_raw,'[^0-9]',''), 2)

      -- Case 4: fallback, return cleaned digits
      ELSE NULLIF(REGEXP_REPLACE(COALESCE(phone_raw,''),'[^0-9]',''), '')
    END AS phone_norm,
    
    -- Clean email
    NULLIF(email_raw, '') AS email_norm
    
  FROM ORDERS_RAW
)

/* ============================================================
   FINAL OUTPUT: Generate person_key using same logic as customers
============================================================ */
SELECT
  order_id_src,
  source,
  order_ts,
  
  /* === Generate person_key matching VW_CUSTOMER_UNIFIED === */
  TO_VARCHAR(SHA2(
  COALESCE(
    IFF(email_norm IS NOT NULL, 'E:'||email_norm, NULL),
    IFF(phone_norm IS NOT NULL, 'P:'||phone_norm, NULL),
    IFF(person_id_raw IS NOT NULL, 'ID:'||UPPER(source)||':'||person_id_raw, NULL),
    'ORDER:'||order_id_src||':T:'||NVL(TO_VARCHAR(order_ts,'YYYY-MM-DD"T"HH24:MI:SS'),'1970-01-01')
  ), 256)) AS person_key,

  
  -- Optional: Keep for debugging/auditing (can be removed if not needed)
  person_id_raw AS src_customer_id,
  
  product_id,
  qty,
  gross_amount,
  currency,
  status,
  SRC_FILE,
  load_ts

FROM ORDERS_WITH_NORMALIZED_PHONE;
