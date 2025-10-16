-- 1. Set role and create warehouse
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS CAT_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

USE WAREHOUSE CAT_WH;


-- 2. Create database (if not exists)
CREATE DATABASE IF NOT EXISTS REALCHINATOURS_DB;
USE DATABASE REALCHINATOURS_DB;

-- 3. Create three schemas for the three layers
CREATE SCHEMA IF NOT EXISTS RAW;     -- landing / raw layer
CREATE SCHEMA IF NOT EXISTS SILVER;  -- cleaned / transformed layer
CREATE SCHEMA IF NOT EXISTS STAR;    -- analytics / reporting layer


-- 4. Create 3 main RAW tables for semi-structured data (no overdesign)
USE SCHEMA RAW;

-- Users from multiple channels (JSON/CSV/XML → VARIANT)
CREATE OR REPLACE TABLE RAW_USERS (
  data VARIANT,
  src_system STRING,
  src_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Orders from website / OTA / store
CREATE OR REPLACE TABLE RAW_ORDERS (
  data VARIANT,
  src_system STRING,
  src_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Inventory from suppliers / partners
CREATE OR REPLACE TABLE RAW_INVENTORY (
  data VARIANT,
  src_system STRING,
  src_file STRING,
  load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);


-- 4.2 Load each staging table into corresponding RAW main table

-- === USERS ===
INSERT INTO RAW_USERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'STORE', 'users_store.csv' FROM LOAD_USERS_STORE;

INSERT INTO RAW_USERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'WEBSITE', 'users_website.csv' FROM LOAD_USERS_WEBSITE;

INSERT INTO RAW_USERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'WHATSAPP', 'users_whatsapp.json' FROM LOAD_USERS_WHATSAPP;

-- === ORDERS ===
INSERT INTO RAW_ORDERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'STORE', 'orders_store.csv' FROM LOAD_ORDERS_STORE;

INSERT INTO RAW_ORDERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'WEBSITE', 'orders_website.csv' FROM LOAD_ORDERS_WEBSITE;

INSERT INTO RAW_ORDERS (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'OTA', 'orders_ota.csv' FROM LOAD_ORDERS_OTA;


-- === INVENTORY ===
INSERT INTO RAW_INVENTORY (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'STORE', 'inventory_store.csv' FROM LOAD_INV_STORE;

INSERT INTO RAW_INVENTORY (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'PARTNER', 'inventory_partner.csv' FROM LOAD_INV_PARTNER;

INSERT INTO RAW_INVENTORY (data, src_system, src_file)
SELECT OBJECT_CONSTRUCT(*), 'OTA', 'inventory_ota.json' FROM LOAD_INV_OTA_JSON;


-- 4.3 Data volume checks

-- Basic count per main table
SELECT 'RAW_USERS' AS table_name, COUNT(*) AS record_count FROM RAW_USERS;
SELECT 'RAW_ORDERS' AS table_name, COUNT(*) AS record_count FROM RAW_ORDERS;
SELECT 'RAW_INVENTORY' AS table_name, COUNT(*) AS record_count FROM RAW_INVENTORY;



