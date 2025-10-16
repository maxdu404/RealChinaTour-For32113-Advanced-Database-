# Real China Tour Analytics Database

This document provides access instructions for the **Real China Tour** unified analytics database. The pipeline, which integrates data from our Website, Store, OTA, and WhatsApp channels, has been successfully deployed and is ready for use.

The data is structured using a **Bronze → Silver → Gold** model to ensure quality and reliability. For all analytical and reporting purposes, you should use the tables in the `GOLD` schema.

-----
### 🗂️ Project Structure

```
DataBaseBuilder/
├── REALCHINATOUR_RAW.sql     # 1. Ingests raw source data
├── REALCHINATOUR_SLIVER.sql  # 2. Cleans and unifies data
├── REALCHINATOUR_GOLD.sql    # 3. Creates the final analytics model
│
├── RAW DATA/
│   ├── inventory/
│   ├── orders/
│   └── users/
│
└── Visualize/                # Optional: Snowsight / Tableau workbooks
```

-----

### 🔑 How to Access the Data

The entire dataset is live and accessible within the Snowflake database `REALCHINATOURS_DB`. You do not need to run any scripts or upload any data.

1.  **Connect to Snowflake:** Log in to your Snowflake account.
2.  **Set Your Context:** Use the following commands in a new worksheet to set your role and warehouse, and select the correct database.
    ```sql
    USE ROLE YOUR_ROLE_NAME;
    USE WAREHOUSE YOUR_WAREHOUSE_NAME;
    USE DATABASE REALCHINATOURS_DB;
    ```
3.  **Query the Gold Layer:** All analytics-ready tables are in the `GOLD` schema.

#### Key Tables for Analytics:

  * `GOLD.DIM_PERSON`: Unified customer information.
  * `GOLD.DIM_PRODUCT`: Details on tours and travel products.
  * `GOLD.DIM_CHANNEL`: The source channel of an order (e.g., Website, Store).
  * `GOLD.FACT_ORDERS`: Transactional order data.
  * `GOLD.FACT_INVENTORY_SNAPSHOT`: Daily inventory snapshots.

#### Example Query:

> To see the 10 most recent orders with customer details:
>
> ```sql
> SELECT
>     o.ORDER_DATE,
>     p.FULL_NAME,
>     pr.PRODUCT_NAME,
>     o.TOTAL_PRICE
> FROM REALCHINATOURS_DB.GOLD.FACT_ORDERS o
> JOIN REALCHINATOURS_DB.GOLD.DIM_PERSON p ON o.PERSON_KEY = p.PERSON_KEY
> JOIN REALCHINATOURS_DB.GOLD.DIM_PRODUCT pr ON o.PRODUCT_KEY = pr.PRODUCT_KEY
> ORDER BY o.ORDER_DATE DESC
> LIMIT 10;
> ```

-----

### ⚠️ **Important Usage Guidelines**

  * **This is a live production database.** The data is for read-only and analytical purposes.
  * **DO NOT run any of the build scripts** (`REALCHINATOUR_RAW.sql`, `_SLIVER.sql`, `_GOLD.sql`). The pipeline is already deployed.
  * **DO NOT** attempt to `INSERT`, `UPDATE`, or `DELETE` any data in any schema unless you are an authorized administrator. Unauthorized changes can corrupt the dataset.

##
  * If you have questions or require changes, please contact the project owner.


### Getting Started for tutor

1. **Upload Data:** Place all files from the `RAW DATA/` directory into a Snowflake stage 
   - (**Already** in REALCHINATOURS_DB.RAW, you don't need to do buy yourself).
2. Change the `CAT_WH` in `REALCHINATOUR_RAW.sql` to your own `XX_WH`
3. **Execute Scripts:** Run the SQL scripts in the following order:
   - `REALCHINATOUR_RAW.sql`
   - `REALCHINATOUR_SLIVER.sql`
   - `REALCHINATOUR_GOLD.sql`
4. **Analyze:** Query the final tables in the Gold schema or connect a BI tool like Tableau to visualize the results.
> *Last Updated: 16 October 2025*
