DataBaseBuilder/
├── REALCHINATOUR_RAW.sql       # Defines raw stage schemas and table ingestion
├── REALCHINATOUR_SLIVER.sql    # Cleansing, standardisation, and VW_CUSTOMER_UNIFIED logic
├── REALCHINATOUR_GOLD.sql      # Star schema (dimensions + facts) and final analytics layer
│
├── RAW DATA/
│   ├── inventory/
│   │   ├── inventory_ota.json
│   │   ├── inventory_partner.csv
│   │   └── inventory_store.csv
│   ├── orders/
│   │   ├── orders_ota.csv
│   │   ├── orders_store.csv
│   │   └── orders_website.csv
│   └── users/
│       ├── users_store.csv
│       ├── users_website.csv
│       └── users_whatsapp.json
│
└── Visualize/                   # Optional dashboard or Snowsight visualisations
