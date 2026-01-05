CREATE SCHEMA IF NOT EXISTS "raw_data";

CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    {{ create_columns_with_types }}
);

TRUNCATE TABLE "raw_data"."3Q_market_info";
