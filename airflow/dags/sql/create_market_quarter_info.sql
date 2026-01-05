CREATE SCHEMA IF NOT EXISTS "raw_data";

CREATE TABLE IF NOT EXISTS "raw_data"."3Q_market_info" (
    {{ create_columns_with_types }}
);

TRUNCATE TABLE "raw_data"."3Q_market_info";
