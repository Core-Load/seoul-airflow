CREATE INDEX IF NOT EXISTS idx_rcd_created_at 
ON raw_data.realtime_city_data (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rcd_area_created 
ON raw_data.realtime_city_data (area_name, created_at DESC);