-- App settings (key-value store for runtime config)
CREATE TABLE IF NOT EXISTS app_settings (
    key VARCHAR(64) PRIMARY KEY,
    value TEXT NOT NULL
);

-- Default settings
INSERT INTO app_settings (key, value) VALUES
    ('farm_url', ''),
    ('farm_enabled', 'false'),
    ('farm_password', ''),
    ('favorite_max_total_bytes', '10737418240'),
    ('favorite_segment_bytes', '268435456'),
    ('favorite_disk_reserve_bytes', '2147483648'),
    ('ai_enabled', 'false'),
    ('ai_model', 'typesafe/jev-1.13'),
    ('ai_threshold', '0.7'),
    ('ai_max_per_tick', '10'),
    ('ai_stream_debounce_ms', '2000'),
    ('ai_stream_min_interval_secs', '10'),
    ('ai_stream_max_bytes', '65536'),
    ('ai_stream_max_entries', '200'),
    ('ai_entry_max_bytes', '8192'),
    ('ai_daily_cost_limit', '5.0'),
    ('ai_verdict_retention_days', '30')
ON CONFLICT (key) DO NOTHING;
