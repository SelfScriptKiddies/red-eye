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
    ('favorite_disk_reserve_bytes', '2147483648')
ON CONFLICT (key) DO NOTHING;
