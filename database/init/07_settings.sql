-- App settings (key-value store for runtime config)
CREATE TABLE IF NOT EXISTS app_settings (
    key VARCHAR(64) PRIMARY KEY,
    value TEXT NOT NULL
);

-- Default settings
INSERT INTO app_settings (key, value) VALUES
    ('farm_url', ''),
    ('farm_enabled', 'false')
ON CONFLICT (key) DO NOTHING;
