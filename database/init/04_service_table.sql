-- Create 'services' table
-- BIGINT id: referenced by stream.service_id, agent_service.service_id and
-- proto_file.service_id (BIGINT end-to-end)
CREATE TABLE IF NOT EXISTS service (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL, -- Имя сервиса
    port INTEGER NOT NULL, -- Порт сервиса
    chunked_transfer_encoding BOOLEAN NOT NULL DEFAULT FALSE, -- Автоматически раскодировать chunked transfer encoding
    urldecode BOOLEAN NOT NULL DEFAULT FALSE, -- Автоматически проводить urldecode пакетов
    merge_adjacent_packets BOOLEAN NOT NULL DEFAULT FALSE, -- Автоматически склеивать соседние пакеты в одном направлении
    inflate_websockets BOOLEAN NOT NULL DEFAULT FALSE, -- Автоматически разархивировать сжатые WebSocket-пакеты
    decrypt_tls BOOLEAN NOT NULL DEFAULT FALSE, -- Автоматически расшифровывать TLS-трафик
    http_parse BOOLEAN NOT NULL DEFAULT FALSE, -- HTTP/1.1 L7 парсинг и автосклейка пакетов
    http2_parse BOOLEAN NOT NULL DEFAULT FALSE, -- HTTP/2 парсинг через harpoon proxy
    CONSTRAINT unique_service_port UNIQUE (name, port) -- Уникальность сочетания имени сервиса и порта
);

-- Создание индекса на поле port для ускорения поиска по порту
CREATE INDEX IF NOT EXISTS idx_services_port ON service(port);

-- Agent-to-service bindings (which agent monitors which service)
CREATE TABLE IF NOT EXISTS agent_service (
    agent_id VARCHAR(255) NOT NULL,
    service_id BIGINT NOT NULL REFERENCES service(id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id, service_id)
);