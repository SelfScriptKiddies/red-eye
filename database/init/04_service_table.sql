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
    alert_flag_leak BOOLEAN NOT NULL DEFAULT FALSE, -- Алерт при утечке флага в трафике
    -- Транспортный протокол сервиса: TCP или UDP. TCP и UDP сервисы могут
    -- делить одно имя и порт как отдельные строки.
    protocol TEXT NOT NULL DEFAULT 'TCP' CHECK (protocol IN ('TCP', 'UDP')),
    -- TRUE = сервис применяется ко всем текущим и будущим агентам;
    -- FALSE = agent_service содержит полный явный набор агентов.
    is_global BOOLEAN NOT NULL DEFAULT TRUE,
    -- Уникальность сочетания имени, порта и протокола (TCP и UDP сервисы
    -- могут делить имя и порт)
    CONSTRAINT unique_service_name_port_protocol UNIQUE (name, port, protocol)
);

-- Создание индекса на поле port для ускорения поиска по порту
CREATE INDEX IF NOT EXISTS idx_services_port ON service(port);

-- Agent-to-service bindings (which agent monitors which service)
CREATE TABLE IF NOT EXISTS agent_service (
    agent_id VARCHAR(255) NOT NULL,
    service_id BIGINT NOT NULL REFERENCES service(id) ON DELETE CASCADE,
    PRIMARY KEY (agent_id, service_id)
);