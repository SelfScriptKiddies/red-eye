-- Create partitioned 'tag' table (filter matches on packets)
-- No FK to packets — cleanup handled by partition drops
-- BIGINT ids end-to-end (packet_id -> packets.id, tag_id -> tag_name.id)
CREATE TABLE IF NOT EXISTS tag (
    id BIGSERIAL,
    packet_id BIGINT NOT NULL,
    tag_id BIGINT NOT NULL,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,
    timestamp BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    PRIMARY KEY (id, timestamp),
    CONSTRAINT fk_tag
        FOREIGN KEY(tag_id)
            REFERENCES tag_name(id)
            ON DELETE CASCADE
) PARTITION BY RANGE (timestamp);

CREATE INDEX IF NOT EXISTS idx_tag_packet_id ON tag(packet_id) INCLUDE (tag_id);
