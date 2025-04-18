CREATE TABLE tag (
    id SERIAL PRIMARY KEY,
    packet_id INTEGER,
    tag_id INTEGER,
    start_index INTEGER NOT NULL,
    end_index INTEGER NOT NULL,

    CONSTRAINT fk_packet
        FOREIGN KEY(packet_id) 
            REFERENCES packets(id)
            ON DELETE CASCADE,
    CONSTRAINT fk_tag
        FOREIGN KEY(tag_id) 
            REFERENCES tag_name(id)
            ON DELETE CASCADE
);