-- BIGINT id: referenced by tag.tag_id (BIGINT end-to-end)
CREATE TABLE tag_name (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    color SMALLINT,  -- Frontender said to contain only HUE degree
    template_type VARCHAR(8) NOT NULL,
    template VARCHAR(255) NOT NULL,
    direction VARCHAR(4) NOT NULL,
    special_type VARCHAR(16) DEFAULT NULL,  -- null=normal, 'flag'=farm submit, 'alert'=notify, 'block'=auto-drop
    -- MCP set_filter_enabled target: disabled filters are not pushed to
    -- agents by the block-filter sync (twin: add_soc_and_mcp.sql).
    enabled BOOLEAN NOT NULL DEFAULT TRUE
);