ALTER TABLE chain_raw_events
    ADD COLUMN id BIGSERIAL,
    ADD CONSTRAINT uq_chain_raw_events_id UNIQUE (id);

CREATE TABLE chain_event_biz_cursors (
    id BIGSERIAL PRIMARY KEY,
    biz TEXT NOT NULL,
    chain_family TEXT NOT NULL,
    cursor BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT uq_chain_event_biz_cursors_biz_family
        UNIQUE (biz, chain_family)
);
