CREATE TABLE chain_checkpoints (
    chain_id BIGINT PRIMARY KEY,
    last_processed_height BIGINT NOT NULL,
    last_processed_hash VARCHAR(66) NOT NULL,
    created_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    updated_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT
);

CREATE TABLE chain_blocks (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    block_time BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'CANONICAL',
    finality VARCHAR(16) NOT NULL DEFAULT 'CONFIRMED',
    transaction_count INTEGER NOT NULL DEFAULT 0,
    state_root VARCHAR(66),
    transactions_root VARCHAR(66),
    receipts_root VARCHAR(66),
    gas_limit NUMERIC(78,0),
    gas_used NUMERIC(78,0),
    base_fee_per_gas NUMERIC(78,0),
    raw_block JSONB,
    raw_receipts JSONB,
    chain_version BIGINT NOT NULL DEFAULT 1,
    reorg_id UUID,
    created_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    orphaned_at BIGINT,
    PRIMARY KEY (chain_id, block_hash)
);

CREATE UNIQUE INDEX uq_chain_blocks_canonical_height
    ON chain_blocks (chain_id, block_number)
    WHERE status = 'CANONICAL';
CREATE INDEX idx_chain_blocks_height ON chain_blocks (chain_id, block_number);
CREATE INDEX idx_chain_blocks_parent_hash ON chain_blocks (chain_id, parent_hash);
CREATE INDEX idx_chain_blocks_reorg ON chain_blocks (reorg_id);

CREATE TABLE chain_raw_events (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    tx_index INTEGER,
    log_index INTEGER NOT NULL,
    contract_address VARCHAR(42) NOT NULL,
    topic0 VARCHAR(66),
    topic1 VARCHAR(66),
    topic2 VARCHAR(66),
    topic3 VARCHAR(66),
    event_data TEXT,
    raw_log JSONB NOT NULL,
    parse_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    parser_version INTEGER,
    parse_error TEXT,
    chain_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    reorg_id UUID,
    created_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    parsed_at BIGINT,
    reverted_at BIGINT,
    PRIMARY KEY (chain_id, block_hash, log_index)
);

CREATE INDEX idx_chain_raw_events_pending
    ON chain_raw_events (chain_id, block_number)
    WHERE parse_status IN ('PENDING', 'FAILED');
CREATE INDEX idx_chain_raw_events_contract_topic
    ON chain_raw_events (chain_id, contract_address, topic0);
CREATE INDEX idx_chain_raw_events_tx ON chain_raw_events (chain_id, tx_hash);
CREATE INDEX idx_chain_raw_events_block ON chain_raw_events (chain_id, block_number);
CREATE INDEX idx_chain_raw_events_reorg ON chain_raw_events (reorg_id);

CREATE TABLE chain_transactions (
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    tx_index INTEGER NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    nonce BIGINT NOT NULL,
    value NUMERIC(78,0) NOT NULL DEFAULT 0,
    input TEXT,
    method_id VARCHAR(10),
    method_name VARCHAR(128),
    tx_type SMALLINT,
    gas_limit NUMERIC(78,0),
    gas_used NUMERIC(78,0),
    gas_price NUMERIC(78,0),
    effective_gas_price NUMERIC(78,0),
    transaction_fee NUMERIC(78,0),
    execution_status VARCHAR(16) NOT NULL,
    contract_address VARCHAR(42),
    chain_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    reorg_id UUID,
    raw_transaction JSONB,
    raw_receipt JSONB,
    block_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    reverted_at BIGINT,
    PRIMARY KEY (chain_id, block_hash, tx_hash)
);

CREATE INDEX idx_chain_transactions_hash ON chain_transactions (chain_id, tx_hash);
CREATE INDEX idx_chain_transactions_from ON chain_transactions (chain_id, from_address, block_number DESC);
CREATE INDEX idx_chain_transactions_to ON chain_transactions (chain_id, to_address, block_number DESC);
CREATE INDEX idx_chain_transactions_block ON chain_transactions (chain_id, block_number, tx_index);
CREATE INDEX idx_chain_transactions_reorg ON chain_transactions (reorg_id);

CREATE TABLE chain_transfers (
    id BIGSERIAL PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    tx_hash VARCHAR(66) NOT NULL,
    tx_index INTEGER,
    source_type VARCHAR(24) NOT NULL,
    source_index VARCHAR(128) NOT NULL,
    transfer_type VARCHAR(16) NOT NULL,
    token_address VARCHAR(42),
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    amount NUMERIC(78,0),
    token_id NUMERIC(78,0),
    batch_index INTEGER NOT NULL DEFAULT 0,
    chain_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    reorg_id UUID,
    raw_data JSONB,
    block_time BIGINT NOT NULL,
    created_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    reverted_at BIGINT,
    UNIQUE (chain_id, block_hash, tx_hash, source_type, source_index, batch_index)
);

CREATE INDEX idx_chain_transfers_tx ON chain_transfers (chain_id, tx_hash);
CREATE INDEX idx_chain_transfers_from ON chain_transfers (chain_id, from_address, block_number DESC);
CREATE INDEX idx_chain_transfers_to ON chain_transfers (chain_id, to_address, block_number DESC);
CREATE INDEX idx_chain_transfers_token ON chain_transfers (chain_id, token_address, block_number DESC);
CREATE INDEX idx_chain_transfers_nft ON chain_transfers (chain_id, token_address, token_id);
CREATE INDEX idx_chain_transfers_reorg ON chain_transfers (reorg_id);

CREATE TABLE chain_reorgs (
    reorg_id UUID PRIMARY KEY,
    chain_id BIGINT NOT NULL,
    ancestor_height BIGINT NOT NULL,
    ancestor_hash VARCHAR(66) NOT NULL,
    old_head_height BIGINT NOT NULL,
    old_head_hash VARCHAR(66) NOT NULL,
    new_head_height BIGINT NOT NULL,
    new_head_hash VARCHAR(66) NOT NULL,
    reorg_depth INTEGER NOT NULL,
    status VARCHAR(16) NOT NULL,
    detected_at BIGINT NOT NULL DEFAULT FLOOR(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP))::BIGINT,
    completed_at BIGINT,
    error_message TEXT
);

CREATE INDEX idx_chain_reorgs_chain_time ON chain_reorgs (chain_id, detected_at DESC);
