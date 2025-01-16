CREATE TABLE projections (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    state JSONB NOT NULL,
    version BIGINT NOT NULL,
    source JSONB NOT NULL,
--     last_sequence_number BIGINT NOT NULL,
--     last_updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
