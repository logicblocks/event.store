CREATE TABLE projections (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    source JSONB NOT NULL,
    state JSONB NOT NULL,
    metadata JSONB NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
