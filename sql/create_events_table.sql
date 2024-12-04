CREATE TABLE events (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    stream TEXT NOT NULL,
    category TEXT NOT NULL,
    position INT NOT NULL,
    payload JSONB NOT NULL,
    sequence_number BIGSERIAL NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
