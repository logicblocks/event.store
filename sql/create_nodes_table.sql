CREATE TABLE nodes (
    id TEXT NOT NULL,
    last_seen TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
