CREATE TABLE subscriptions (
    id TEXT NOT NULL,
    "group" TEXT NOT NULL,
    event_sources JSONB NOT NULL,
    PRIMARY KEY (id, "group"),
    UNIQUE (id, "group")
);
