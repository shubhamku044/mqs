CREATE TABLE topics (
    slug VARCHAR(255) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE webhooks (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL REFERENCES topics(slug) ON DELETE CASCADE,
    url VARCHAR(2048) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_webhooks_topic_slug ON webhooks(topic);

CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL REFERENCES topics(slug) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_messages_created_at ON messages(created_at);