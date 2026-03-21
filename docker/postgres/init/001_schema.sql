CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    forum_user_id BIGINT NOT NULL UNIQUE,
    username TEXT NOT NULL,
    profile_url TEXT,
    source TEXT NOT NULL DEFAULT 'forum',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topics (
    id BIGSERIAL PRIMARY KEY,
    forum_topic_id BIGINT NOT NULL UNIQUE,
    forum_section_id BIGINT,
    title TEXT NOT NULL,
    topic_url TEXT NOT NULL,
    author_user_id BIGINT REFERENCES users(forum_user_id),
    created_at_forum TIMESTAMPTZ,
    last_post_at_forum TIMESTAMPTZ,
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scanned_at TIMESTAMPTZ,
    bot_replied_once BOOLEAN NOT NULL DEFAULT FALSE,
    bot_replied_at TIMESTAMPTZ,
    reply_target TEXT NOT NULL DEFAULT 'topic'
);

CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    forum_post_id BIGINT NOT NULL UNIQUE,
    forum_topic_id BIGINT NOT NULL REFERENCES topics(forum_topic_id) ON DELETE CASCADE,
    forum_user_id BIGINT REFERENCES users(forum_user_id),
    reply_to_post_id BIGINT,
    post_url TEXT,
    content_raw TEXT NOT NULL,
    content_text TEXT,
    created_at_forum TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_topic_starter BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bot_replies (
    id BIGSERIAL PRIMARY KEY,
    forum_topic_id BIGINT NOT NULL REFERENCES topics(forum_topic_id) ON DELETE CASCADE,
    forum_post_id BIGINT,
    target_type TEXT NOT NULL CHECK (target_type IN ('topic', 'conversation')),
    target_url TEXT NOT NULL,
    reply_text TEXT NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_style_profiles (
    id BIGSERIAL PRIMARY KEY,
    forum_user_id BIGINT NOT NULL UNIQUE REFERENCES users(forum_user_id) ON DELETE CASCADE,
    source_profile_url TEXT,
    style_summary TEXT,
    lexicon JSONB NOT NULL DEFAULT '{}'::jsonb,
    signature_phrases JSONB NOT NULL DEFAULT '[]'::jsonb,
    preferred_topics JSONB NOT NULL DEFAULT '[]'::jsonb,
    tone TEXT,
    message_length_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    example_messages JSONB NOT NULL DEFAULT '[]'::jsonb,
    confidence_score NUMERIC(5,2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scan_state (
    id BIGSERIAL PRIMARY KEY,
    scope TEXT NOT NULL UNIQUE,
    last_forum_topic_id BIGINT,
    last_forum_post_id BIGINT,
    last_scan_at TIMESTAMPTZ,
    cursor_payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_topics_reply_flags
    ON topics (bot_replied_once, first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_topics_section_last_seen
    ON topics (forum_section_id, first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_posts_topic_created
    ON posts (forum_topic_id, created_at_forum);

CREATE INDEX IF NOT EXISTS idx_posts_user
    ON posts (forum_user_id);

CREATE INDEX IF NOT EXISTS idx_bot_replies_topic
    ON bot_replies (forum_topic_id, created_at DESC);

INSERT INTO scan_state (scope, last_scan_at)
VALUES ('forum_section:taverna', NULL)
ON CONFLICT (scope) DO NOTHING;
