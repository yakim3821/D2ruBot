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
    forum_reply_count INTEGER,
    is_closed BOOLEAN NOT NULL DEFAULT FALSE,
    is_pinned BOOLEAN NOT NULL DEFAULT FALSE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_scanned_at TIMESTAMPTZ,
    bot_replied_once BOOLEAN NOT NULL DEFAULT FALSE,
    bot_replied_at TIMESTAMPTZ,
    reply_not_before TIMESTAMPTZ,
    reply_skip_reason TEXT,
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

CREATE TABLE IF NOT EXISTS scheduler_settings (
    key TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    schedule_time TEXT NOT NULL DEFAULT '12:00',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS quote_reply_notifications (
    id BIGSERIAL PRIMARY KEY,
    forum_post_id BIGINT NOT NULL UNIQUE,
    forum_topic_id BIGINT,
    post_url TEXT NOT NULL,
    topic_url TEXT,
    source_username TEXT,
    source_user_id BIGINT,
    topic_title TEXT,
    notification_text TEXT,
    quote_text TEXT,
    user_message_text TEXT,
    reply_text TEXT,
    status TEXT NOT NULL,
    error_message TEXT,
    replied_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_summary_runs (
    id BIGSERIAL PRIMARY KEY,
    summary_date DATE NOT NULL UNIQUE,
    status TEXT NOT NULL,
    scheduled_time TEXT,
    topic_title TEXT,
    topic_url TEXT,
    source_topic_count INTEGER NOT NULL DEFAULT 0,
    summary_text TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topic_generation_prompts (
    id BIGSERIAL PRIMARY KEY,
    prompt_code TEXT NOT NULL UNIQUE,
    prompt_name TEXT NOT NULL,
    prompt_text TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_topic_runs (
    id BIGSERIAL PRIMARY KEY,
    topic_date DATE NOT NULL UNIQUE,
    status TEXT NOT NULL,
    scheduled_time TEXT,
    prompt_code TEXT REFERENCES topic_generation_prompts(prompt_code),
    topic_title TEXT,
    topic_body TEXT,
    topic_url TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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

CREATE INDEX IF NOT EXISTS idx_quote_reply_notifications_status_updated
    ON quote_reply_notifications (status, updated_at DESC);

INSERT INTO scan_state (scope, last_scan_at)
VALUES ('forum_section:taverna', NULL)
ON CONFLICT (scope) DO NOTHING;

INSERT INTO scheduler_settings (key, enabled, schedule_time, updated_at)
VALUES
    ('daily_summary', FALSE, '12:00', NOW()),
    ('daily_topic', FALSE, '18:00', NOW())
ON CONFLICT (key) DO NOTHING;

INSERT INTO topic_generation_prompts (
    prompt_code,
    prompt_name,
    prompt_text,
    is_active,
    created_at,
    updated_at
)
VALUES (
    'daily_relationship_topic',
    'Daily relationship forum topic',
    'тему для форума (в стиле dota2.ru), связанную с обществом, деньгами, отношениями, политикой или жизнью.

1. Заголовок:
короткий (1–6 слов)
простой, без сложных формулировок
может быть:
одним словом («капитализм», «свобода»)
утверждением («внешность не важна»)
вопросом («почему в россии не станет лучше»)
фактом/ситуацией («доллар по 85»)
допускается провокация или спорный тейк
стиль — как будто обычный пользователь форума
2. Первый пост:
4–10 предложений
разговорный стиль (без заумных формулировок)
структура:
ввод (что заметил / что происходит)
своё мнение или тейк
немного аргументов или примеров
сомнение или усиление мысли
вопрос аудитории
3. Обязательно добавь:
ощущение «я подумал и вот что понял»
лёгкую провокацию или спорную мысль
обращение к форуму («как думаете», «у вас так же?»)
можно немного негатива или скепсиса
4. Не делай:
слишком грамотно или академично
длинные сложные предложения
нейтральный скучный текст',
    TRUE,
    NOW(),
    NOW()
)
ON CONFLICT (prompt_code) DO UPDATE
SET prompt_name = EXCLUDED.prompt_name,
    prompt_text = EXCLUDED.prompt_text,
    is_active = EXCLUDED.is_active,
    updated_at = NOW();
