from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .exceptions import DatabaseError
from .parsers import ForumUserRecord, PostRecord, TopicRecord, UserProfilePostRecord


DB_TIMEZONE = "Europe/Moscow"


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    name: str
    user: str
    password: str


class Database:
    def __init__(self, settings: DatabaseSettings) -> None:
        self.settings = settings
        self.ensure_runtime_schema()

    def _connect(self):
        try:
            import psycopg
        except ImportError as exc:
            raise DatabaseError(
                "psycopg is not installed. Install it with `pip install psycopg[binary]`."
            ) from exc

        try:
            return psycopg.connect(
                host=self.settings.host,
                port=self.settings.port,
                dbname=self.settings.name,
                user=self.settings.user,
                password=self.settings.password,
                options=f"-c timezone={DB_TIMEZONE}",
            )
        except Exception as exc:
            raise DatabaseError(f"Failed to connect to PostgreSQL: {exc}") from exc

    def upsert_user(self, user: ForumUserRecord | None) -> None:
        if user is None:
            return

        sql = """
        INSERT INTO users (forum_user_id, username, profile_url)
        VALUES (%s, %s, %s)
        ON CONFLICT (forum_user_id) DO UPDATE
        SET username = EXCLUDED.username,
            profile_url = COALESCE(EXCLUDED.profile_url, users.profile_url),
            updated_at = NOW()
        """
        self._execute(sql, (user.forum_user_id, user.username, user.profile_url))

    def upsert_topic(self, topic: TopicRecord) -> None:
        if topic.author is not None:
            self.upsert_user(topic.author)

        sql = """
        INSERT INTO topics (
            forum_topic_id,
            forum_section_id,
            title,
            topic_url,
            author_user_id,
            created_at_forum,
            last_post_at_forum,
            forum_reply_count,
            is_closed,
            is_pinned,
            last_scanned_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (forum_topic_id) DO UPDATE
        SET forum_section_id = EXCLUDED.forum_section_id,
            title = EXCLUDED.title,
            topic_url = EXCLUDED.topic_url,
            author_user_id = COALESCE(EXCLUDED.author_user_id, topics.author_user_id),
            created_at_forum = COALESCE(EXCLUDED.created_at_forum, topics.created_at_forum),
            last_post_at_forum = COALESCE(EXCLUDED.last_post_at_forum, topics.last_post_at_forum),
            forum_reply_count = COALESCE(EXCLUDED.forum_reply_count, topics.forum_reply_count),
            is_closed = EXCLUDED.is_closed,
            is_pinned = EXCLUDED.is_pinned,
            last_scanned_at = NOW()
        """
        self._execute(
            sql,
            (
                topic.forum_topic_id,
                topic.forum_section_id,
                topic.title,
                topic.topic_url,
                topic.author.forum_user_id if topic.author else None,
                topic.created_at_forum,
                topic.last_post_at_forum,
                topic.forum_reply_count,
                topic.is_closed,
                topic.is_pinned,
            ),
        )

    def upsert_post(self, post: PostRecord) -> None:
        if post.author is not None:
            self.upsert_user(post.author)

        sql = """
        INSERT INTO posts (
            forum_post_id,
            forum_topic_id,
            forum_user_id,
            reply_to_post_id,
            post_url,
            content_raw,
            content_text,
            created_at_forum,
            is_topic_starter
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (forum_post_id) DO UPDATE
        SET forum_user_id = COALESCE(EXCLUDED.forum_user_id, posts.forum_user_id),
            reply_to_post_id = COALESCE(EXCLUDED.reply_to_post_id, posts.reply_to_post_id),
            post_url = COALESCE(EXCLUDED.post_url, posts.post_url),
            content_raw = EXCLUDED.content_raw,
            content_text = EXCLUDED.content_text,
            created_at_forum = COALESCE(EXCLUDED.created_at_forum, posts.created_at_forum),
            is_topic_starter = EXCLUDED.is_topic_starter
        """
        self._execute(
            sql,
            (
                post.forum_post_id,
                post.forum_topic_id,
                post.author.forum_user_id if post.author else None,
                post.reply_to_post_id,
                post.post_url,
                post.content_raw,
                post.content_text,
                post.created_at_forum,
                post.is_topic_starter,
            ),
        )

    def update_scan_state(self, scope: str, topic_id: int | None = None, post_id: int | None = None) -> None:
        sql = """
        INSERT INTO scan_state (scope, last_forum_topic_id, last_forum_post_id, last_scan_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (scope) DO UPDATE
        SET last_forum_topic_id = COALESCE(EXCLUDED.last_forum_topic_id, scan_state.last_forum_topic_id),
            last_forum_post_id = COALESCE(EXCLUDED.last_forum_post_id, scan_state.last_forum_post_id),
            last_scan_at = NOW()
        """
        self._execute(sql, (scope, topic_id, post_id))

    def get_new_topics(self, limit: int = 50) -> list[dict[str, Any]]:
        sql = """
        SELECT forum_topic_id, title, topic_url, first_seen_at, bot_replied_once
        FROM topics
        WHERE bot_replied_once = FALSE
        ORDER BY first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_recently_seen_topics(self, limit: int = 10) -> list[dict[str, Any]]:
        sql = """
        SELECT
            forum_topic_id,
            title,
            topic_url,
            created_at_forum,
            first_seen_at,
            forum_reply_count,
            reply_not_before,
            is_closed,
            is_pinned,
            bot_replied_once
        FROM topics
        ORDER BY first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_dashboard_status(self) -> dict[str, Any]:
        sql = """
        SELECT
            (SELECT count(*) FROM topics) AS topics_total,
            (SELECT count(*) FROM topics WHERE bot_replied_once = FALSE) AS topics_unreplied,
            (
                SELECT count(*)
                FROM topics
                WHERE bot_replied_once = FALSE
                  AND is_closed = FALSE
                  AND is_pinned = FALSE
                  AND reply_not_before IS NOT NULL
                  AND reply_not_before <= NOW()
            ) AS topics_ready_to_reply,
            (
                SELECT count(*)
                FROM topics
                WHERE bot_replied_once = FALSE
                  AND is_closed = FALSE
                  AND is_pinned = FALSE
                  AND reply_not_before IS NOT NULL
                  AND reply_not_before > NOW()
            ) AS topics_waiting_delay,
            (SELECT count(*) FROM bot_replies) AS bot_replies_total,
            (SELECT count(*) FROM bot_replies WHERE status = 'llm_auto_published') AS bot_auto_published
        """
        rows = self._fetch_all(sql, ())
        return rows[0] if rows else {}

    def get_waiting_topics(self, limit: int = 50) -> list[dict[str, Any]]:
        sql = """
        SELECT
            forum_topic_id,
            title,
            topic_url,
            forum_reply_count,
            reply_not_before,
            created_at_forum,
            first_seen_at
        FROM topics
        WHERE bot_replied_once = FALSE
          AND is_closed = FALSE
          AND is_pinned = FALSE
          AND reply_not_before IS NOT NULL
          AND reply_not_before > NOW()
        ORDER BY reply_not_before ASC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_ready_topics(self, limit: int = 50) -> list[dict[str, Any]]:
        sql = """
        SELECT
            forum_topic_id,
            title,
            topic_url,
            forum_reply_count,
            reply_not_before,
            created_at_forum,
            first_seen_at
        FROM topics
        WHERE bot_replied_once = FALSE
          AND is_closed = FALSE
          AND is_pinned = FALSE
          AND reply_not_before IS NOT NULL
          AND reply_not_before <= NOW()
        ORDER BY reply_not_before ASC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_recent_bot_replies(self, limit: int = 50) -> list[dict[str, Any]]:
        sql = """
        SELECT
            br.created_at,
            br.forum_topic_id,
            t.title,
            br.target_type,
            br.target_url,
            br.status,
            LEFT(br.reply_text, 280) AS reply_preview
        FROM bot_replies br
        LEFT JOIN topics t ON t.forum_topic_id = br.forum_topic_id
        ORDER BY br.created_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_recent_failures(self, limit: int = 50) -> list[dict[str, Any]]:
        sql = """
        SELECT
            br.created_at,
            br.forum_topic_id,
            t.title,
            br.target_type,
            br.status,
            br.error_message
        FROM bot_replies br
        LEFT JOIN topics t ON t.forum_topic_id = br.forum_topic_id
        WHERE br.error_message IS NOT NULL
           OR br.status LIKE '%%failed%%'
        ORDER BY br.created_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def topic_needs_reply_schedule(self, forum_topic_id: int) -> bool:
        sql = """
        SELECT reply_not_before
        FROM topics
        WHERE forum_topic_id = %s
        LIMIT 1
        """
        rows = self._fetch_all(sql, (forum_topic_id,))
        if not rows:
            return True
        return rows[0]["reply_not_before"] is None

    def get_topics_pending_draft(self, limit: int = 20) -> list[dict[str, Any]]:
        sql = """
        SELECT t.forum_topic_id, t.title, t.topic_url
        FROM topics t
        WHERE t.bot_replied_once = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM bot_replies br
              WHERE br.forum_topic_id = t.forum_topic_id
                AND br.target_type = 'conversation'
                AND br.status = 'draft_sent'
          )
        ORDER BY t.first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_topics_pending_llm_draft(self, limit: int = 20) -> list[dict[str, Any]]:
        sql = """
        SELECT t.forum_topic_id, t.title, t.topic_url
        FROM topics t
        WHERE t.bot_replied_once = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM bot_replies br
              WHERE br.forum_topic_id = t.forum_topic_id
                AND br.target_type = 'conversation'
                AND br.status = 'llm_draft_sent'
          )
        ORDER BY t.first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_recent_topics_pending_auto_reply(
        self,
        max_age_days: int = 3,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT t.forum_topic_id, t.title, t.topic_url
        FROM topics t
        WHERE t.bot_replied_once = FALSE
          AND t.is_closed = FALSE
          AND t.is_pinned = FALSE
          AND COALESCE(t.created_at_forum, t.first_seen_at) >= NOW() - (%s * INTERVAL '1 day')
          AND t.reply_not_before IS NOT NULL
          AND t.reply_not_before <= NOW()
        ORDER BY COALESCE(t.created_at_forum, t.first_seen_at) DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (max_age_days, limit))

    def get_topics_created_since(
        self,
        hours: int,
        forum_section_id: int | None = None,
        exclude_pinned: bool = True,
        exclude_closed: bool = True,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        filters = [
            "COALESCE(t.created_at_forum, t.first_seen_at) >= NOW() - (%s * INTERVAL '1 hour')",
        ]
        params: list[Any] = [hours]
        if forum_section_id is not None:
            filters.append("t.forum_section_id = %s")
            params.append(forum_section_id)
        if exclude_pinned:
            filters.append("t.is_pinned = FALSE")
        if exclude_closed:
            filters.append("t.is_closed = FALSE")
        params.append(limit)

        sql = f"""
        SELECT
            t.forum_topic_id,
            t.title,
            t.topic_url,
            t.created_at_forum,
            t.first_seen_at,
            t.forum_reply_count,
            t.is_closed,
            t.is_pinned
        FROM topics t
        WHERE {' AND '.join(filters)}
        ORDER BY COALESCE(t.created_at_forum, t.first_seen_at) DESC
        LIMIT %s
        """
        return self._fetch_all(sql, tuple(params))

    def get_recent_topic_titles(
        self,
        hours: int,
        forum_section_id: int | None = None,
        limit: int = 50,
    ) -> list[str]:
        filters = [
            "COALESCE(created_at_forum, first_seen_at) >= NOW() - (%s * INTERVAL '1 hour')",
        ]
        params: list[Any] = [hours]
        if forum_section_id is not None:
            filters.append("forum_section_id = %s")
            params.append(forum_section_id)
        params.append(limit)

        sql = f"""
        SELECT title
        FROM topics
        WHERE {' AND '.join(filters)}
        ORDER BY COALESCE(created_at_forum, first_seen_at) DESC
        LIMIT %s
        """
        rows = self._fetch_all(sql, tuple(params))
        return [row["title"] for row in rows if row.get("title")]

    def set_topic_reply_schedule(self, forum_topic_id: int, reply_not_before, reply_skip_reason: str | None = None) -> None:
        sql = """
        UPDATE topics
        SET reply_not_before = %s,
            reply_skip_reason = %s
        WHERE forum_topic_id = %s
        """
        self._execute(sql, (reply_not_before, reply_skip_reason, forum_topic_id))

    def get_topics_ready_to_publish(self, limit: int = 10) -> list[dict[str, Any]]:
        sql = """
        SELECT
            t.forum_topic_id,
            t.title,
            t.topic_url,
            br.reply_text
        FROM topics t
        JOIN LATERAL (
            SELECT reply_text
            FROM bot_replies
            WHERE forum_topic_id = t.forum_topic_id
              AND target_type = 'conversation'
              AND status = 'draft_sent'
            ORDER BY id DESC
            LIMIT 1
        ) br ON TRUE
        WHERE t.bot_replied_once = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM bot_replies b2
              WHERE b2.forum_topic_id = t.forum_topic_id
                AND b2.target_type = 'topic'
                AND b2.status = 'published'
          )
        ORDER BY t.first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_topics_ready_to_publish_by_status(
        self,
        draft_status: str,
        published_status: str,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        sql = """
        SELECT
            t.forum_topic_id,
            t.title,
            t.topic_url,
            br.reply_text
        FROM topics t
        JOIN LATERAL (
            SELECT reply_text
            FROM bot_replies
            WHERE forum_topic_id = t.forum_topic_id
              AND target_type = 'conversation'
              AND status = %s
            ORDER BY id DESC
            LIMIT 1
        ) br ON TRUE
        WHERE t.bot_replied_once = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM bot_replies b2
              WHERE b2.forum_topic_id = t.forum_topic_id
                AND b2.target_type = 'topic'
                AND b2.status = %s
          )
        ORDER BY t.first_seen_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (draft_status, published_status, limit))

    def topic_has_starter_post(self, forum_topic_id: int) -> bool:
        sql = """
        SELECT 1
        FROM posts
        WHERE forum_topic_id = %s
          AND is_topic_starter = TRUE
        LIMIT 1
        """
        rows = self._fetch_all(sql, (forum_topic_id,))
        return bool(rows)

    def get_topic_with_starter_post(self, forum_topic_id: int) -> dict[str, Any] | None:
        sql = """
        SELECT
            t.forum_topic_id,
            t.title,
            t.topic_url,
            p.forum_post_id,
            p.content_raw,
            p.content_text
        FROM topics t
        LEFT JOIN posts p
            ON p.forum_topic_id = t.forum_topic_id
           AND p.is_topic_starter = TRUE
        WHERE t.forum_topic_id = %s
        LIMIT 1
        """
        rows = self._fetch_all(sql, (forum_topic_id,))
        return rows[0] if rows else None

    def add_bot_reply(
        self,
        forum_topic_id: int,
        target_type: str,
        target_url: str,
        reply_text: str,
        status: str,
        error_message: str | None = None,
        forum_post_id: int | None = None,
    ) -> None:
        sql = """
        INSERT INTO bot_replies (
            forum_topic_id,
            forum_post_id,
            target_type,
            target_url,
            reply_text,
            status,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        self._execute(
            sql,
            (
                forum_topic_id,
                forum_post_id,
                target_type,
                target_url,
                reply_text,
                status,
                error_message,
            ),
        )

    def get_latest_draft_for_topic(self, forum_topic_id: int) -> dict[str, Any] | None:
        sql = """
        SELECT forum_topic_id, target_url, reply_text, status, created_at
        FROM bot_replies
        WHERE forum_topic_id = %s
          AND target_type = 'conversation'
          AND status = 'draft_sent'
        ORDER BY id DESC
        LIMIT 1
        """
        rows = self._fetch_all(sql, (forum_topic_id,))
        return rows[0] if rows else None

    def upsert_user_profile_post(self, post: UserProfilePostRecord) -> None:
        self.upsert_user(
            ForumUserRecord(
                forum_user_id=post.source_profile_user_id,
                username=post.source_profile_username,
                profile_url=f"https://dota2.ru/forum/members/{post.source_profile_username.lower()}.{post.source_profile_user_id}/",
            )
        )

        sql = """
        INSERT INTO user_profile_posts (
            source_profile_user_id,
            forum_post_id,
            post_url,
            topic_title,
            forum_section_name,
            forum_section_url,
            content_text,
            created_at_forum,
            activity_page_url
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (forum_post_id) DO UPDATE
        SET source_profile_user_id = EXCLUDED.source_profile_user_id,
            post_url = EXCLUDED.post_url,
            topic_title = EXCLUDED.topic_title,
            forum_section_name = EXCLUDED.forum_section_name,
            forum_section_url = EXCLUDED.forum_section_url,
            content_text = EXCLUDED.content_text,
            created_at_forum = COALESCE(EXCLUDED.created_at_forum, user_profile_posts.created_at_forum),
            activity_page_url = EXCLUDED.activity_page_url
        """
        self._execute(
            sql,
            (
                post.source_profile_user_id,
                post.forum_post_id,
                post.post_url,
                post.topic_title,
                post.forum_section_name,
                post.forum_section_url,
                post.content_text,
                post.created_at_forum,
                post.activity_page_url,
            ),
        )

    def get_user_profile_post_count(self, forum_user_id: int) -> int:
        sql = "SELECT count(*) AS count FROM user_profile_posts WHERE source_profile_user_id = %s"
        rows = self._fetch_all(sql, (forum_user_id,))
        return int(rows[0]["count"]) if rows else 0

    def get_user_profile_posts(self, forum_user_id: int, limit: int | None = None) -> list[dict[str, Any]]:
        sql = """
        SELECT forum_post_id, post_url, topic_title, forum_section_name, content_text, created_at_forum
        FROM user_profile_posts
        WHERE source_profile_user_id = %s
        ORDER BY created_at_forum DESC NULLS LAST, id DESC
        """
        params: tuple[Any, ...]
        if limit is not None:
            sql += " LIMIT %s"
            params = (forum_user_id, limit)
        else:
            params = (forum_user_id,)
        return self._fetch_all(sql, params)

    def upsert_user_style_profile(
        self,
        forum_user_id: int,
        source_profile_url: str,
        style_summary: str,
        lexicon: str,
        signature_phrases: str,
        preferred_topics: str,
        tone: str,
        message_length_stats: str,
        example_messages: str,
        confidence_score: float,
    ) -> None:
        sql = """
        INSERT INTO user_style_profiles (
            forum_user_id,
            source_profile_url,
            style_summary,
            lexicon,
            signature_phrases,
            preferred_topics,
            tone,
            message_length_stats,
            example_messages,
            confidence_score,
            updated_at
        )
        VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s::jsonb, %s::jsonb, %s, NOW())
        ON CONFLICT (forum_user_id) DO UPDATE
        SET source_profile_url = EXCLUDED.source_profile_url,
            style_summary = EXCLUDED.style_summary,
            lexicon = EXCLUDED.lexicon,
            signature_phrases = EXCLUDED.signature_phrases,
            preferred_topics = EXCLUDED.preferred_topics,
            tone = EXCLUDED.tone,
            message_length_stats = EXCLUDED.message_length_stats,
            example_messages = EXCLUDED.example_messages,
            confidence_score = EXCLUDED.confidence_score,
            updated_at = NOW()
        """
        self._execute(
            sql,
            (
                forum_user_id,
                source_profile_url,
                style_summary,
                lexicon,
                signature_phrases,
                preferred_topics,
                tone,
                message_length_stats,
                example_messages,
                confidence_score,
            ),
        )

    def get_user_style_profile(self, forum_user_id: int) -> dict[str, Any] | None:
        sql = """
        SELECT forum_user_id, source_profile_url, style_summary, lexicon, signature_phrases,
               preferred_topics, tone, message_length_stats, example_messages, confidence_score, updated_at
        FROM user_style_profiles
        WHERE forum_user_id = %s
        LIMIT 1
        """
        rows = self._fetch_all(sql, (forum_user_id,))
        return rows[0] if rows else None

    def get_daily_summary_schedule(self) -> dict[str, Any]:
        sql = """
        SELECT enabled, schedule_time, updated_at
        FROM scheduler_settings
        WHERE key = 'daily_summary'
        LIMIT 1
        """
        rows = self._fetch_all(sql, ())
        if rows:
            return rows[0]
        return {"enabled": False, "schedule_time": "12:00", "updated_at": None}

    def set_daily_summary_schedule(self, enabled: bool, schedule_time: str) -> None:
        sql = """
        INSERT INTO scheduler_settings (key, enabled, schedule_time, updated_at)
        VALUES ('daily_summary', %s, %s, NOW())
        ON CONFLICT (key) DO UPDATE
        SET enabled = EXCLUDED.enabled,
            schedule_time = EXCLUDED.schedule_time,
            updated_at = NOW()
        """
        self._execute(sql, (enabled, schedule_time))

    def get_daily_summary_run(self, summary_date) -> dict[str, Any] | None:
        sql = """
        SELECT
            summary_date,
            status,
            scheduled_time,
            topic_title,
            topic_url,
            source_topic_count,
            summary_text,
            error_message,
            created_at,
            updated_at
        FROM daily_summary_runs
        WHERE summary_date = %s
        LIMIT 1
        """
        rows = self._fetch_all(sql, (summary_date,))
        return rows[0] if rows else None

    def upsert_daily_summary_run(
        self,
        summary_date,
        status: str,
        scheduled_time: str | None = None,
        topic_title: str | None = None,
        topic_url: str | None = None,
        source_topic_count: int = 0,
        summary_text: str | None = None,
        error_message: str | None = None,
    ) -> None:
        sql = """
        INSERT INTO daily_summary_runs (
            summary_date,
            status,
            scheduled_time,
            topic_title,
            topic_url,
            source_topic_count,
            summary_text,
            error_message,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (summary_date) DO UPDATE
        SET status = EXCLUDED.status,
            scheduled_time = COALESCE(EXCLUDED.scheduled_time, daily_summary_runs.scheduled_time),
            topic_title = COALESCE(EXCLUDED.topic_title, daily_summary_runs.topic_title),
            topic_url = COALESCE(EXCLUDED.topic_url, daily_summary_runs.topic_url),
            source_topic_count = EXCLUDED.source_topic_count,
            summary_text = COALESCE(EXCLUDED.summary_text, daily_summary_runs.summary_text),
            error_message = EXCLUDED.error_message,
            updated_at = NOW()
        """
        self._execute(
            sql,
            (
                summary_date,
                status,
                scheduled_time,
                topic_title,
                topic_url,
                source_topic_count,
                summary_text,
                error_message,
            ),
        )

    def get_recent_daily_summary_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        sql = """
        SELECT
            summary_date,
            status,
            scheduled_time,
            topic_title,
            topic_url,
            source_topic_count,
            error_message,
            created_at,
            updated_at
        FROM daily_summary_runs
        ORDER BY summary_date DESC, updated_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_daily_topic_schedule(self) -> dict[str, Any]:
        sql = """
        SELECT enabled, schedule_time, updated_at
        FROM scheduler_settings
        WHERE key = 'daily_topic'
        LIMIT 1
        """
        rows = self._fetch_all(sql, ())
        if rows:
            return rows[0]
        return {"enabled": False, "schedule_time": "18:00", "updated_at": None}

    def set_daily_topic_schedule(self, enabled: bool, schedule_time: str) -> None:
        sql = """
        INSERT INTO scheduler_settings (key, enabled, schedule_time, updated_at)
        VALUES ('daily_topic', %s, %s, NOW())
        ON CONFLICT (key) DO UPDATE
        SET enabled = EXCLUDED.enabled,
            schedule_time = EXCLUDED.schedule_time,
            updated_at = NOW()
        """
        self._execute(sql, (enabled, schedule_time))

    def get_daily_topic_run(self, topic_date) -> dict[str, Any] | None:
        sql = """
        SELECT
            topic_date,
            status,
            scheduled_time,
            prompt_code,
            topic_title,
            topic_body,
            topic_url,
            error_message,
            created_at,
            updated_at
        FROM daily_topic_runs
        WHERE topic_date = %s
        LIMIT 1
        """
        rows = self._fetch_all(sql, (topic_date,))
        return rows[0] if rows else None

    def upsert_daily_topic_run(
        self,
        topic_date,
        status: str,
        scheduled_time: str | None = None,
        prompt_code: str | None = None,
        topic_title: str | None = None,
        topic_body: str | None = None,
        topic_url: str | None = None,
        error_message: str | None = None,
    ) -> None:
        sql = """
        INSERT INTO daily_topic_runs (
            topic_date,
            status,
            scheduled_time,
            prompt_code,
            topic_title,
            topic_body,
            topic_url,
            error_message,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT (topic_date) DO UPDATE
        SET status = EXCLUDED.status,
            scheduled_time = COALESCE(EXCLUDED.scheduled_time, daily_topic_runs.scheduled_time),
            prompt_code = COALESCE(EXCLUDED.prompt_code, daily_topic_runs.prompt_code),
            topic_title = COALESCE(EXCLUDED.topic_title, daily_topic_runs.topic_title),
            topic_body = COALESCE(EXCLUDED.topic_body, daily_topic_runs.topic_body),
            topic_url = COALESCE(EXCLUDED.topic_url, daily_topic_runs.topic_url),
            error_message = EXCLUDED.error_message,
            updated_at = NOW()
        """
        self._execute(
            sql,
            (
                topic_date,
                status,
                scheduled_time,
                prompt_code,
                topic_title,
                topic_body,
                topic_url,
                error_message,
            ),
        )

    def get_recent_daily_topic_runs(self, limit: int = 10) -> list[dict[str, Any]]:
        sql = """
        SELECT
            topic_date,
            status,
            scheduled_time,
            prompt_code,
            topic_title,
            topic_url,
            error_message,
            created_at,
            updated_at
        FROM daily_topic_runs
        ORDER BY topic_date DESC, updated_at DESC
        LIMIT %s
        """
        return self._fetch_all(sql, (limit,))

    def get_active_topic_prompt(self, prompt_code: str) -> dict[str, Any] | None:
        sql = """
        SELECT prompt_code, prompt_name, prompt_text, is_active, created_at, updated_at
        FROM topic_generation_prompts
        WHERE prompt_code = %s
          AND is_active = TRUE
        LIMIT 1
        """
        rows = self._fetch_all(sql, (prompt_code,))
        return rows[0] if rows else None

    def ensure_runtime_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS user_profile_posts (
                id BIGSERIAL PRIMARY KEY,
                source_profile_user_id BIGINT NOT NULL REFERENCES users(forum_user_id) ON DELETE CASCADE,
                forum_post_id BIGINT NOT NULL UNIQUE,
                post_url TEXT NOT NULL,
                topic_title TEXT NOT NULL,
                forum_section_name TEXT,
                forum_section_url TEXT,
                content_text TEXT NOT NULL,
                created_at_forum TIMESTAMPTZ,
                activity_page_url TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_user_profile_posts_user_time
            ON user_profile_posts (source_profile_user_id, created_at_forum DESC)
            """,
            """
            ALTER TABLE topics
            ADD COLUMN IF NOT EXISTS forum_reply_count INTEGER
            """,
            """
            ALTER TABLE topics
            ADD COLUMN IF NOT EXISTS reply_not_before TIMESTAMPTZ
            """,
            """
            ALTER TABLE topics
            ADD COLUMN IF NOT EXISTS reply_skip_reason TEXT
            """,
            """
            CREATE TABLE IF NOT EXISTS scheduler_settings (
                key TEXT PRIMARY KEY,
                enabled BOOLEAN NOT NULL DEFAULT FALSE,
                schedule_time TEXT NOT NULL DEFAULT '12:00',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
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
            )
            """,
            """
            INSERT INTO scheduler_settings (key, enabled, schedule_time, updated_at)
            VALUES ('daily_summary', FALSE, '12:00', NOW())
            ON CONFLICT (key) DO NOTHING
            """,
            """
            CREATE TABLE IF NOT EXISTS topic_generation_prompts (
                id BIGSERIAL PRIMARY KEY,
                prompt_code TEXT NOT NULL UNIQUE,
                prompt_name TEXT NOT NULL,
                prompt_text TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """,
            """
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
            )
            """,
            """
            INSERT INTO scheduler_settings (key, enabled, schedule_time, updated_at)
            VALUES ('daily_topic', FALSE, '18:00', NOW())
            ON CONFLICT (key) DO NOTHING
            """,
            """
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
                'тема для форума (в стиле dota2.ru), связанную с девушками / отношениями / одиночеством.

Требования:

Заголовок:
максимально короткий (2–6 слов)
простой, разговорный, без сложных формулировок
можно использовать слова: «девушка», «тян», «тянка»
допускается грубость или провокация
стиль должен выглядеть как реальный пользовательский заголовок

Форматы заголовка (выбери один):

вопрос (например: «почему тян игнорят»)
короткая проблема («девушка не отвечает»)
эмоциональный заголовок («тян сломала жизнь»)
обсуждение («идеальная девушка»)
Первый пост:
3–8 предложений
пишется от первого лица
стиль: разговорный, немного небрежный
допускаются ошибки или упрощённая речь
содержание:
либо личная ситуация
либо вопрос к форуму
либо жалоба / рассуждение
Добавь элементы:
неуверенность / обида / злость / растерянность
обращение к аудитории («пацаны», «ребят»)
открытый вопрос в конце
Не делай текст слишком умным или литературным — он должен выглядеть как пост обычного юзера форума.',
                TRUE,
                NOW(),
                NOW()
            )
            ON CONFLICT (prompt_code) DO UPDATE
            SET prompt_name = EXCLUDED.prompt_name,
                prompt_text = EXCLUDED.prompt_text,
                is_active = EXCLUDED.is_active,
                updated_at = NOW()
            """,
        ]
        with self._connect() as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
            conn.commit()

    def mark_topic_replied(self, forum_topic_id: int) -> None:
        sql = """
        UPDATE topics
        SET bot_replied_once = TRUE,
            bot_replied_at = NOW()
        WHERE forum_topic_id = %s
        """
        self._execute(sql, (forum_topic_id,))

    def topic_exists(self, forum_topic_id: int) -> bool:
        sql = "SELECT 1 FROM topics WHERE forum_topic_id = %s LIMIT 1"
        rows = self._fetch_all(sql, (forum_topic_id,))
        return bool(rows)

    def _execute(self, sql: str, params: tuple[Any, ...]) -> None:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()

    def _fetch_all(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                columns = [col.name for col in cur.description]
                rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
