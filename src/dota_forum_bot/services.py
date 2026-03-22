from __future__ import annotations

import time
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time as dt_time, timedelta, timezone
import random
from zoneinfo import ZoneInfo

from .client import Dota2ForumClient
from .db import Database
from .drafts import build_topic_draft
from .exceptions import ForumBotError, MessageSendError
from .llm_client import LLMClient
from .parsers import (
    PostRecord,
    TopicPageRecord,
    TopicRecord,
    extract_post_message_text,
    extract_quoted_text,
    parse_quote_notifications_api,
    parse_quote_notifications,
    parse_profile_posts_page,
    parse_profile_posts_total_pages,
    parse_taverna_topics,
    parse_topic_page,
    parse_topic_thread_page,
)
from .style_profile import build_style_profile, profile_to_db_payload


TAVERNA_URL = "https://dota2.ru/forum/forums/taverna.6/"
TAVERNA_SCOPE = "forum_section:taverna"
NOTIFICATIONS_URL = "https://dota2.ru/forum/notifications/"
BOT_PROFILE_POSTS_URL = "https://dota2.ru/forum/members/opera-mobile.847606/activity/posts/"
BOT_USER_ID = 847606
BOT_USERNAME = "Opera Mobile"
DISPLAY_TIMEZONE = ZoneInfo("Europe/Moscow")
BOT_AUTHORED_SKIP_REASON = "bot_authored_topic"


@dataclass
class ScanResult:
    found: int
    inserted_or_updated: int
    new_topics: int


@dataclass
class DraftResult:
    processed: int
    sent: int
    failed: int


@dataclass
class PublishResult:
    processed: int
    published: int
    failed: int


@dataclass
class ProfileSyncResult:
    pages_scanned: int
    posts_saved: int
    total_pages: int


@dataclass
class StyleProfileResult:
    forum_user_id: int
    posts_used: int
    confidence_score: float


@dataclass
class LLMDraftResult:
    processed: int
    sent: int
    failed: int


@dataclass
class LLMPublishResult:
    processed: int
    published: int
    failed: int


@dataclass
class AutoReplyResult:
    scanned: int
    processed: int
    published: int
    failed: int
    details: list[str] = field(default_factory=list)


@dataclass
class QuoteReplyResult:
    scanned: int
    new_notifications: int
    processed: int
    replied: int
    ignored: int
    failed: int
    details: list[str] = field(default_factory=list)


@dataclass
class DailySummaryResult:
    summary_date: date
    scanned: int
    topics_selected: int
    status: str
    topic_title: str | None = None
    topic_url: str | None = None
    details: list[str] = field(default_factory=list)


@dataclass
class DailyTopicResult:
    topic_date: date
    status: str
    topic_title: str | None = None
    topic_url: str | None = None
    details: list[str] = field(default_factory=list)


class ForumSyncService:
    DAILY_SUMMARY_RETRY_DELAY_SECONDS = 600
    DAILY_SUMMARY_IN_PROGRESS_TIMEOUT_SECONDS = 300
    DAILY_TOPIC_PROMPT_CODE = "daily_relationship_topic"
    DAILY_TOPIC_RETRY_DELAY_SECONDS = 600
    DAILY_TOPIC_IN_PROGRESS_TIMEOUT_SECONDS = 300

    def __init__(self, client: Dota2ForumClient, db: Database) -> None:
        self.client = client
        self.db = db

    @staticmethod
    def _emit(log, message: str) -> None:
        if log is not None:
            log(message)

    @staticmethod
    def _human_reply_delay_minutes(reply_count: int | None) -> int:
        if reply_count is None:
            return random.randint(20, 60)
        if reply_count <= 2:
            return random.randint(30, 90)
        if reply_count <= 5:
            return random.randint(10, 30)
        return random.randint(3, 10)

    @staticmethod
    def _format_dt(value) -> str:
        if value is None:
            return "None"
        if hasattr(value, "strftime"):
            return value.astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
        return str(value)

    @staticmethod
    def _parse_schedule_time(value: str | None) -> dt_time:
        raw = (value or "12:00").strip()
        try:
            hours, minutes = raw.split(":", 1)
            return dt_time(hour=int(hours), minute=int(minutes))
        except Exception:
            return dt_time(hour=12, minute=0)

    @staticmethod
    def _build_topic_page_url(topic_url: str, page_number: int) -> str:
        normalized = topic_url.rstrip("/")
        if page_number <= 1:
            return f"{normalized}/"
        return f"{normalized}/page-{page_number}"

    @staticmethod
    def _trim_text(value: str, limit: int) -> str:
        text = " ".join((value or "").split())
        if len(text) <= limit:
            return text
        return text[: limit - 1].rstrip() + "…"

    @staticmethod
    def _looks_like_bot_accusation(user_message_text: str) -> bool:
        normalized = re.sub(r"\s+", " ", (user_message_text or "").lower().replace("ё", "е")).strip()
        if not normalized or "бот" not in normalized:
            return False

        patterns = [
            r"\b(?:ты|тебя|тебе|тобой|твой|твоя|твое|твои)\b.{0,20}\bбот\w*",
            r"\bбот\w*\b.{0,20}\b(?:ты|тебя|тебе|тобой|твой|твоя|твое|твои)\b",
            r"\bopera(?:\s|-)?mobile\b.{0,20}\bбот\w*",
            r"\bбот\w*\b.{0,20}\bopera(?:\s|-)?mobile\b",
            r"\bopera\b.{0,20}\bбот\w*",
            r"\bбот\w*\b.{0,20}\bopera\b",
        ]
        return any(re.search(pattern, normalized, flags=re.IGNORECASE) for pattern in patterns)

    @staticmethod
    def _build_quote_reply_message(
        source_username: str | None,
        source_user_id: int | None,
        forum_post_id: int,
        quoted_message_text: str,
        reply_text: str,
    ) -> str:
        username = (source_username or "").strip() or "user"
        body = (quoted_message_text or "").strip() or "..."
        header = f'[QUOTE="{username}, post: {forum_post_id}'
        if source_user_id is not None:
            header += f", member: {source_user_id}"
        header += '"]'
        return f"{header}\n{body}\n[/QUOTE]\n\n{reply_text.strip()}"

    @staticmethod
    def _normalize_generated_summary(body: str) -> str:
        text = body.replace("\r\n", "\n")
        text = text.replace("Краткое содержание темы", "")
        text = text.replace("Краткое содержание о чем писали юзеры", "")
        text = text.replace("Интересные моменты", "")
        text = text.replace("Самые популярные комментарии\n", "Самые популярные комментарии:\n")

        text = re.sub(r"\n{3,}", "\n\n", text)

        def spoiler_cleanup(match):
            title = match.group(1)
            content = match.group(2)
            content = re.sub(r"[ \t]+\n", "\n", content)
            content = re.sub(r"\n{3,}", "\n\n", content)
            content = re.sub(
                r"([^.:\n])\s+(Самые популярные комментарии:)",
                r"\1\n\n\2",
                content,
            )
            lines = [line.strip() for line in content.split("\n")]
            cleaned_lines: list[str] = []
            for line in lines:
                if not line:
                    if cleaned_lines and cleaned_lines[-1] != "":
                        cleaned_lines.append("")
                    continue
                cleaned_lines.append(line)
            cleaned = "\n".join(cleaned_lines).strip()
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
            return f'[SPOILER="{title}"]\n{cleaned}\n[/SPOILER]'

        text = re.sub(
            r'\[SPOILER="([^"]+)"\]\s*(.*?)\s*\[/SPOILER\]',
            spoiler_cleanup,
            text,
            flags=re.DOTALL,
        )
        return text.strip()

    @staticmethod
    def _normalize_generated_topic_title(title: str) -> str:
        text = " ".join((title or "").replace("\n", " ").split()).strip(" -:.")
        if not text:
            raise ValueError("Generated topic title is empty.")
        return text[:120].strip()

    @staticmethod
    def _normalize_generated_topic_body(body: str) -> str:
        text = (body or "").replace("\r\n", "\n").strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        if not text:
            raise ValueError("Generated topic body is empty.")
        return text

    @staticmethod
    def _summarize_reactions(post) -> list[str]:
        items = []
        for reaction in post.reactions[:4]:
            items.append(f"{reaction.title}: {reaction.count}")
        return items

    def _build_summary_topic_payload(self, topic: dict, posts: list) -> dict[str, object]:
        starter = posts[0] if posts else None
        replies = posts[1:] if len(posts) > 1 else []

        participants = sorted(
            {
                post.author.username
                for post in posts
                if post.author is not None and post.author.username.strip()
            }
        )

        popular_posts = sorted(
            replies,
            key=lambda item: (
                item.positive_reaction_count,
                item.total_reaction_count,
                item.post_number or 0,
            ),
            reverse=True,
        )

        highlight_posts = []
        seen_post_ids: set[int] = set()
        for candidate in [*replies[:5], *popular_posts[:3], *replies[-2:]]:
            if candidate is None or candidate.forum_post_id in seen_post_ids:
                continue
            seen_post_ids.add(candidate.forum_post_id)
            highlight_posts.append(
                {
                    "post_number": candidate.post_number,
                    "author": candidate.author.username if candidate.author else None,
                    "text": self._trim_text(candidate.content_text, 420),
                    "positive_reactions": candidate.positive_reaction_count,
                    "total_reactions": candidate.total_reaction_count,
                    "reactions": self._summarize_reactions(candidate),
                }
            )

        popular_comments = [
            {
                "post_number": candidate.post_number,
                "author": candidate.author.username if candidate.author else None,
                "text": self._trim_text(candidate.content_text, 280),
                "positive_reactions": candidate.positive_reaction_count,
                "total_reactions": candidate.total_reaction_count,
                "reactions": self._summarize_reactions(candidate),
            }
            for candidate in popular_posts[:3]
        ]

        return {
            "topic_id": topic["forum_topic_id"],
            "title": topic["title"],
            "url": topic["topic_url"],
            "created_at": self._format_dt(topic.get("created_at_forum") or topic.get("first_seen_at")),
            "reply_count": max(0, len(replies)),
            "participant_count": len(participants),
            "participants_sample": participants[:12],
            "starter_post": self._trim_text(starter.content_text if starter else "", 1600),
            "highlights": highlight_posts,
            "popular_comments": popular_comments,
        }

    def _fetch_topic_thread_posts(self, topic_url: str) -> tuple[TopicRecord, list]:
        first_page = self._fetch_page_or_raise(topic_url, context="topic thread page")
        first_record = parse_topic_thread_page(first_page.url, first_page.text)
        posts = list(first_record.posts)
        seen_post_ids = {post.forum_post_id for post in posts}

        for page_number in range(2, first_record.total_pages + 1):
            response = self._fetch_page_or_raise(
                self._build_topic_page_url(first_record.topic.topic_url, page_number),
                context=f"topic thread page {page_number}",
            )
            page_record = parse_topic_thread_page(response.url, response.text)
            for post in page_record.posts:
                if post.forum_post_id in seen_post_ids:
                    continue
                seen_post_ids.add(post.forum_post_id)
                posts.append(post)

        posts.sort(key=lambda item: ((item.post_number or 0), item.forum_post_id))
        return first_record.topic, posts

    def _fetch_page_or_raise(self, url: str, context: str) -> "HttpResponse":
        response = self.client.fetch_page(url)
        if response.status >= 400:
            preview = re.sub(r"\s+", " ", response.text[:160]).strip()
            raise ForumBotError(
                f"Failed to load {context}. HTTP {response.status} for {url}. "
                f"Response preview: {preview}"
            )
        return response

    @staticmethod
    def _thread_post_to_post_record(post, is_topic_starter: bool) -> PostRecord:
        return PostRecord(
            forum_post_id=post.forum_post_id,
            forum_topic_id=post.forum_topic_id,
            author=post.author,
            post_url=post.post_url,
            content_raw=post.content_raw,
            content_text=post.content_text,
            created_at_forum=post.created_at_forum,
            is_topic_starter=is_topic_starter,
            reply_to_post_id=None,
        )

    def reply_to_quote_notifications_with_llm(
        self,
        llm: LLMClient,
        limit: int = 20,
        log=None,
    ) -> QuoteReplyResult:
        load_payload = self.client.load_notifications()
        categories = load_payload.get("categories") or {}
        quote_category_name = next(
            (
                name
                for name in categories.keys()
                if isinstance(name, str) and name.lower().replace("ё", "е") == "цитаты"
            ),
            "Цитаты",
        )
        preload_payload = self.client.preload_notifications(name=quote_category_name, page=1)
        notifications = parse_quote_notifications_api(preload_payload.get("notices") or [])
        style_profile: dict | None = None

        scanned = len(notifications)
        new_notifications = 0
        processed = 0
        replied = 0
        ignored = 0
        failed = 0
        details: list[str] = []

        summary = f"Quote notifications fetched: total={scanned}, category={quote_category_name}"
        details.append(summary)
        self._emit(log, summary)

        for notification in notifications:
            if processed >= limit:
                break

            inserted = self.db.create_quote_reply_notification(
                forum_post_id=notification.forum_post_id,
                post_url=notification.post_url,
                source_username=notification.source_username,
                source_user_id=notification.source_user_id,
                topic_title=notification.topic_title,
                notification_text=notification.notification_text,
            )
            if not inserted:
                continue

            new_notifications += 1
            processed += 1
            self.db.update_quote_reply_notification(notification.forum_post_id, status="in_progress")
            start_message = (
                f"Quote post {notification.forum_post_id}: "
                f"user={notification.source_username or '-'} topic={notification.topic_title or '-'}"
            )
            details.append(start_message)
            self._emit(log, start_message)

            topic_record: TopicRecord | None = None
            quote_text = ""
            user_message_text = ""
            try:
                topic_record, posts = self._fetch_topic_thread_posts(notification.post_url)
                self.db.upsert_topic(topic_record)
                for index, post in enumerate(posts):
                    self.db.upsert_post(self._thread_post_to_post_record(post, is_topic_starter=index == 0))

                target_post = next((post for post in posts if post.forum_post_id == notification.forum_post_id), None)
                if target_post is None:
                    raise ValueError(f"Quoted post {notification.forum_post_id} was not found in thread.")

                quote_text = extract_quoted_text(target_post.content_raw)
                user_message_text = extract_post_message_text(target_post.content_raw) or target_post.content_text
                starter_post_text = posts[0].content_text if posts else ""

                if style_profile is None and not self._looks_like_bot_accusation(user_message_text):
                    style_profile = self.db.get_user_style_profile(BOT_USER_ID)
                    if style_profile is None:
                        raise ValueError("Bot style profile is not built yet. Run build-yakim-profile first.")

                if self._looks_like_bot_accusation(user_message_text):
                    status = "ignored_bot_accusation"
                    ignored += 1
                    self.db.update_quote_reply_notification(
                        forum_post_id=notification.forum_post_id,
                        status=status,
                        forum_topic_id=topic_record.forum_topic_id,
                        topic_url=topic_record.topic_url,
                        quote_text=quote_text,
                        user_message_text=user_message_text,
                        reply_text="",
                        error_message=None,
                    )
                    skip_message = "  Ignored because the user called the bot a bot."
                    details.append(skip_message)
                    self._emit(log, skip_message)
                    continue
                else:
                    reply_text = llm.generate_quote_reply(
                        topic_title=topic_record.title,
                        starter_post_text=starter_post_text,
                        quoted_text=quote_text or "(quoted fragment was not extracted)",
                        user_message_text=user_message_text,
                        style_profile=style_profile or {},
                    )
                    reply_text = self._build_quote_reply_message(
                        source_username=notification.source_username,
                        source_user_id=notification.source_user_id,
                        forum_post_id=notification.forum_post_id,
                        quoted_message_text=user_message_text,
                        reply_text=reply_text,
                    )
                    status = "llm_replied"
                    replied += 1

                self.client.send_message_to_thread(topic_record.topic_url, reply_text)
                self.db.add_bot_reply(
                    forum_topic_id=topic_record.forum_topic_id,
                    target_type="topic",
                    target_url=topic_record.topic_url,
                    reply_text=reply_text,
                    status=f"quote_{status}",
                    forum_post_id=notification.forum_post_id,
                )
                self.db.update_quote_reply_notification(
                    forum_post_id=notification.forum_post_id,
                    status=status,
                    forum_topic_id=topic_record.forum_topic_id,
                    topic_url=topic_record.topic_url,
                    quote_text=quote_text,
                    user_message_text=user_message_text,
                    reply_text=reply_text,
                    error_message=None,
                )
                success_message = f"  Replied successfully with status={status}."
                details.append(success_message)
                self._emit(log, success_message)
                time.sleep(10)
            except MessageSendError as exc:
                failed += 1
                if topic_record is not None:
                    self.db.add_bot_reply(
                        forum_topic_id=topic_record.forum_topic_id,
                        target_type="topic",
                        target_url=topic_record.topic_url,
                        reply_text="",
                        status="quote_reply_failed",
                        error_message=str(exc),
                        forum_post_id=notification.forum_post_id,
                    )
                self.db.update_quote_reply_notification(
                    forum_post_id=notification.forum_post_id,
                    status="reply_failed",
                    forum_topic_id=topic_record.forum_topic_id if topic_record else None,
                    topic_url=topic_record.topic_url if topic_record else None,
                    quote_text=quote_text or None,
                    user_message_text=user_message_text or None,
                    error_message=str(exc),
                )
                error_message = f"  Reply failed: {exc}"
                details.append(error_message)
                self._emit(log, error_message)
                time.sleep(10)
            except Exception as exc:
                failed += 1
                self.db.update_quote_reply_notification(
                    forum_post_id=notification.forum_post_id,
                    status="processing_failed",
                    forum_topic_id=topic_record.forum_topic_id if topic_record else None,
                    topic_url=topic_record.topic_url if topic_record else None,
                    quote_text=quote_text or None,
                    user_message_text=user_message_text or None,
                    error_message=str(exc),
                )
                error_message = f"  Quote processing failed: {exc}"
                details.append(error_message)
                self._emit(log, error_message)

        return QuoteReplyResult(
            scanned=scanned,
            new_notifications=new_notifications,
            processed=processed,
            replied=replied,
            ignored=ignored,
            failed=failed,
            details=details,
        )

    def _explain_auto_reply_eligibility(self, topic: dict, max_age_days: int) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if topic.get("bot_replied_once"):
            reasons.append("already_replied")
        if topic.get("is_closed"):
            reasons.append("closed")
        if topic.get("is_pinned"):
            reasons.append("pinned")
        if topic.get("author_user_id") == BOT_USER_ID:
            reasons.append(BOT_AUTHORED_SKIP_REASON)
        elif topic.get("reply_skip_reason") == BOT_AUTHORED_SKIP_REASON:
            reasons.append(BOT_AUTHORED_SKIP_REASON)

        created_at = topic.get("created_at_forum") or topic.get("first_seen_at")
        if created_at is None:
            reasons.append("missing_created_at")
        else:
            now = datetime.now(timezone.utc)
            cutoff = now - timedelta(days=max_age_days)
            if created_at < cutoff:
                reasons.append("older_than_max_age")

        reply_not_before = topic.get("reply_not_before")
        if reply_not_before is None:
            reasons.append("reply_not_scheduled")
        else:
            now = datetime.now(timezone.utc)
            if reply_not_before > now:
                reasons.append("not_due_yet")

        return (len(reasons) == 0, reasons)

    def scan_taverna(self) -> ScanResult:
        response = self._fetch_page_or_raise(TAVERNA_URL, context="taverna section page")
        topics = parse_taverna_topics(response.text)

        changed = 0
        new_topics = 0
        last_topic_id = None

        for topic in topics:
            exists = self.db.topic_exists(topic.forum_topic_id)
            self.db.upsert_topic(topic)
            if topic.author is not None and topic.author.forum_user_id == BOT_USER_ID:
                self.db.set_topic_reply_schedule(
                    topic.forum_topic_id,
                    reply_not_before=None,
                    reply_skip_reason=BOT_AUTHORED_SKIP_REASON,
                )
                self._emit(
                    log if 'log' in locals() else None,
                    f"Skipped auto-reply for bot-authored topic {topic.forum_topic_id}.",
                )
            elif not exists or self.db.topic_needs_reply_schedule(topic.forum_topic_id):
                delay_minutes = self._human_reply_delay_minutes(topic.forum_reply_count)
                reply_not_before = datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)
                self.db.set_topic_reply_schedule(topic.forum_topic_id, reply_not_before, reply_skip_reason=None)
                self._emit(
                    log if 'log' in locals() else None,
                    f"Scheduled topic {topic.forum_topic_id} reply after {delay_minutes} minutes "
                    f"(reply_count={topic.forum_reply_count}, not_before={self._format_dt(reply_not_before)})",
                )
            changed += 1
            if not exists:
                new_topics += 1
            if last_topic_id is None or topic.forum_topic_id > last_topic_id:
                last_topic_id = topic.forum_topic_id

        self.db.update_scan_state(TAVERNA_SCOPE, topic_id=last_topic_id)
        return ScanResult(found=len(topics), inserted_or_updated=changed, new_topics=new_topics)

    def sync_topic(self, topic_url: str) -> TopicPageRecord:
        response = self._fetch_page_or_raise(topic_url, context="topic page")
        topic_page = parse_topic_page(response.url, response.text)
        self.db.upsert_topic(topic_page.topic)
        self.db.upsert_post(topic_page.first_post)
        self.db.update_scan_state(TAVERNA_SCOPE, topic_id=topic_page.topic.forum_topic_id, post_id=topic_page.first_post.forum_post_id)
        return topic_page

    def list_new_topics(self, limit: int = 50) -> list[dict]:
        return self.db.get_new_topics(limit=limit)

    def draft_new_topics_to_conversation(self, conversation_url: str, limit: int = 10) -> DraftResult:
        topics = self.db.get_topics_pending_draft(limit=limit)
        processed = 0
        sent = 0
        failed = 0

        for topic in topics:
            processed += 1
            forum_topic_id = topic["forum_topic_id"]
            try:
                if not self.db.topic_has_starter_post(forum_topic_id):
                    self.sync_topic(topic["topic_url"])

                topic_data = self.db.get_topic_with_starter_post(forum_topic_id)
                if topic_data is None:
                    raise ValueError(f"Topic {forum_topic_id} was not found after sync.")

                draft_text = build_topic_draft(
                    title=topic_data["title"],
                    content_text=topic_data.get("content_text") or "",
                )
                self.client.send_message_to_thread(conversation_url, draft_text)
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text=draft_text,
                    status="draft_sent",
                    forum_post_id=topic_data.get("forum_post_id"),
                )
                sent += 1
                time.sleep(10)
            except MessageSendError as exc:
                if "throttle" in str(exc).lower():
                    self.db.add_bot_reply(
                        forum_topic_id=forum_topic_id,
                        target_type="conversation",
                        target_url=conversation_url,
                        reply_text=draft_text,
                        status="draft_throttled",
                        error_message=str(exc),
                        forum_post_id=topic_data.get("forum_post_id") if "topic_data" in locals() and topic_data else None,
                    )
                    failed += 1
                    time.sleep(10)
                    continue
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text=draft_text if "draft_text" in locals() else "",
                    status="draft_failed",
                    error_message=str(exc),
                    forum_post_id=topic_data.get("forum_post_id") if "topic_data" in locals() and topic_data else None,
                )
                failed += 1
            except Exception as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text="",
                    status="draft_failed",
                    error_message=str(exc),
                )
                failed += 1

        return DraftResult(processed=processed, sent=sent, failed=failed)

    def publish_drafted_topics(self, limit: int = 5) -> PublishResult:
        topics = self.db.get_topics_ready_to_publish(limit=limit)
        processed = 0
        published = 0
        failed = 0

        for topic in topics:
            processed += 1
            forum_topic_id = topic["forum_topic_id"]
            topic_url = topic["topic_url"]
            reply_text = topic["reply_text"]

            try:
                self.client.send_message_to_thread(topic_url, reply_text)
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="published",
                )
                self.db.mark_topic_replied(forum_topic_id)
                published += 1
                time.sleep(10)
            except MessageSendError as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="publish_failed",
                    error_message=str(exc),
                )
                failed += 1
                time.sleep(10)
            except Exception as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="publish_failed",
                    error_message=str(exc),
                )
                failed += 1

        return PublishResult(processed=processed, published=published, failed=failed)

    def sync_user_profile_posts(self, max_pages: int = 3) -> ProfileSyncResult:
        first_page = self.client.fetch_page(BOT_PROFILE_POSTS_URL)
        total_pages = parse_profile_posts_total_pages(first_page.text)
        pages_to_scan = min(max_pages, total_pages)
        saved = 0

        for page_num in range(1, pages_to_scan + 1):
            if page_num == 1:
                response = first_page
                page_url = BOT_PROFILE_POSTS_URL
            else:
                page_url = f"{BOT_PROFILE_POSTS_URL}page-{page_num}"
                response = self.client.fetch_page(page_url)

            posts = parse_profile_posts_page(
                profile_user_id=BOT_USER_ID,
                profile_username=BOT_USERNAME,
                page_url=page_url,
                html_text=response.text,
            )
            for post in posts:
                self.db.upsert_user_profile_post(post)
                saved += 1

        return ProfileSyncResult(pages_scanned=pages_to_scan, posts_saved=saved, total_pages=total_pages)

    def build_yakim_style_profile(self, limit: int | None = None) -> StyleProfileResult:
        posts = self.db.get_user_profile_posts(BOT_USER_ID, limit=limit)
        messages = [post["content_text"] for post in posts if post.get("content_text")]
        topics = [post["topic_title"] for post in posts if post.get("topic_title")]

        profile = build_style_profile(messages=messages, topic_titles=topics)
        payload = profile_to_db_payload(profile)
        self.db.upsert_user_style_profile(
            forum_user_id=BOT_USER_ID,
            source_profile_url=BOT_PROFILE_POSTS_URL,
            style_summary=payload["style_summary"],
            lexicon=payload["lexicon"],
            signature_phrases=payload["signature_phrases"],
            preferred_topics=payload["preferred_topics"],
            tone=payload["tone"],
            message_length_stats=payload["message_length_stats"],
            example_messages=payload["example_messages"],
            confidence_score=payload["confidence_score"],
        )
        return StyleProfileResult(
            forum_user_id=BOT_USER_ID,
            posts_used=len(messages),
            confidence_score=profile.confidence_score,
        )

    def draft_new_topics_with_llm(
        self,
        llm: LLMClient,
        conversation_url: str,
        limit: int = 5,
    ) -> LLMDraftResult:
        topics = self.db.get_topics_pending_llm_draft(limit=limit)
        style_profile = self.db.get_user_style_profile(BOT_USER_ID)
        if style_profile is None:
            raise ValueError("Bot style profile is not built yet. Run build-yakim-profile first.")

        processed = 0
        sent = 0
        failed = 0

        for topic in topics:
            processed += 1
            forum_topic_id = topic["forum_topic_id"]
            try:
                if not self.db.topic_has_starter_post(forum_topic_id):
                    self.sync_topic(topic["topic_url"])

                topic_data = self.db.get_topic_with_starter_post(forum_topic_id)
                if topic_data is None:
                    raise ValueError(f"Topic {forum_topic_id} was not found after sync.")

                reply_text = llm.generate_forum_reply(
                    topic_title=topic_data["title"],
                    topic_text=topic_data.get("content_text") or "",
                    style_profile=style_profile,
                )
                self.client.send_message_to_thread(conversation_url, reply_text)
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text=reply_text,
                    status="llm_draft_sent",
                    forum_post_id=topic_data.get("forum_post_id"),
                )
                sent += 1
                time.sleep(10)
            except MessageSendError as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text="",
                    status="llm_draft_failed",
                    error_message=str(exc),
                )
                failed += 1
                time.sleep(10)
            except Exception as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="conversation",
                    target_url=conversation_url,
                    reply_text="",
                    status="llm_draft_failed",
                    error_message=str(exc),
                )
                failed += 1

        return LLMDraftResult(processed=processed, sent=sent, failed=failed)

    def publish_llm_drafted_topics(self, limit: int = 5) -> LLMPublishResult:
        topics = self.db.get_topics_ready_to_publish_by_status(
            draft_status="llm_draft_sent",
            published_status="llm_published",
            limit=limit,
        )
        processed = 0
        published = 0
        failed = 0

        for topic in topics:
            processed += 1
            forum_topic_id = topic["forum_topic_id"]
            topic_url = topic["topic_url"]
            reply_text = topic["reply_text"]

            try:
                self.client.send_message_to_thread(topic_url, reply_text)
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="llm_published",
                )
                self.db.mark_topic_replied(forum_topic_id)
                published += 1
                time.sleep(10)
            except MessageSendError as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="llm_publish_failed",
                    error_message=str(exc),
                )
                failed += 1
                time.sleep(10)
            except Exception as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="llm_publish_failed",
                    error_message=str(exc),
                )
                failed += 1

        return LLMPublishResult(processed=processed, published=published, failed=failed)

    def auto_reply_recent_topics_with_llm(
        self,
        llm: LLMClient,
        max_age_days: int = 3,
        limit: int = 5,
        log=None,
    ) -> AutoReplyResult:
        scan_result = self.scan_taverna()
        skipped_topics = self.db.skip_topics_by_author(BOT_USER_ID, BOT_AUTHORED_SKIP_REASON)
        topics = self.db.get_recent_topics_pending_auto_reply(
            max_age_days=max_age_days,
            limit=limit,
            excluded_author_user_id=BOT_USER_ID,
        )
        style_profile = self.db.get_user_style_profile(BOT_USER_ID)
        if style_profile is None:
            raise ValueError("Bot style profile is not built yet. Run build-yakim-profile first.")

        processed = 0
        published = 0
        failed = 0
        details: list[str] = []

        summary = (
            f"Scan result: found={scan_result.found}, saved={scan_result.inserted_or_updated}, "
            f"new={scan_result.new_topics}, bot_authored_skipped={skipped_topics}, "
            f"reply_candidates={len(topics)}"
        )
        details.append(summary)
        self._emit(log, summary)

        if scan_result.new_topics > 0 or not topics:
            recent_topics = self.db.get_recently_seen_topics(limit=max(scan_result.new_topics, 5))
            self._emit(log, "Recent topics after scan:")
            for topic in recent_topics[: max(scan_result.new_topics, 5)]:
                eligible, reasons = self._explain_auto_reply_eligibility(topic, max_age_days=max_age_days)
                wait_minutes = None
                reply_not_before = topic.get("reply_not_before")
                if reply_not_before is not None:
                    wait_seconds = (reply_not_before - datetime.now(timezone.utc)).total_seconds()
                    wait_minutes = max(0, int(wait_seconds // 60))
                self._emit(
                    log,
                    "  "
                    f"{topic['forum_topic_id']} | eligible={eligible} | "
                    f"reasons={','.join(reasons) if reasons else 'ok'} | "
                    f"reply_count={topic.get('forum_reply_count')} | "
                    f"pinned={topic['is_pinned']} | closed={topic['is_closed']} | "
                    f"replied={topic['bot_replied_once']} | "
                    f"author_user_id={topic.get('author_user_id')} | "
                    f"skip_reason={topic.get('reply_skip_reason') or 'None'} | "
                    f"wait_minutes={wait_minutes if wait_minutes is not None else 'None'} | "
                    f"reply_not_before={self._format_dt(topic.get('reply_not_before'))} | "
                    f"created={self._format_dt(topic.get('created_at_forum'))} | "
                    f"first_seen={self._format_dt(topic.get('first_seen_at'))} | "
                    f"title={topic['title']}"
                )

        for topic in topics:
            processed += 1
            forum_topic_id = topic["forum_topic_id"]
            topic_url = topic["topic_url"]
            topic_title = topic["title"]
            start_message = f"Topic {forum_topic_id}: {topic_title}"
            details.append(start_message)
            self._emit(log, start_message)
            try:
                if not self.db.topic_has_starter_post(forum_topic_id):
                    self._emit(log, f"  Syncing starter post from {topic_url}")
                    self.sync_topic(topic_url)

                topic_data = self.db.get_topic_with_starter_post(forum_topic_id)
                if topic_data is None:
                    raise ValueError(f"Topic {forum_topic_id} was not found after sync.")

                reply_text = llm.generate_forum_reply(
                    topic_title=topic_data["title"],
                    topic_text=topic_data.get("content_text") or "",
                    style_profile=style_profile,
                )
                self._emit(log, f"  LLM reply generated: {len(reply_text)} chars")
                self.client.send_message_to_thread(topic_url, reply_text)
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text=reply_text,
                    status="llm_auto_published",
                    forum_post_id=topic_data.get("forum_post_id"),
                )
                self.db.mark_topic_replied(forum_topic_id)
                published += 1
                success_message = "  Published successfully and marked as replied."
                details.append(success_message)
                self._emit(log, success_message)
                time.sleep(10)
            except MessageSendError as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text="",
                    status="llm_auto_failed",
                    error_message=str(exc),
                )
                failed += 1
                error_message = f"  Publish failed: {exc}"
                details.append(error_message)
                self._emit(log, error_message)
                time.sleep(10)
            except Exception as exc:
                self.db.add_bot_reply(
                    forum_topic_id=forum_topic_id,
                    target_type="topic",
                    target_url=topic_url,
                    reply_text="",
                    status="llm_auto_failed",
                    error_message=str(exc),
                )
                failed += 1
                error_message = f"  Topic processing failed: {exc}"
                details.append(error_message)
                self._emit(log, error_message)

        return AutoReplyResult(
            scanned=scan_result.found,
            processed=processed,
            published=published,
            failed=failed,
            details=details,
        )

    def run_auto_reply_worker(
        self,
        llm: LLMClient,
        poll_interval_seconds: int = 30,
        max_age_days: int = 3,
        batch_limit: int = 5,
    ) -> None:
        cycle = 0

        def log(message: str) -> None:
            timestamp = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"[{timestamp}] {message}")

        while True:
            cycle += 1
            try:
                log(
                    f"Cycle #{cycle} started: interval={poll_interval_seconds}s, "
                    f"max_age_days={max_age_days}, batch_limit={batch_limit}"
                )
                result = self.auto_reply_recent_topics_with_llm(
                    llm=llm,
                    max_age_days=max_age_days,
                    limit=batch_limit,
                    log=log,
                )
                log(
                    f"Cycle #{cycle} finished: scanned={result.scanned}, "
                    f"processed={result.processed}, published={result.published}, failed={result.failed}"
                )
                log(f"Sleeping for {poll_interval_seconds} seconds before next cycle.")
                time.sleep(poll_interval_seconds)
            except KeyboardInterrupt:
                log("Worker stopped by user.")
                raise
            except Exception as exc:
                log(f"Cycle #{cycle} failed: {exc}")
                log(f"Sleeping for {poll_interval_seconds} seconds before retry.")
                time.sleep(poll_interval_seconds)

    def run_quote_reply_worker(
        self,
        llm: LLMClient,
        poll_interval_seconds: int = 30,
        batch_limit: int = 20,
    ) -> None:
        cycle = 0

        def log(message: str) -> None:
            timestamp = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"[{timestamp}] {message}")

        while True:
            cycle += 1
            try:
                log(
                    f"Quote worker cycle #{cycle} started: interval={poll_interval_seconds}s, "
                    f"batch_limit={batch_limit}"
                )
                result = self.reply_to_quote_notifications_with_llm(
                    llm=llm,
                    limit=batch_limit,
                    log=log,
                )
                log(
                    f"Quote worker cycle #{cycle} finished: scanned={result.scanned}, "
                    f"new={result.new_notifications}, processed={result.processed}, "
                    f"replied={result.replied}, ignored={result.ignored}, failed={result.failed}"
                )
                log(f"Sleeping for {poll_interval_seconds} seconds before next cycle.")
                time.sleep(poll_interval_seconds)
            except KeyboardInterrupt:
                log("Quote worker stopped by user.")
                raise
            except Exception as exc:
                log(f"Quote worker cycle #{cycle} failed: {exc}")
                log(f"Sleeping for {poll_interval_seconds} seconds before retry.")
                time.sleep(poll_interval_seconds)

    def publish_daily_taverna_summary(
        self,
        llm: LLMClient,
        lookback_hours: int = 24,
        force: bool = False,
        log=None,
    ) -> DailySummaryResult:
        now_local = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE)
        summary_date = now_local.date()
        title = f"Суммаризация - {summary_date.strftime('%d.%m.%Y')}"
        schedule = self.db.get_daily_summary_schedule()
        existing = self.db.get_daily_summary_run(summary_date)
        start_message = (
            f"Daily summary started: date={summary_date.isoformat()}, "
            f"lookback_hours={lookback_hours}, force={force}"
        )
        self._emit(log, start_message)

        if existing and existing["status"] == "in_progress" and not force:
            message = f"Daily summary for {summary_date.isoformat()} is already in progress."
            self._emit(log, message)
            return DailySummaryResult(
                summary_date=summary_date,
                scanned=0,
                topics_selected=0,
                status="already_running",
                topic_title=existing.get("topic_title"),
                topic_url=existing.get("topic_url"),
                details=[message],
            )

        if existing and existing["status"] == "published" and not force:
            message = f"Daily summary for {summary_date.isoformat()} already published: {existing.get('topic_url')}"
            self._emit(log, message)
            return DailySummaryResult(
                summary_date=summary_date,
                scanned=0,
                topics_selected=0,
                status="already_published",
                topic_title=existing.get("topic_title"),
                topic_url=existing.get("topic_url"),
                details=[message],
            )

        scan_result = self.scan_taverna()
        topics = self.db.get_topics_created_since(
            hours=lookback_hours,
            forum_section_id=6,
            exclude_pinned=True,
            exclude_closed=True,
            limit=50,
        )
        details = [
            (
                f"Fresh topics for summary: found={len(topics)}, "
                f"scan_found={scan_result.found}, scan_saved={scan_result.inserted_or_updated}"
            )
        ]
        for message in details:
            self._emit(log, message)

        if not topics:
            self.db.upsert_daily_summary_run(
                summary_date=summary_date,
                status="no_topics",
                scheduled_time=schedule.get("schedule_time"),
                topic_title=title,
                source_topic_count=0,
            )
            return DailySummaryResult(
                summary_date=summary_date,
                scanned=scan_result.found,
                topics_selected=0,
                status="no_topics",
                topic_title=title,
                details=details,
            )

        self.db.upsert_daily_summary_run(
            summary_date=summary_date,
            status="in_progress",
            scheduled_time=schedule.get("schedule_time"),
            topic_title=title,
            source_topic_count=len(topics),
        )

        try:
            payloads = []
            for topic in topics:
                self._emit(log, f"Collecting topic {topic['forum_topic_id']}: {topic['title']}")
                try:
                    _, posts = self._fetch_topic_thread_posts(topic["topic_url"])
                    if not posts:
                        skip_message = (
                            f"Skipping topic {topic['forum_topic_id']}: no posts were parsed "
                            f"from {topic['topic_url']}"
                        )
                        self._emit(log, skip_message)
                        details.append(skip_message)
                        continue
                    payloads.append(self._build_summary_topic_payload(topic=topic, posts=posts))
                except Exception as exc:
                    skip_message = (
                        f"Skipping topic {topic['forum_topic_id']} ({topic['topic_url']}): {exc}"
                    )
                    self._emit(log, skip_message)
                    details.append(skip_message)
                    continue

            if not payloads:
                raise ValueError("No valid topics were collected for daily summary.")

            body = llm.generate_taverna_daily_summary(
                summary_date=summary_date.strftime("%d.%m.%Y"),
                topics_payload=payloads,
            )
            body = self._normalize_generated_summary(body)
            created = self.client.create_topic(
                forum_id=6,
                title=title,
                content=body,
                subscribe=True,
                prefix=-1,
                pinned=False,
                referer_url="https://dota2.ru/forum/forums/taverna.6/create-thread/",
            )
            topic_url = created.get("redirect")
            self.db.upsert_daily_summary_run(
                summary_date=summary_date,
                status="published",
                scheduled_time=schedule.get("schedule_time"),
                topic_title=title,
                topic_url=topic_url,
                source_topic_count=len(payloads),
                summary_text=body,
            )
            success = f"Published daily summary: {topic_url}"
            self._emit(log, success)
            details.append(success)
            return DailySummaryResult(
                summary_date=summary_date,
                scanned=scan_result.found,
                topics_selected=len(payloads),
                status="published",
                topic_title=title,
                topic_url=topic_url,
                details=details,
            )
        except Exception as exc:
            error_message = f"Daily summary failed: {exc}"
            self._emit(log, error_message)
            self.db.upsert_daily_summary_run(
                summary_date=summary_date,
                status="failed",
                scheduled_time=schedule.get("schedule_time"),
                topic_title=title,
                source_topic_count=len(topics),
                error_message=str(exc),
            )
            details.append(error_message)
            return DailySummaryResult(
                summary_date=summary_date,
                scanned=scan_result.found,
                topics_selected=len(topics),
                status="failed",
                topic_title=title,
                details=details,
            )

    def run_daily_summary_worker(
        self,
        llm: LLMClient,
        poll_interval_seconds: int = 30,
        lookback_hours: int = 24,
    ) -> None:
        cycle = 0

        def log(message: str) -> None:
            timestamp = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"[{timestamp}] {message}")

        while True:
            cycle += 1
            try:
                schedule = self.db.get_daily_summary_schedule()
                if not schedule.get("enabled"):
                    log(f"Daily summary cycle #{cycle}: disabled, sleeping {poll_interval_seconds}s.")
                    time.sleep(poll_interval_seconds)
                    continue

                now_local = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE)
                schedule_time = self._parse_schedule_time(schedule.get("schedule_time"))
                scheduled_at = datetime.combine(now_local.date(), schedule_time, tzinfo=DISPLAY_TIMEZONE)
                existing = self.db.get_daily_summary_run(now_local.date())

                if existing:
                    if existing["status"] in {"published", "no_topics", "skipped"}:
                        log(
                            f"Daily summary cycle #{cycle}: today's run already exists "
                            f"with status={existing['status']}, sleeping {poll_interval_seconds}s."
                        )
                        time.sleep(poll_interval_seconds)
                        continue
                    if existing["status"] == "in_progress":
                        updated_at = existing.get("updated_at")
                        if updated_at is not None:
                            stale_after = updated_at + timedelta(
                                seconds=self.DAILY_SUMMARY_IN_PROGRESS_TIMEOUT_SECONDS
                            )
                            if now_local >= stale_after:
                                stale_message = (
                                    f"Daily summary cycle #{cycle}: in_progress run became stale, "
                                    f"marking as failed and retrying."
                                )
                                log(stale_message)
                                self.db.upsert_daily_summary_run(
                                    summary_date=now_local.date(),
                                    status="failed",
                                    scheduled_time=schedule.get("schedule_time"),
                                    topic_title=existing.get("topic_title"),
                                    topic_url=existing.get("topic_url"),
                                    source_topic_count=existing.get("source_topic_count") or 0,
                                    summary_text=existing.get("summary_text"),
                                    error_message=(
                                        "Daily summary run timed out while in progress."
                                    ),
                                )
                            else:
                                wait_seconds = max(0, int((stale_after - now_local).total_seconds()))
                                log(
                                    f"Daily summary cycle #{cycle}: today's run is still in progress, "
                                    f"stale in {wait_seconds}s."
                                )
                                time.sleep(min(poll_interval_seconds, max(1, wait_seconds)))
                                continue
                        else:
                            log(
                                f"Daily summary cycle #{cycle}: today's run is still in progress, "
                                f"sleeping {poll_interval_seconds}s."
                            )
                            time.sleep(poll_interval_seconds)
                            continue
                    if existing["status"] == "failed":
                        updated_at = existing.get("updated_at")
                        if updated_at is not None:
                            retry_at = updated_at + timedelta(seconds=self.DAILY_SUMMARY_RETRY_DELAY_SECONDS)
                            if now_local < retry_at:
                                wait_seconds = max(0, int((retry_at - now_local).total_seconds()))
                                log(
                                    f"Daily summary cycle #{cycle}: previous run failed, "
                                    f"retry after {wait_seconds}s."
                                )
                                time.sleep(min(poll_interval_seconds, max(1, wait_seconds)))
                                continue

                if now_local < scheduled_at:
                    log(
                        f"Daily summary cycle #{cycle}: waiting for schedule "
                        f"{schedule_time.strftime('%H:%M')}, sleeping {poll_interval_seconds}s."
                    )
                    time.sleep(poll_interval_seconds)
                    continue

                log(
                    f"Daily summary cycle #{cycle}: schedule reached "
                    f"({schedule_time.strftime('%H:%M')}), publishing summary."
                )
                result = self.publish_daily_taverna_summary(
                    llm=llm,
                    lookback_hours=lookback_hours,
                    force=False,
                    log=log,
                )
                log(
                    f"Daily summary cycle #{cycle} finished: "
                    f"status={result.status}, topics={result.topics_selected}, url={result.topic_url}"
                )
                time.sleep(poll_interval_seconds)
            except KeyboardInterrupt:
                log("Daily summary worker stopped by user.")
                raise
            except Exception as exc:
                log(f"Daily summary cycle #{cycle} failed: {exc}")
                time.sleep(poll_interval_seconds)

    def publish_daily_forum_topic(
        self,
        llm: LLMClient,
        force: bool = False,
        log=None,
    ) -> DailyTopicResult:
        now_local = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE)
        topic_date = now_local.date()
        schedule = self.db.get_daily_topic_schedule()
        existing = self.db.get_daily_topic_run(topic_date)
        prompt = self.db.get_active_topic_prompt(self.DAILY_TOPIC_PROMPT_CODE)
        if prompt is None:
            raise ValueError(f"Active prompt {self.DAILY_TOPIC_PROMPT_CODE!r} was not found in database.")

        start_message = f"Daily topic started: date={topic_date.isoformat()}, force={force}"
        self._emit(log, start_message)

        if existing and existing["status"] == "in_progress" and not force:
            message = f"Daily topic for {topic_date.isoformat()} is already in progress."
            self._emit(log, message)
            return DailyTopicResult(
                topic_date=topic_date,
                status="already_running",
                topic_title=existing.get("topic_title"),
                topic_url=existing.get("topic_url"),
                details=[message],
            )

        if existing and existing["status"] == "published" and not force:
            message = f"Daily topic for {topic_date.isoformat()} already published: {existing.get('topic_url')}"
            self._emit(log, message)
            return DailyTopicResult(
                topic_date=topic_date,
                status="already_published",
                topic_title=existing.get("topic_title"),
                topic_url=existing.get("topic_url"),
                details=[message],
            )

        recent_titles = self.db.get_recent_topic_titles(hours=24 * 14, forum_section_id=6, limit=100)
        existing_titles = {title.strip().lower() for title in recent_titles if title and title.strip()}
        self.db.upsert_daily_topic_run(
            topic_date=topic_date,
            status="in_progress",
            scheduled_time=schedule.get("schedule_time"),
            prompt_code=prompt["prompt_code"],
        )

        details = [f"Recent titles loaded for uniqueness check: {len(existing_titles)}"]
        for message in details:
            self._emit(log, message)

        try:
            generated_title = ""
            generated_body = ""
            for attempt in range(1, 4):
                self._emit(log, f"Generating daily topic attempt #{attempt}.")
                title, body = llm.generate_daily_forum_topic(
                    prompt_text=prompt["prompt_text"],
                    recent_titles=recent_titles,
                )
                title = self._normalize_generated_topic_title(title)
                body = self._normalize_generated_topic_body(body)
                generated_title = title
                generated_body = body
                normalized_title = title.lower()
                if normalized_title in existing_titles:
                    duplicate_message = f"Attempt #{attempt} returned duplicate title: {title}"
                    self._emit(log, duplicate_message)
                    details.append(duplicate_message)
                    recent_titles.append(title)
                    existing_titles.add(normalized_title)
                    continue
                break
            else:
                raise ValueError("Could not generate a unique daily topic title after 3 attempts.")

            created = self.client.create_topic(
                forum_id=6,
                title=generated_title,
                content=generated_body,
                subscribe=True,
                prefix=-1,
                pinned=False,
                referer_url="https://dota2.ru/forum/forums/taverna.6/create-thread/",
            )
            topic_url = created.get("redirect")
            self.db.upsert_daily_topic_run(
                topic_date=topic_date,
                status="published",
                scheduled_time=schedule.get("schedule_time"),
                prompt_code=prompt["prompt_code"],
                topic_title=generated_title,
                topic_body=generated_body,
                topic_url=topic_url,
            )
            success = f"Published daily topic: title={generated_title!r}, url={topic_url}"
            self._emit(log, success)
            details.append(success)
            return DailyTopicResult(
                topic_date=topic_date,
                status="published",
                topic_title=generated_title,
                topic_url=topic_url,
                details=details,
            )
        except Exception as exc:
            error_message = f"Daily topic failed: {exc}"
            self._emit(log, error_message)
            self.db.upsert_daily_topic_run(
                topic_date=topic_date,
                status="failed",
                scheduled_time=schedule.get("schedule_time"),
                prompt_code=prompt["prompt_code"],
                topic_title=generated_title or (existing.get("topic_title") if existing else None),
                topic_body=generated_body or (existing.get("topic_body") if existing else None),
                topic_url=existing.get("topic_url") if existing else None,
                error_message=str(exc),
            )
            details.append(error_message)
            return DailyTopicResult(
                topic_date=topic_date,
                status="failed",
                topic_title=generated_title or (existing.get("topic_title") if existing else None),
                topic_url=existing.get("topic_url") if existing else None,
                details=details,
            )

    def run_daily_topic_worker(
        self,
        llm: LLMClient,
        poll_interval_seconds: int = 30,
    ) -> None:
        cycle = 0

        def log(message: str) -> None:
            timestamp = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"[{timestamp}] {message}")

        while True:
            cycle += 1
            try:
                schedule = self.db.get_daily_topic_schedule()
                if not schedule.get("enabled"):
                    log(f"Daily topic cycle #{cycle}: disabled, sleeping {poll_interval_seconds}s.")
                    time.sleep(poll_interval_seconds)
                    continue

                now_local = datetime.now(timezone.utc).astimezone(DISPLAY_TIMEZONE)
                schedule_time = self._parse_schedule_time(schedule.get("schedule_time"))
                scheduled_at = datetime.combine(now_local.date(), schedule_time, tzinfo=DISPLAY_TIMEZONE)
                existing = self.db.get_daily_topic_run(now_local.date())

                if existing:
                    if existing["status"] in {"published", "skipped"}:
                        log(
                            f"Daily topic cycle #{cycle}: today's run already exists "
                            f"with status={existing['status']}, sleeping {poll_interval_seconds}s."
                        )
                        time.sleep(poll_interval_seconds)
                        continue
                    if existing["status"] == "in_progress":
                        updated_at = existing.get("updated_at")
                        if updated_at is not None:
                            stale_after = updated_at + timedelta(
                                seconds=self.DAILY_TOPIC_IN_PROGRESS_TIMEOUT_SECONDS
                            )
                            if now_local >= stale_after:
                                log(
                                    f"Daily topic cycle #{cycle}: in_progress run became stale, "
                                    f"marking as failed and retrying."
                                )
                                self.db.upsert_daily_topic_run(
                                    topic_date=now_local.date(),
                                    status="failed",
                                    scheduled_time=schedule.get("schedule_time"),
                                    prompt_code=existing.get("prompt_code"),
                                    topic_title=existing.get("topic_title"),
                                    topic_body=existing.get("topic_body"),
                                    topic_url=existing.get("topic_url"),
                                    error_message="Daily topic run timed out while in progress.",
                                )
                            else:
                                wait_seconds = max(0, int((stale_after - now_local).total_seconds()))
                                log(
                                    f"Daily topic cycle #{cycle}: today's run is still in progress, "
                                    f"stale in {wait_seconds}s."
                                )
                                time.sleep(min(poll_interval_seconds, max(1, wait_seconds)))
                                continue
                        else:
                            log(
                                f"Daily topic cycle #{cycle}: today's run is still in progress, "
                                f"sleeping {poll_interval_seconds}s."
                            )
                            time.sleep(poll_interval_seconds)
                            continue
                    if existing["status"] == "failed":
                        updated_at = existing.get("updated_at")
                        if updated_at is not None:
                            retry_at = updated_at + timedelta(seconds=self.DAILY_TOPIC_RETRY_DELAY_SECONDS)
                            if now_local < retry_at:
                                wait_seconds = max(0, int((retry_at - now_local).total_seconds()))
                                log(
                                    f"Daily topic cycle #{cycle}: previous run failed, "
                                    f"retry after {wait_seconds}s."
                                )
                                time.sleep(min(poll_interval_seconds, max(1, wait_seconds)))
                                continue

                if now_local < scheduled_at:
                    log(
                        f"Daily topic cycle #{cycle}: waiting for schedule "
                        f"{schedule_time.strftime('%H:%M')}, sleeping {poll_interval_seconds}s."
                    )
                    time.sleep(poll_interval_seconds)
                    continue

                log(
                    f"Daily topic cycle #{cycle}: schedule reached "
                    f"({schedule_time.strftime('%H:%M')}), publishing topic."
                )
                result = self.publish_daily_forum_topic(
                    llm=llm,
                    force=False,
                    log=log,
                )
                log(
                    f"Daily topic cycle #{cycle} finished: "
                    f"status={result.status}, title={result.topic_title}, url={result.topic_url}"
                )
                time.sleep(poll_interval_seconds)
            except KeyboardInterrupt:
                log("Daily topic worker stopped by user.")
                raise
            except Exception as exc:
                log(f"Daily topic cycle #{cycle} failed: {exc}")
                time.sleep(poll_interval_seconds)
