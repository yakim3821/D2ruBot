from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import random

from .client import Dota2ForumClient
from .db import Database
from .drafts import build_topic_draft
from .exceptions import MessageSendError
from .llm_client import LLMClient
from .parsers import (
    TopicPageRecord,
    TopicRecord,
    parse_profile_posts_page,
    parse_profile_posts_total_pages,
    parse_taverna_topics,
    parse_topic_page,
)
from .style_profile import build_style_profile, profile_to_db_payload


TAVERNA_URL = "https://dota2.ru/forum/forums/taverna.6/"
TAVERNA_SCOPE = "forum_section:taverna"
YAKIM38_PROFILE_POSTS_URL = "https://dota2.ru/forum/members/yakim38.815329/activity/posts/"
YAKIM38_USER_ID = 815329
YAKIM38_USERNAME = "Yakim38"


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


class ForumSyncService:
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
            return value.strftime("%Y-%m-%d %H:%M:%S")
        return str(value)

    def _explain_auto_reply_eligibility(self, topic: dict, max_age_days: int) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if topic.get("bot_replied_once"):
            reasons.append("already_replied")
        if topic.get("is_closed"):
            reasons.append("closed")
        if topic.get("is_pinned"):
            reasons.append("pinned")

        created_at = topic.get("created_at_forum") or topic.get("first_seen_at")
        if created_at is None:
            reasons.append("missing_created_at")
        else:
            if getattr(created_at, "tzinfo", None) is None:
                now = datetime.now()
            else:
                now = datetime.now(created_at.tzinfo)
            cutoff = now - timedelta(days=max_age_days)
            if created_at < cutoff:
                reasons.append("older_than_max_age")

        reply_not_before = topic.get("reply_not_before")
        if reply_not_before is None:
            reasons.append("reply_not_scheduled")
        else:
            if getattr(reply_not_before, "tzinfo", None) is None:
                now = datetime.now()
            else:
                now = datetime.now(reply_not_before.tzinfo)
            if reply_not_before > now:
                reasons.append("not_due_yet")

        return (len(reasons) == 0, reasons)

    def scan_taverna(self) -> ScanResult:
        response = self.client.fetch_page(TAVERNA_URL)
        topics = parse_taverna_topics(response.text)

        changed = 0
        new_topics = 0
        last_topic_id = None

        for topic in topics:
            exists = self.db.topic_exists(topic.forum_topic_id)
            self.db.upsert_topic(topic)
            if not exists or self.db.topic_needs_reply_schedule(topic.forum_topic_id):
                delay_minutes = self._human_reply_delay_minutes(topic.forum_reply_count)
                reply_not_before = datetime.now() + timedelta(minutes=delay_minutes)
                self.db.set_topic_reply_schedule(topic.forum_topic_id, reply_not_before)
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
        response = self.client.fetch_page(topic_url)
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
        first_page = self.client.fetch_page(YAKIM38_PROFILE_POSTS_URL)
        total_pages = parse_profile_posts_total_pages(first_page.text)
        pages_to_scan = min(max_pages, total_pages)
        saved = 0

        for page_num in range(1, pages_to_scan + 1):
            if page_num == 1:
                response = first_page
                page_url = YAKIM38_PROFILE_POSTS_URL
            else:
                page_url = f"{YAKIM38_PROFILE_POSTS_URL}page-{page_num}"
                response = self.client.fetch_page(page_url)

            posts = parse_profile_posts_page(
                profile_user_id=YAKIM38_USER_ID,
                profile_username=YAKIM38_USERNAME,
                page_url=page_url,
                html_text=response.text,
            )
            for post in posts:
                self.db.upsert_user_profile_post(post)
                saved += 1

        return ProfileSyncResult(pages_scanned=pages_to_scan, posts_saved=saved, total_pages=total_pages)

    def build_yakim_style_profile(self, limit: int | None = None) -> StyleProfileResult:
        posts = self.db.get_user_profile_posts(YAKIM38_USER_ID, limit=limit)
        messages = [post["content_text"] for post in posts if post.get("content_text")]
        topics = [post["topic_title"] for post in posts if post.get("topic_title")]

        profile = build_style_profile(messages=messages, topic_titles=topics)
        payload = profile_to_db_payload(profile)
        self.db.upsert_user_style_profile(
            forum_user_id=YAKIM38_USER_ID,
            source_profile_url=YAKIM38_PROFILE_POSTS_URL,
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
            forum_user_id=YAKIM38_USER_ID,
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
        style_profile = self.db.get_user_style_profile(YAKIM38_USER_ID)
        if style_profile is None:
            raise ValueError("Yakim38 style profile is not built yet. Run build-yakim-profile first.")

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
        topics = self.db.get_recent_topics_pending_auto_reply(max_age_days=max_age_days, limit=limit)
        style_profile = self.db.get_user_style_profile(YAKIM38_USER_ID)
        if style_profile is None:
            raise ValueError("Yakim38 style profile is not built yet. Run build-yakim-profile first.")

        processed = 0
        published = 0
        failed = 0
        details: list[str] = []

        summary = (
            f"Scan result: found={scan_result.found}, saved={scan_result.inserted_or_updated}, "
            f"new={scan_result.new_topics}, reply_candidates={len(topics)}"
        )
        details.append(summary)
        self._emit(log, summary)

        if scan_result.new_topics > 0:
            recent_topics = self.db.get_recently_seen_topics(limit=max(scan_result.new_topics, 5))
            self._emit(log, "Recent topics after scan:")
            for topic in recent_topics[: max(scan_result.new_topics, 5)]:
                eligible, reasons = self._explain_auto_reply_eligibility(topic, max_age_days=max_age_days)
                self._emit(
                    log,
                    "  "
                    f"{topic['forum_topic_id']} | eligible={eligible} | "
                    f"reasons={','.join(reasons) if reasons else 'ok'} | "
                    f"reply_count={topic.get('forum_reply_count')} | "
                    f"pinned={topic['is_pinned']} | closed={topic['is_closed']} | "
                    f"replied={topic['bot_replied_once']} | "
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
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
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
