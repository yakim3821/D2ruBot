from __future__ import annotations

import html
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable


THREAD_URL_RE = re.compile(r"(?:https://dota2\.ru)?/forum/threads/[^\"'?#\s>]+?\.(\d+)(?:/|\?action=unread)?")
MEMBER_URL_RE = re.compile(r"/forum/members/[^\"'?#\s>]+?\.(\d+)/?")
POST_URL_RE = re.compile(r"/forum/posts/(\d+)/")


@dataclass
class ForumUserRecord:
    forum_user_id: int
    username: str
    profile_url: str | None = None


@dataclass
class TopicRecord:
    forum_topic_id: int
    forum_section_id: int | None
    title: str
    topic_url: str
    author: ForumUserRecord | None
    created_at_forum: datetime | None = None
    last_post_at_forum: datetime | None = None
    forum_reply_count: int | None = None
    is_closed: bool = False
    is_pinned: bool = False


@dataclass
class PostRecord:
    forum_post_id: int
    forum_topic_id: int
    author: ForumUserRecord | None
    post_url: str | None
    content_raw: str
    content_text: str
    created_at_forum: datetime | None
    is_topic_starter: bool
    reply_to_post_id: int | None = None


@dataclass
class TopicPageRecord:
    topic: TopicRecord
    first_post: PostRecord


@dataclass
class UserProfilePostRecord:
    source_profile_user_id: int
    source_profile_username: str
    forum_post_id: int
    post_url: str
    topic_title: str
    forum_section_name: str | None
    forum_section_url: str | None
    content_text: str
    created_at_forum: datetime | None
    activity_page_url: str


def parse_taverna_topics(section_html: str) -> list[TopicRecord]:
    list_match = re.search(
        r'<ul class="forum-section__list">(.*?)</ul>',
        section_html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if list_match:
        list_html = list_match.group(1)
        topics = _parse_taverna_topic_blocks(list_html)
        if topics:
            return topics

    topics: list[TopicRecord] = []
    seen_topic_ids: set[int] = set()

    for match in THREAD_URL_RE.finditer(section_html):
        topic_id = int(match.group(1))
        if topic_id in seen_topic_ids:
            continue

        anchor_start = section_html.rfind("<a", 0, match.start())
        anchor_end = section_html.find("</a>", match.end())
        if anchor_start == -1 or anchor_end == -1:
            continue

        anchor_html = section_html[anchor_start : anchor_end + 4]
        title = _extract_anchor_text(anchor_html)
        if not title:
            continue

        context_start = max(0, anchor_start - 2500)
        context_end = min(len(section_html), anchor_end + 2500)
        context = section_html[context_start:context_end]

        author = _extract_user_from_context(context)
        reply_count = _extract_topic_reply_count(context)
        is_closed = "closed" in context.lower() or "закрыта" in context.lower()
        is_pinned = "sticky" in context.lower() or "pinned" in context.lower() or "закреп" in context.lower()

        topics.append(
            TopicRecord(
                forum_topic_id=topic_id,
                forum_section_id=6,
                title=title,
                topic_url=match.group(0),
                author=author,
                forum_reply_count=reply_count,
                is_closed=is_closed,
                is_pinned=is_pinned,
            )
        )
        seen_topic_ids.add(topic_id)

    return topics


def _parse_taverna_topic_blocks(list_html: str) -> list[TopicRecord]:
    topics: list[TopicRecord] = []
    item_matches = list(re.finditer(r'<li id="topic-(\d+)"[^>]*class="([^"]*)"', list_html))

    for index, match in enumerate(item_matches):
        start = match.start()
        end = item_matches[index + 1].start() if index + 1 < len(item_matches) else len(list_html)
        block = list_html[start:end]
        topic_id = int(match.group(1))
        classes = match.group(2).lower()

        href_match = None
        for candidate in re.finditer(r'href="([^"]+/forum/threads/[^"]+|/forum/threads/[^"]+)"', block):
            href = candidate.group(1)
            if "?action=unread" in href:
                continue
            href_match = candidate
            break

        if href_match is None:
            continue

        topic_url = _to_absolute_url(href_match.group(1))

        anchor_start = block.rfind("<a", 0, href_match.end())
        anchor_end = block.find("</a>", href_match.end())
        if anchor_start == -1 or anchor_end == -1:
            continue
        title = _extract_anchor_text(block[anchor_start : anchor_end + 4])
        if not title:
            continue

        author = _extract_author_from_topic_block(block)
        created_at = _extract_topic_created_at(block)

        topics.append(
            TopicRecord(
                forum_topic_id=topic_id,
                forum_section_id=6,
                title=title,
                topic_url=topic_url,
                author=author,
                created_at_forum=created_at,
                forum_reply_count=_extract_topic_reply_count(block),
                is_closed="closed" in classes,
                is_pinned="sticky" in classes,
            )
        )

    return topics


def parse_topic_page(topic_url: str, topic_html: str) -> TopicPageRecord:
    topic_id_match = re.search(r"/threads/[^/]+\.(\d+)/?$", topic_url)
    if not topic_id_match:
        raise ValueError(f"Unable to parse topic id from URL: {topic_url}")
    topic_id = int(topic_id_match.group(1))

    title = _extract_meta_title(topic_html) or _extract_html_title(topic_html) or f"Topic {topic_id}"
    author = _extract_first_user(topic_html)

    post_id = _extract_first_post_id(topic_html)
    post_url = f"https://dota2.ru/forum/posts/{post_id}/" if post_id is not None else None
    content_raw = _extract_first_post_html(topic_html)
    content_text = _html_to_text(content_raw)

    topic = TopicRecord(
        forum_topic_id=topic_id,
        forum_section_id=6,
        title=title,
        topic_url=topic_url,
        author=author,
        is_closed="closedtopic" in topic_html.lower() or "данная тема закрыта" in topic_html.lower(),
        is_pinned="sticky" in topic_html.lower() or "закреп" in topic_html.lower(),
    )

    post = PostRecord(
        forum_post_id=post_id or topic_id,
        forum_topic_id=topic_id,
        author=author,
        post_url=post_url,
        content_raw=content_raw,
        content_text=content_text,
        created_at_forum=None,
        is_topic_starter=True,
    )

    return TopicPageRecord(topic=topic, first_post=post)


def parse_profile_posts_page(profile_user_id: int, profile_username: str, page_url: str, html_text: str) -> list[UserProfilePostRecord]:
    blocks = _extract_profile_post_blocks(html_text)
    records: list[UserProfilePostRecord] = []

    for block in blocks:
        post_match = re.search(r'href="(/forum/posts/(\d+))"', block)
        if not post_match:
            continue

        topic_title_match = re.search(r'В теме\s*<a href="/forum/posts/\d+">(.*?)</a>', block, flags=re.IGNORECASE | re.DOTALL)
        section_match = re.search(r'раздела\s*<a href="([^"]+)">(.*?)</a>', block, flags=re.IGNORECASE | re.DOTALL)
        content_match = re.search(
            r'<div class="forum-profile__content-block-active-post">\s*(.*?)\s*</div>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        time_match = re.search(r'<time[^>]+data-time="(\d+)"', block, flags=re.IGNORECASE)

        topic_title = _normalize_space(html.unescape(re.sub(r"<[^>]+>", " ", topic_title_match.group(1)))) if topic_title_match else ""
        forum_section_name = (
            _normalize_space(html.unescape(re.sub(r"<[^>]+>", " ", section_match.group(2))))
            if section_match
            else None
        )
        forum_section_url = _to_absolute_url(section_match.group(1)) if section_match else None
        content_text = _normalize_space(html.unescape(re.sub(r"<[^>]+>", " ", content_match.group(1)))) if content_match else ""
        created_at_forum = datetime.fromtimestamp(int(time_match.group(1))) if time_match else None

        records.append(
            UserProfilePostRecord(
                source_profile_user_id=profile_user_id,
                source_profile_username=profile_username,
                forum_post_id=int(post_match.group(2)),
                post_url=_to_absolute_url(post_match.group(1)),
                topic_title=topic_title,
                forum_section_name=forum_section_name,
                forum_section_url=forum_section_url,
                content_text=content_text,
                created_at_forum=created_at_forum,
                activity_page_url=page_url,
            )
        )

    return records


def parse_profile_posts_total_pages(html_text: str) -> int:
    match = re.search(r'<ul class="pagination"[^>]*data-per-page="\d+"[^>]*data-pages="(\d+)"', html_text)
    if match:
        return int(match.group(1))
    return 1


def _extract_anchor_text(anchor_html: str) -> str:
    anchor_html = re.sub(r"<script.*?</script>", "", anchor_html, flags=re.IGNORECASE | re.DOTALL)
    anchor_html = re.sub(r"<style.*?</style>", "", anchor_html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", anchor_html)
    return _normalize_space(html.unescape(text))


def _extract_profile_post_blocks(html_text: str) -> list[str]:
    marker = '<div class="forum-profile__content-block-active-block">'
    starts = []
    search_from = 0
    while True:
        idx = html_text.find(marker, search_from)
        if idx == -1:
            break
        starts.append(idx)
        search_from = idx + len(marker)

    blocks: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else html_text.find('<div class="mb24">', start)
        if end == -1:
            end = len(html_text)
        blocks.append(html_text[start:end])

    return blocks


def _extract_user_from_context(context: str) -> ForumUserRecord | None:
    user_matches = list(MEMBER_URL_RE.finditer(context))
    if not user_matches:
        return None

    for match in user_matches:
        anchor_start = context.rfind("<a", 0, match.start())
        anchor_end = context.find("</a>", match.end())
        if anchor_start == -1 or anchor_end == -1:
            continue
        anchor_html = context[anchor_start : anchor_end + 4]
        username = _extract_anchor_text(anchor_html)
        if not username:
            continue
        return ForumUserRecord(
            forum_user_id=int(match.group(1)),
            username=username,
            profile_url=_to_absolute_url(match.group(0)),
        )

    return None


def _extract_first_user(html_text: str) -> ForumUserRecord | None:
    first_chunk = html_text[:15000]
    return _extract_user_from_context(first_chunk)


def _extract_author_from_topic_block(block: str) -> ForumUserRecord | None:
    match = re.search(
        r'<a[^>]+class="user-link"[^>]+href="([^"]*members/[^"]+?\.(\d+)/?)"[^>]*>\s*(.*?)\s*</a>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return _extract_user_from_context(block)

    return ForumUserRecord(
        forum_user_id=int(match.group(2)),
        username=_normalize_space(html.unescape(re.sub(r"<[^>]+>", " ", match.group(3)))),
        profile_url=_to_absolute_url(match.group(1)),
    )


def _extract_topic_created_at(block: str) -> datetime | None:
    match = re.search(
        r'<span[^>]+class="date-time"[^>]+data-time="(\d+)"[^>]*title="Дата создания темы"',
        block,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return datetime.fromtimestamp(int(match.group(1)))


def _extract_topic_reply_count(block: str) -> int | None:
    for pattern in (r"Сообщений:\s*<span>([\d\.\s]+)</span>", r"Сообщений:\s*([\d\.\s]+)"):
        match = re.search(pattern, block, flags=re.IGNORECASE)
        if not match:
            continue
        digits = re.sub(r"\D", "", match.group(1))
        if digits:
            return int(digits)
    return None


def _extract_first_post_id(html_text: str) -> int | None:
    match = re.search(r'id="post-(\d+)"', html_text)
    if match:
        return int(match.group(1))

    match = POST_URL_RE.search(html_text)
    if match:
        return int(match.group(1))

    return None


def _extract_first_post_html(html_text: str) -> str:
    block_patterns: Iterable[str] = (
        r'<blockquote[^>]+class="[^"]*messageText[^"]*"[^>]*>(.*?)</blockquote>',
        r'<div[^>]+class="[^"]*messageText[^"]*"[^>]*>(.*?)</div>',
    )
    for pattern in block_patterns:
        match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).strip()

    return ""


def _extract_meta_title(html_text: str) -> str | None:
    match = re.search(r'<meta property="og:title" content="([^"]+)"', html_text)
    if match:
        return _normalize_space(html.unescape(match.group(1)))
    return None


def _extract_html_title(html_text: str) -> str | None:
    match = re.search(r"<title>(.*?)</title>", html_text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return _normalize_space(html.unescape(match.group(1)))


def _html_to_text(raw_html: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", raw_html, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    return _normalize_space(html.unescape(text.replace("\xa0", " ")))


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _to_absolute_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        return f"https://dota2.ru{path}"
    return f"https://dota2.ru/{path}"
