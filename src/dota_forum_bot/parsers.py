from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Iterable


THREAD_URL_RE = re.compile(r"(?:https://dota2\.ru)?/forum/threads/[^\"'?#\s>]+?\.(\d+)(?:/|\?action=unread)?")
MEMBER_URL_RE = re.compile(r"/forum/members/[^\"'?#\s>]+?\.(\d+)/?")
POST_URL_RE = re.compile(r"/forum/posts/(\d+)/")
TOPIC_POST_BLOCK_RE = re.compile(
    r'<div\b(?=[^>]*\bid="post-(\d+)")(?=[^>]*\bclass="[^"]*forum-theme__item[^"]*")[^>]*>',
    flags=re.IGNORECASE,
)


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
class PostReactionRecord:
    smile_id: int
    title: str
    count: int


@dataclass
class TopicThreadPostRecord:
    forum_post_id: int
    forum_topic_id: int
    author: ForumUserRecord | None
    post_url: str | None
    content_raw: str
    content_text: str
    created_at_forum: datetime | None
    post_number: int | None
    reactions: list[PostReactionRecord]
    total_reaction_count: int
    positive_reaction_count: int


@dataclass
class TopicThreadPageRecord:
    topic: TopicRecord
    posts: list[TopicThreadPostRecord]
    current_page: int
    total_pages: int


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


@dataclass
class QuoteNotificationRecord:
    forum_post_id: int
    post_url: str
    source_username: str | None
    source_user_id: int | None
    topic_title: str | None
    notification_text: str


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
    topic_id = _extract_topic_id_from_url(topic_url)
    if topic_id is None:
        raise ValueError(f"Unable to parse topic id from URL: {topic_url}")

    title = _extract_meta_title(topic_html) or _extract_html_title(topic_html) or f"Topic {topic_id}"
    author = _extract_first_user(topic_html)
    first_post_block = _extract_first_topic_post_block(topic_html)
    post_id = _extract_first_post_id(first_post_block)
    post_url = f"https://dota2.ru/forum/posts/{post_id}/" if post_id is not None else None
    content_raw = _extract_post_content_html(first_post_block)
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


def parse_topic_thread_page(topic_url: str, topic_html: str) -> TopicThreadPageRecord:
    topic_id = _extract_topic_id_from_url(topic_url)
    if topic_id is None:
        raise ValueError(f"Unable to parse topic id from URL: {topic_url}")

    title = _extract_meta_title(topic_html) or _extract_html_title(topic_html) or f"Topic {topic_id}"
    posts = _extract_topic_thread_posts(topic_id=topic_id, html_text=topic_html)
    topic_author = posts[0].author if posts else _extract_first_user(topic_html)

    topic = TopicRecord(
        forum_topic_id=topic_id,
        forum_section_id=6,
        title=title,
        topic_url=_normalize_topic_url(topic_url),
        author=topic_author,
        created_at_forum=posts[0].created_at_forum if posts else None,
        forum_reply_count=max(0, len(posts) - 1),
        is_closed="closedtopic" in topic_html.lower() or "данная тема закрыта" in topic_html.lower(),
        is_pinned="sticky" in topic_html.lower() or "закреп" in topic_html.lower(),
    )
    return TopicThreadPageRecord(
        topic=topic,
        posts=posts,
        current_page=_extract_current_page(topic_url),
        total_pages=_extract_total_pages(topic_html),
    )


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
        created_at_forum = datetime.fromtimestamp(int(time_match.group(1)), tz=timezone.utc) if time_match else None

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


def parse_quote_notifications(html_text: str) -> list[QuoteNotificationRecord]:
    records: list[QuoteNotificationRecord] = []
    seen_post_ids: set[int] = set()

    blocks = _extract_notification_item_blocks(html_text)
    for block in blocks:
        context_text = _normalize_space(_html_to_text(block))
        normalized_text = context_text.lower().replace("ё", "е")
        if "процитировал ваше сообщение" not in normalized_text:
            continue

        match = re.search(
            r'href="((?:https://dota2\.ru)?/forum/posts/(\d+)/)"',
            block,
            flags=re.IGNORECASE,
        )
        if match is None:
            continue

        forum_post_id = int(match.group(2))
        if forum_post_id in seen_post_ids:
            continue

        source_username = None
        source_user_id = None
        source_user = _extract_user_from_context(block)
        if source_user is not None:
            source_username = source_user.username
            source_user_id = source_user.forum_user_id
        else:
            author_match = re.search(
                r"пользователь\s+(.+?)\s+процитировал\s+ваше\s+сообщение",
                context_text,
                flags=re.IGNORECASE,
            )
            if author_match:
                source_username = _normalize_space(author_match.group(1))

        topic_title = None
        topic_link_match = re.search(
            r'href="([^"]*(?:threads/[^"]+?\.\d+|posts/\d+/)[^"]*)"[^>]*>(.*?)</a>',
            block,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if topic_link_match:
            topic_title = _extract_anchor_text(topic_link_match.group(2))
        if not topic_title:
            topic_match = re.search(
                r"в\s+теме\s+(.+)$",
                context_text,
                flags=re.IGNORECASE,
            )
            if topic_match:
                topic_title = _normalize_space(topic_match.group(1))

        records.append(
            QuoteNotificationRecord(
                forum_post_id=forum_post_id,
                post_url=_to_absolute_url(match.group(1)),
                source_username=source_username,
                source_user_id=source_user_id,
                topic_title=topic_title,
                notification_text=context_text,
            )
        )
        seen_post_ids.add(forum_post_id)

    return records


def _extract_notification_item_blocks(html_text: str) -> list[str]:
    starts = list(
        re.finditer(
            r'<div class="notices-body__items-item background">',
            html_text,
            flags=re.IGNORECASE,
        )
    )
    blocks: list[str] = []
    for index, match in enumerate(starts):
        start = match.start()
        end = starts[index + 1].start() if index + 1 < len(starts) else len(html_text)
        blocks.append(html_text[start:end])
    return blocks


def extract_quoted_text(raw_html: str) -> str:
    parts = [
        _cleanup_quote_artifacts(_html_to_text(fragment))
        for fragment in _extract_top_level_blockquote_html(raw_html)
    ]
    cleaned = [_normalize_space(part) for part in parts if _normalize_space(part)]
    return "\n\n".join(cleaned)


def extract_post_message_text(raw_html: str) -> str:
    text = _normalize_space(_html_to_text(_remove_blockquotes_html(raw_html)))
    return _cleanup_post_message_artifacts(text)


def parse_quote_notifications_api(notices: list[dict]) -> list[QuoteNotificationRecord]:
    records: list[QuoteNotificationRecord] = []
    seen_post_ids: set[int] = set()

    for item in notices:
        description_html = str(item.get("description") or "")
        description_text = _normalize_space(_html_to_text(description_html))
        normalized_text = description_text.lower().replace("ё", "е")
        if "процитировал ваше сообщение" not in normalized_text:
            continue

        post_match = re.search(
            r'href="((?:https://dota2\.ru)?/forum/posts/(\d+)/)"',
            description_html,
            flags=re.IGNORECASE,
        )
        if post_match is None:
            continue

        forum_post_id = int(post_match.group(2))
        if forum_post_id in seen_post_ids:
            continue

        source_username = None
        source_user_id = None
        sender = item.get("sender")
        if isinstance(sender, dict):
            source_username = str(sender.get("username") or "").strip() or None
            raw_sender_id = sender.get("user_id") or sender.get("id")
            try:
                source_user_id = int(raw_sender_id) if raw_sender_id is not None else None
            except (TypeError, ValueError):
                source_user_id = None

        if source_username is None:
            source_username = str(item.get("sender.username") or "").strip() or None

        if source_user_id is None:
            profile_candidates = [
                str(item.get("link") or ""),
                description_html,
            ]
            for candidate in profile_candidates:
                profile_match = re.search(
                    r'(?:https://dota2\.ru)?/forum/members/[^"\']+?\.(\d+)/?',
                    candidate,
                    flags=re.IGNORECASE,
                )
                if profile_match:
                    source_user_id = int(profile_match.group(1))
                    break

        if source_username is None:
            source_user = _extract_user_from_context(description_html)
            if source_user is not None:
                source_username = source_user.username
                source_user_id = source_user_id or source_user.forum_user_id

        topic_title = None
        topic_match = re.search(
            r'href="[^"]*(?:threads/[^"]+?\.\d+|posts/\d+/)[^"]*"[^>]*>(.*?)</a>\s*$',
            description_html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if topic_match:
            topic_title = _extract_anchor_text(topic_match.group(1))

        records.append(
            QuoteNotificationRecord(
                forum_post_id=forum_post_id,
                post_url=_to_absolute_url(post_match.group(1)),
                source_username=source_username,
                source_user_id=source_user_id,
                topic_title=topic_title,
                notification_text=description_text,
            )
        )
        seen_post_ids.add(forum_post_id)

    return records


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
    return datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)


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
    if not html_text:
        return None

    match = re.search(r'id="post-(\d+)"', html_text)
    if match:
        return int(match.group(1))

    match = POST_URL_RE.search(html_text)
    if match:
        return int(match.group(1))

    return None


def _extract_first_post_html(html_text: str) -> str:
    return _extract_message_text_html(html_text)


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


class _QuoteHTMLExtractor(HTMLParser):
    _QUOTE_META_MARKERS = (
        "quoteexpand",
        "quote-expand",
        "quoteheader",
        "bbcodeblock-title",
        "bbcodeblock-expandlink",
    )

    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self.blockquote_depth = 0
        self.skip_meta_depth = 0
        self.outside_parts: list[str] = []
        self.quote_parts: list[list[str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "blockquote":
            if self.blockquote_depth == 0:
                self.quote_parts.append([])
            self.blockquote_depth += 1
            return

        if self._should_skip_quote_meta(normalized_tag, attrs):
            self.skip_meta_depth = 1
            return

        if self.skip_meta_depth > 0:
            self.skip_meta_depth += 1
            return

        self._append_markup(self.get_starttag_text(), include_inside_quote=self.blockquote_depth == 1)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "blockquote":
            return
        if self._should_skip_quote_meta(normalized_tag, attrs) or self.skip_meta_depth > 0:
            return
        self._append_markup(self.get_starttag_text(), include_inside_quote=self.blockquote_depth == 1)

    def handle_endtag(self, tag: str) -> None:
        normalized_tag = tag.lower()
        if normalized_tag == "blockquote":
            if self.blockquote_depth > 0:
                self.blockquote_depth -= 1
            return

        if self.skip_meta_depth > 0:
            self.skip_meta_depth -= 1
            return

        self._append_markup(f"</{tag}>", include_inside_quote=self.blockquote_depth == 1)

    def handle_data(self, data: str) -> None:
        if self.skip_meta_depth > 0:
            return
        self._append_markup(data, include_inside_quote=self.blockquote_depth == 1)

    def handle_entityref(self, name: str) -> None:
        if self.skip_meta_depth > 0:
            return
        self._append_markup(f"&{name};", include_inside_quote=self.blockquote_depth == 1)

    def handle_charref(self, name: str) -> None:
        if self.skip_meta_depth > 0:
            return
        self._append_markup(f"&#{name};", include_inside_quote=self.blockquote_depth == 1)

    def _append_markup(self, markup: str, include_inside_quote: bool) -> None:
        if self.blockquote_depth == 0:
            self.outside_parts.append(markup)
        elif include_inside_quote and self.quote_parts:
            self.quote_parts[-1].append(markup)

    def _should_skip_quote_meta(self, tag: str, attrs: list[tuple[str, str | None]]) -> bool:
        if self.blockquote_depth != 1:
            return False
        if tag == "cite":
            return True

        attr_values = " ".join((value or "") for _, value in attrs).lower()
        return any(marker in attr_values for marker in self._QUOTE_META_MARKERS)


def _extract_top_level_blockquote_html(raw_html: str) -> list[str]:
    parser = _QuoteHTMLExtractor()
    parser.feed(raw_html or "")
    parser.close()
    return ["".join(parts).strip() for parts in parser.quote_parts if "".join(parts).strip()]


def _remove_blockquotes_html(raw_html: str) -> str:
    parser = _QuoteHTMLExtractor()
    parser.feed(raw_html or "")
    parser.close()
    return "".join(parser.outside_parts)


def _cleanup_quote_artifacts(value: str) -> str:
    text = value.strip()
    text = re.sub(
        r"^.*?Нажмите,\s*чтобы\s*раскрыть\.{0,3}\s*",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    lines = [line.strip() for line in text.splitlines()]
    cleaned_lines = [
        line
        for line in lines
        if line
        and "нажмите, чтобы раскрыть" not in line.lower()
        and not re.fullmatch(r".+сказал\(а\):\s*↑?", line, flags=re.IGNORECASE)
    ]
    return "\n".join(cleaned_lines).strip()


def _cleanup_post_message_artifacts(value: str) -> str:
    text = value.strip()
    text = re.sub(r"^.+?\s+сказал\(а\):\s*↑?\s*", "", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"\bНажмите,\s*чтобы\s*раскрыть\.{0,3}\b", "", text, flags=re.IGNORECASE)
    return _normalize_space(text)


def _extract_topic_thread_posts(topic_id: int, html_text: str) -> list[TopicThreadPostRecord]:
    posts: list[TopicThreadPostRecord] = []
    blocks = _extract_topic_post_blocks(html_text)

    for block in blocks:
        post_id_match = re.search(r'<div id="post-(\d+)"', block)
        if not post_id_match:
            continue

        post_id = int(post_id_match.group(1))
        author = _extract_author_from_post_block(block)
        created_at = _extract_post_created_at(block)
        post_url = _extract_post_url(block)
        post_number = _extract_post_number(block)
        content_raw = _extract_post_content_html(block)
        content_text = _html_to_text(content_raw)
        reactions = _extract_post_reactions(block)

        posts.append(
            TopicThreadPostRecord(
                forum_post_id=post_id,
                forum_topic_id=topic_id,
                author=author,
                post_url=post_url,
                content_raw=content_raw,
                content_text=content_text,
                created_at_forum=created_at,
                post_number=post_number,
                reactions=reactions,
                total_reaction_count=sum(item.count for item in reactions),
                positive_reaction_count=sum(
                    item.count for item in reactions if "dislike" not in item.title.lower()
                ),
            )
        )

    return posts


def _extract_topic_post_blocks(html_text: str) -> list[str]:
    starts = list(TOPIC_POST_BLOCK_RE.finditer(html_text))
    blocks: list[str] = []
    for index, match in enumerate(starts):
        start = match.start()
        end = starts[index + 1].start() if index + 1 < len(starts) else len(html_text)
        blocks.append(html_text[start:end])
    return blocks


def _extract_first_topic_post_block(html_text: str) -> str | None:
    blocks = _extract_topic_post_blocks(html_text)
    return blocks[0] if blocks else None


def _extract_author_from_post_block(block: str) -> ForumUserRecord | None:
    match = re.search(
        r'data-user-id="(\d+)"\s+data-username="([^"]+)"',
        block,
        flags=re.IGNORECASE,
    )
    if match:
        profile_match = re.search(r'href="([^"]*members/[^"]+?\.\d+/?)"', block)
        return ForumUserRecord(
            forum_user_id=int(match.group(1)),
            username=html.unescape(match.group(2)),
            profile_url=_to_absolute_url(profile_match.group(1)) if profile_match else None,
        )
    return _extract_user_from_context(block)


def _extract_post_created_at(block: str) -> datetime | None:
    match = re.search(r'<time[^>]+data-time="(\d+)"', block, flags=re.IGNORECASE)
    if not match:
        return None
    return datetime.fromtimestamp(int(match.group(1)), tz=timezone.utc)


def _extract_post_url(block: str) -> str | None:
    match = re.search(
        r'<a href="([^"]+#post-\d+|/forum/threads/[^"]+#post-\d+)"[^>]+class="item item-post-link',
        block,
        flags=re.IGNORECASE,
    )
    if not match:
        return None
    return _to_absolute_url(match.group(1))


def _extract_post_number(block: str) -> int | None:
    match = re.search(
        r'class="item item-post-link[^"]*"[^>]*>\s*<span>#(\d+)</span>',
        block,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    return int(match.group(1))


def _extract_post_content_html(block: str) -> str:
    if not block:
        return ""

    extracted = _extract_message_text_html(block)
    if extracted:
        return extracted
    return _extract_first_post_html(block)


class _FirstMatchingElementExtractor(HTMLParser):
    def __init__(self, matcher) -> None:
        super().__init__(convert_charrefs=False)
        self.matcher = matcher
        self.depth = 0
        self.parts: list[str] = []
        self.captured = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.captured:
            return
        if self.depth == 0 and self.matcher(tag, attrs):
            self.depth = 1
            return
        if self.depth > 0:
            self.parts.append(self.get_starttag_text())
            self.depth += 1

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if self.captured:
            return
        if self.depth == 0 and self.matcher(tag, attrs):
            self.captured = True
            return
        if self.depth > 0:
            self.parts.append(self.get_starttag_text())

    def handle_endtag(self, tag: str) -> None:
        if self.captured or self.depth == 0:
            return
        self.depth -= 1
        if self.depth == 0:
            self.captured = True
            return
        self.parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if not self.captured and self.depth > 0:
            self.parts.append(data)

    def handle_entityref(self, name: str) -> None:
        if not self.captured and self.depth > 0:
            self.parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if not self.captured and self.depth > 0:
            self.parts.append(f"&#{name};")


def _extract_message_text_html(html_text: str) -> str:
    def matcher(tag: str, attrs: list[tuple[str, str | None]]) -> bool:
        if tag.lower() not in {"blockquote", "div"}:
            return False
        attr_map = {key.lower(): (value or "") for key, value in attrs}
        classes = attr_map.get("class", "")
        element_id = attr_map.get("id", "")
        return "messagetext" in classes.lower() or element_id.startswith("message-content-")

    parser = _FirstMatchingElementExtractor(matcher)
    parser.feed(html_text or "")
    parser.close()
    return "".join(parser.parts).strip()


def _extract_post_reactions(block: str) -> list[PostReactionRecord]:
    match = re.search(r":data-rated='([^']*)'", block, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    raw_json = html.unescape(match.group(1))
    try:
        payload = json.loads(raw_json)
    except Exception:
        return []

    aggregated: dict[int, PostReactionRecord] = {}
    for item in payload:
        try:
            smile_id = int(item.get("smile_id"))
        except (TypeError, ValueError):
            continue

        title = str(item.get("smile.title") or item.get("title") or "").strip()
        try:
            count = int(item.get("smiles.count") or item.get("count") or 0)
        except (TypeError, ValueError):
            count = 0

        existing = aggregated.get(smile_id)
        if existing is None or count > existing.count:
            aggregated[smile_id] = PostReactionRecord(smile_id=smile_id, title=title, count=count)

    return sorted(aggregated.values(), key=lambda item: (-item.count, item.title.lower(), item.smile_id))


def _normalize_space(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _extract_topic_id_from_url(topic_url: str) -> int | None:
    match = re.search(r"/threads/[^/]+\.(\d+)(?:/page-\d+)?/?(?:[?#].*)?$", topic_url)
    if not match:
        return None
    return int(match.group(1))


def _extract_current_page(topic_url: str) -> int:
    match = re.search(r"/page-(\d+)", topic_url)
    if not match:
        return 1
    return int(match.group(1))


def _extract_total_pages(html_text: str) -> int:
    pages = [1]
    data_pages_match = re.search(r'<ul class="pagination"[^>]*data-pages="(\d+)"', html_text)
    if data_pages_match:
        pages.append(int(data_pages_match.group(1)))
    pages.extend(int(match.group(1)) for match in re.finditer(r"/page-(\d+)", html_text))
    return max(pages)


def _normalize_topic_url(topic_url: str) -> str:
    normalized = re.sub(r"#post-\d+$", "", topic_url)
    normalized = re.sub(r"/page-\d+/?(?:\?.*)?$", "/", normalized)
    normalized = re.sub(r"([?#].*)$", "", normalized)
    return normalized.rstrip("/") + "/"


def _to_absolute_url(path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if path.startswith("/"):
        return f"https://dota2.ru{path}"
    return f"https://dota2.ru/{path}"
