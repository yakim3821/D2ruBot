from __future__ import annotations

import re


def build_topic_draft(title: str, content_text: str) -> str:
    normalized = _normalize_text(content_text)
    excerpt = normalized[:280].rstrip()

    if _looks_like_question(title, normalized):
        body = (
            f"Тема: {title}\n"
            f"Коротко по стартовому сообщению: {excerpt or 'без текста'}\n\n"
            "Черновик ответа:\n"
            "Похоже, тут сначала стоит уточнить пару деталей, иначе можно промахнуться с советом. "
            "Если опишешь чуть подробнее исходные условия и что уже пробовал, будет проще ответить по делу."
        )
    else:
        body = (
            f"Тема: {title}\n"
            f"Коротко по стартовому сообщению: {excerpt or 'без текста'}\n\n"
            "Черновик ответа:\n"
            "Понял общий контекст. Тут можно либо обсудить ситуацию подробнее, либо сразу перейти к конкретике, "
            "если есть цель, с которой ты это поднимаешь."
        )

    return body.strip()


def _looks_like_question(title: str, content_text: str) -> bool:
    if "?" in title or "?" in content_text:
        return True
    question_words = ("как ", "почему ", "зачем ", "что ", "где ", "когда ", "стоит ли", "можно ли")
    haystack = f"{title.lower()} {content_text.lower()}"
    return any(word in haystack for word in question_words)


def _normalize_text(value: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip()
    return value[:1200]
