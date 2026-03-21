from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass


STOPWORDS = {
    "и", "в", "во", "на", "с", "со", "по", "к", "ко", "из", "за", "у", "от", "до", "под", "над",
    "не", "ни", "а", "но", "или", "ли", "же", "то", "это", "этот", "эта", "эти", "как", "что", "где",
    "когда", "почему", "зачем", "я", "ты", "он", "она", "мы", "вы", "они", "мне", "меня", "тебя",
    "его", "ее", "их", "мой", "моя", "мои", "твой", "твоя", "свой", "своя", "бы", "был", "была",
    "были", "быть", "есть", "нет", "да", "ну", "вот", "так", "тут", "там", "для", "при", "если",
    "чтобы", "уже", "еще", "ещё", "просто", "очень", "всё", "все", "сам", "сама", "само", "самый",
}


@dataclass
class StyleProfile:
    style_summary: str
    lexicon: dict
    signature_phrases: list[str]
    preferred_topics: list[str]
    tone: str
    message_length_stats: dict
    example_messages: list[str]
    confidence_score: float


def build_style_profile(messages: list[str], topic_titles: list[str]) -> StyleProfile:
    cleaned_messages = [normalize_text(message) for message in messages if normalize_text(message)]
    cleaned_topics = [normalize_text(title) for title in topic_titles if normalize_text(title)]

    lengths = [len(message) for message in cleaned_messages]
    word_counter = Counter()
    phrase_counter = Counter()
    topic_counter = Counter(cleaned_topics)

    for message in cleaned_messages:
        words = extract_words(message)
        word_counter.update(word for word in words if word not in STOPWORDS and len(word) >= 4)
        phrase_counter.update(extract_phrases(words))

    avg_length = round(sum(lengths) / len(lengths), 2) if lengths else 0.0
    tone = detect_tone(cleaned_messages)
    signature_phrases = [phrase for phrase, _ in phrase_counter.most_common(10)]
    preferred_topics = [topic for topic, _ in topic_counter.most_common(10)]
    example_messages = cleaned_messages[:5]
    top_words = word_counter.most_common(20)

    style_summary = (
        f"Пишет в {tone} тоне. "
        f"Средняя длина сообщения около {int(avg_length) if avg_length else 0} символов. "
        f"Часто использует бытовые и форумные формулировки, предпочитает прямую подачу без формальностей."
    )

    confidence = min(100.0, round(len(cleaned_messages) * 1.25, 2))

    return StyleProfile(
        style_summary=style_summary,
        lexicon={"top_words": [{"word": word, "count": count} for word, count in top_words]},
        signature_phrases=signature_phrases,
        preferred_topics=preferred_topics,
        tone=tone,
        message_length_stats={
            "count": len(cleaned_messages),
            "avg_chars": avg_length,
            "min_chars": min(lengths) if lengths else 0,
            "max_chars": max(lengths) if lengths else 0,
        },
        example_messages=example_messages,
        confidence_score=confidence,
    )


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").replace("\xa0", " ")).strip()


def extract_words(text: str) -> list[str]:
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", text.lower())


def extract_phrases(words: list[str]) -> list[str]:
    phrases: list[str] = []
    for size in (2, 3):
        for index in range(len(words) - size + 1):
            chunk = words[index : index + size]
            if any(word in STOPWORDS for word in chunk):
                continue
            phrase = " ".join(chunk)
            if len(phrase) >= 8:
                phrases.append(phrase)
    return phrases


def detect_tone(messages: list[str]) -> str:
    if not messages:
        return "нейтральном"

    lowered = " ".join(messages).lower()
    if any(token in lowered for token in ("работяги", "чел", "челы", "чил", "зп", "форум")):
        return "разговорном"
    if any(token in lowered for token in ("проблема", "нужно", "стоит", "лучше", "если")):
        return "практичном"
    return "нейтральном"


def profile_to_db_payload(profile: StyleProfile) -> dict:
    return {
        "style_summary": profile.style_summary,
        "lexicon": json.dumps(profile.lexicon, ensure_ascii=False),
        "signature_phrases": json.dumps(profile.signature_phrases, ensure_ascii=False),
        "preferred_topics": json.dumps(profile.preferred_topics, ensure_ascii=False),
        "tone": profile.tone,
        "message_length_stats": json.dumps(profile.message_length_stats, ensure_ascii=False),
        "example_messages": json.dumps(profile.example_messages, ensure_ascii=False),
        "confidence_score": profile.confidence_score,
    }
