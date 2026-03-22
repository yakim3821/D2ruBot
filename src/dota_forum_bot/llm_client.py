from __future__ import annotations

import json
import re

from .exceptions import ForumBotError


class LLMClient:
    def __init__(self, api_key: str, model: str, base_url: str) -> None:
        if not api_key.strip():
            raise ForumBotError("DEEPSEEK_API_KEY is not set in .env.")

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ForumBotError("OpenAI SDK is not installed. Run `pip install -r requirements.txt`.") from exc

        self.client = OpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    def generate_forum_reply(
        self,
        topic_title: str,
        topic_text: str,
        style_profile: dict,
    ) -> str:
        system_prompt = (
            "You write short forum replies for dota2.ru. "
            "Reply naturally, briefly, and like a real person. "
            "Keep the tone neutral with a light conversational feel. "
            "Do not mention bots, AI, or models. "
            "Do not copy example messages verbatim. "
            "Avoid toxicity, threats, insults, politics, and personal data leaks. "
            "Return only the final reply text."
        )

        payload = {
            "style_summary": style_profile.get("style_summary"),
            "tone": style_profile.get("tone"),
            "lexicon": style_profile.get("lexicon"),
            "signature_phrases": style_profile.get("signature_phrases"),
            "preferred_topics": style_profile.get("preferred_topics"),
            "example_messages": style_profile.get("example_messages"),
        }

        user_prompt = (
            f"Topic title:\n{topic_title}\n\n"
            f"Starter post:\n{topic_text}\n\n"
            f"Style profile:\n{json.dumps(payload, ensure_ascii=False)}\n\n"
            "Generate one short relevant reply for this topic. "
            "Target length: about 1 to 4 sentences."
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=220,
            temperature=0.7,
        )

        output_text = ""
        if response.choices:
            message = response.choices[0].message
            output_text = (message.content or "").strip()
        if not output_text:
            raise ForumBotError("DeepSeek returned an empty response.")
        return output_text

    def generate_taverna_daily_summary(
        self,
        summary_date: str,
        topics_payload: list[dict],
    ) -> str:
        if not topics_payload:
            raise ForumBotError("No topic data was provided for daily summary generation.")

        system_prompt = (
            "Ты пишешь итоговый пост для форума dota2.ru в разделе Таверна. "
            "Нужно сделать живую, читаемую сводку за последние сутки по свежим темам. "
            "Пиши по-русски, естественно, без канцелярита и без упоминания ИИ. "
            "Соблюдай точный BBCode-формат: короткое вступление, затем по одному SPOILER на тему, затем блок 'Итоги дня'. "
            "Каждый спойлер обязан содержать 4 части в таком виде: "
            "1) первый абзац без заголовка, сразу краткое содержание темы; "
            "2) второй абзац без заголовка, о чем писали пользователи; "
            "3) третий абзац без заголовка, интересные моменты; "
            "4) отдельный блок с заголовком 'Самые популярные комментарии:' и списком комментариев по строкам. "
            "Фразы-заголовки 'Краткое содержание темы', 'Краткое содержание о чем писали юзеры', 'Интересные моменты' писать нельзя. "
            "Между абзацами внутри спойлера обязательно оставляй пустую строку. "
            "Не используй markdown-кодблоки. Не выдумывай темы и факты вне переданных данных. "
            "Если комментарий токсичный, передай смысл мягче и нейтральнее."
        )

        user_prompt = (
            f"Дата публикации: {summary_date}\n\n"
            "Верни только готовый текст поста в таком формате:\n"
            "Вступительный текст...\n"
            '[SPOILER=\"Название темы\"]\n'
            "Первый абзац без заголовка\n\n"
            "Второй абзац без заголовка\n\n"
            "Третий абзац без заголовка\n\n"
            "Самые популярные комментарии:\n"
            "user: comment\n"
            "user: comment\n"
            "[/SPOILER]\n"
            "...\n"
            "Итоги дня\n\n"
            f"Данные по темам:\n{json.dumps(topics_payload, ensure_ascii=False)}"
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=2200,
            temperature=0.6,
        )

        output_text = ""
        if response.choices:
            message = response.choices[0].message
            output_text = (message.content or "").strip()
        if not output_text:
            raise ForumBotError("DeepSeek returned an empty daily summary.")
        return output_text

    def generate_daily_forum_topic(
        self,
        prompt_text: str,
        recent_titles: list[str] | None = None,
    ) -> tuple[str, str]:
        recent_titles = recent_titles or []
        system_prompt = (
            "Ты генерируешь новую тему для форума dota2.ru в разделе Таверна. "
            "Нужно вернуть только итоговый результат в строгом формате без пояснений. "
            "Сначала строка 'TITLE: <заголовок>', потом строка 'POST:', потом текст первого поста. "
            "Заголовок и пост должны выглядеть как реальные, живые и разговорные. "
            "Не упоминай ИИ, ботов или промты. "
            "Не копируй дословно недавние заголовки из списка."
        )

        user_prompt = (
            f"Промт для генерации:\n{prompt_text}\n\n"
            f"Недавние заголовки, которых нужно избегать:\n{json.dumps(recent_titles, ensure_ascii=False)}\n\n"
            "Верни результат только в таком виде:\n"
            "TITLE: короткий заголовок\n"
            "POST:\n"
            "текст первого поста"
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=600,
            temperature=0.95,
        )

        output_text = ""
        if response.choices:
            message = response.choices[0].message
            output_text = (message.content or "").strip()
        if not output_text:
            raise ForumBotError("DeepSeek returned an empty daily topic.")

        match = re.search(r"TITLE:\s*(.+?)\nPOST:\s*(.+)", output_text, flags=re.DOTALL | re.IGNORECASE)
        if not match:
            raise ForumBotError(f"Daily topic response has invalid format: {output_text[:200]}")

        title = match.group(1).strip()
        post = match.group(2).strip()
        if not title or not post:
            raise ForumBotError("Daily topic response is missing title or post body.")
        return title, post
