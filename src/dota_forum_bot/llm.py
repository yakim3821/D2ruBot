from __future__ import annotations

import json

from .exceptions import ForumBotError


class LLMClient:
    def __init__(self, api_key: str, model: str, reasoning_effort: str = "low") -> None:
        if not api_key.strip():
            raise ForumBotError("OPENAI_API_KEY is not set in .env.")

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise ForumBotError("OpenAI SDK is not installed. Run `pip install -r requirements.txt`.") from exc

        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.reasoning_effort = reasoning_effort

    def generate_forum_reply(
        self,
        topic_title: str,
        topic_text: str,
        style_profile: dict,
    ) -> str:
        instructions = (
            "Ты пишешь ответ для форума dota2.ru. "
            "Нужно ответить по теме коротко, естественно и по-человечески. "
            "Стиль должен быть нейтральным, но слегка разговорным, без переигрывания. "
            "Не упоминай, что ты бот, ИИ или модель. "
            "Не копируй дословно примеры из профиля. "
            "Не используй токсичность, угрозы, оскорбления, политику и личные данные. "
            "Верни только готовый текст ответа без пояснений."
        )

        payload = {
            "style_summary": style_profile.get("style_summary"),
            "tone": style_profile.get("tone"),
            "lexicon": style_profile.get("lexicon"),
            "signature_phrases": style_profile.get("signature_phrases"),
            "preferred_topics": style_profile.get("preferred_topics"),
            "example_messages": style_profile.get("example_messages"),
        }

        user_input = (
            f"Заголовок темы:\n{topic_title}\n\n"
            f"Стартовый пост:\n{topic_text}\n\n"
            f"Профиль стиля:\n{json.dumps(payload, ensure_ascii=False)}\n\n"
            "Сгенерируй один короткий уместный ответ для этой темы. "
            "Длина: примерно 1-4 предложения."
        )

        response = self.client.responses.create(
            model=self.model,
            instructions=instructions,
            input=user_input,
            reasoning={"effort": self.reasoning_effort},
            max_output_tokens=220,
        )

        output_text = getattr(response, "output_text", "") or ""
        output_text = output_text.strip()
        if not output_text:
            raise ForumBotError("OpenAI returned an empty response.")
        return output_text
