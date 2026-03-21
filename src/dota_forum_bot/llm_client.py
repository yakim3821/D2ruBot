from __future__ import annotations

import json

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
