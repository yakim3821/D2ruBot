from __future__ import annotations

import os
from dataclasses import dataclass

from .db import DatabaseSettings

ENV_FILE = ".env"


def load_local_env(path: str = ENV_FILE) -> None:
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            os.environ.setdefault(key, value)


load_local_env()


@dataclass(frozen=True)
class Settings:
    username: str
    password: str
    base_url: str
    remember_me: bool
    session_file: str
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    deepseek_api_key: str
    deepseek_model: str
    deepseek_base_url: str
    test_conversation_url: str | None
    test_thread_url: str | None
    test_message: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        username = os.getenv("DOTA2_FORUM_USERNAME", "").strip()
        password = os.getenv("DOTA2_FORUM_PASSWORD", "").strip()
        base_url = os.getenv("DOTA2_FORUM_BASE_URL", "https://dota2.ru").strip().rstrip("/")
        remember_raw = os.getenv("DOTA2_FORUM_REMEMBER_ME", "true").strip().lower()
        remember_me = remember_raw in {"1", "true", "yes", "on"}
        session_file = os.getenv("DOTA2_FORUM_SESSION_FILE", "session.json").strip() or "session.json"
        db_host = os.getenv("DOTA2_FORUM_DB_HOST", "127.0.0.1").strip() or "127.0.0.1"
        db_port = int(os.getenv("DOTA2_FORUM_DB_PORT", "5432").strip() or "5432")
        db_name = os.getenv("DOTA2_FORUM_DB_NAME", "dota_forum_bot").strip() or "dota_forum_bot"
        db_user = os.getenv("DOTA2_FORUM_DB_USER", "dota_forum_bot").strip() or "dota_forum_bot"
        db_password = os.getenv("DOTA2_FORUM_DB_PASSWORD", "dota_forum_bot").strip()
        deepseek_api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
        deepseek_model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
        deepseek_base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip().rstrip("/")
        test_conversation_url = os.getenv(
            "DOTA2_FORUM_TEST_CONVERSATION_URL",
            "https://dota2.ru/forum/conversation/123.1039856/",
        ).strip() or None
        test_thread_url = os.getenv("DOTA2_FORUM_TEST_THREAD_URL", "").strip() or None
        test_message = os.getenv("DOTA2_FORUM_TEST_MESSAGE", "").strip() or None

        return cls(
            username=username,
            password=password,
            base_url=base_url,
            remember_me=remember_me,
            session_file=session_file,
            db_host=db_host,
            db_port=db_port,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            deepseek_api_key=deepseek_api_key,
            deepseek_model=deepseek_model,
            deepseek_base_url=deepseek_base_url,
            test_conversation_url=test_conversation_url,
            test_thread_url=test_thread_url,
            test_message=test_message,
        )

    def db_settings(self) -> DatabaseSettings:
        return DatabaseSettings(
            host=self.db_host,
            port=self.db_port,
            name=self.db_name,
            user=self.db_user,
            password=self.db_password,
        )
