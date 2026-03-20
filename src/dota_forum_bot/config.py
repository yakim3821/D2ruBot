from __future__ import annotations

import os
from dataclasses import dataclass

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
        test_thread_url = os.getenv("DOTA2_FORUM_TEST_THREAD_URL", "").strip() or None
        test_message = os.getenv("DOTA2_FORUM_TEST_MESSAGE", "").strip() or None

        return cls(
            username=username,
            password=password,
            base_url=base_url,
            remember_me=remember_me,
            session_file=session_file,
            test_thread_url=test_thread_url,
            test_message=test_message,
        )
