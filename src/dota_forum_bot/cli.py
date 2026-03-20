from __future__ import annotations

import argparse
import sys

from .client import Dota2ForumClient
from .config import Settings
from .exceptions import ForumBotError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dota-forum-bot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("login-check", help="Log in and verify that the session is authenticated.")
    subparsers.add_parser("send-test", help="Log in and send a test message to the configured thread URL.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()
    client = Dota2ForumClient(base_url=settings.base_url)

    try:
        client.login(settings.username, settings.password, remember=settings.remember_me)
        print("Login successful. Session is authenticated.")

        if args.command == "send-test":
            if not settings.test_thread_url or not settings.test_message:
                raise ForumBotError(
                    "DOTA2_FORUM_TEST_THREAD_URL and DOTA2_FORUM_TEST_MESSAGE must be set for send-test."
                )

            result = client.send_message_to_thread(settings.test_thread_url, settings.test_message)
            print(f"Send-test completed: {result}")

        return 0
    except ForumBotError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
