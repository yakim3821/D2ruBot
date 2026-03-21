from __future__ import annotations

import argparse
import sys

from .client import Dota2ForumClient
from .config import Settings
from .db import Database
from .exceptions import ForumBotError
from .llm_client import LLMClient
from .services import ForumSyncService
from .ui import run_ui_server


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dota-forum-bot")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("login-check", help="Log in and verify that the session is authenticated.")
    subparsers.add_parser("send-test", help="Log in and send a test message to the configured thread URL.")
    subparsers.add_parser("scan-taverna", help="Scan Taverna section and save discovered topics to PostgreSQL.")

    sync_topic_parser = subparsers.add_parser("sync-topic", help="Read one topic page and save its starter post.")
    sync_topic_parser.add_argument("url", help="Absolute forum topic URL.")

    list_topics_parser = subparsers.add_parser("list-new-topics", help="List topics where the bot has not replied yet.")
    list_topics_parser.add_argument("--limit", type=int, default=20, help="Maximum number of topics to print.")

    draft_topics_parser = subparsers.add_parser(
        "draft-new-topics",
        help="Sync new topics and send draft replies into the safe test conversation.",
    )
    draft_topics_parser.add_argument("--limit", type=int, default=5, help="Maximum number of draft topics to process.")

    publish_topics_parser = subparsers.add_parser(
        "publish-drafted-topics",
        help="Publish previously drafted replies into real forum topics.",
    )
    publish_topics_parser.add_argument("--limit", type=int, default=1, help="Maximum number of drafted topics to publish.")

    profile_posts_parser = subparsers.add_parser(
        "sync-yakim-posts",
        help="Sync Yakim38 profile posts into PostgreSQL for future style profiling.",
    )
    profile_posts_parser.add_argument("--pages", type=int, default=3, help="How many activity pages to scan.")

    profile_build_parser = subparsers.add_parser(
        "build-yakim-profile",
        help="Build and store a basic style profile for Yakim38 from synced profile posts.",
    )
    profile_build_parser.add_argument("--limit", type=int, default=200, help="How many synced profile posts to use.")

    llm_draft_parser = subparsers.add_parser(
        "llm-draft-new-topics",
        help="Generate LLM replies for new topics and send them only to the safe test conversation.",
    )
    llm_draft_parser.add_argument("--limit", type=int, default=2, help="Maximum number of topics to process.")

    llm_publish_parser = subparsers.add_parser(
        "publish-llm-drafted-topics",
        help="Publish LLM-generated drafts from the test conversation pipeline into real forum topics.",
    )
    llm_publish_parser.add_argument("--limit", type=int, default=1, help="Maximum number of topics to publish.")

    worker_parser = subparsers.add_parser(
        "run-auto-reply-worker",
        help="Run a background worker that scans Taverna and auto-replies once per fresh topic.",
    )
    worker_parser.add_argument("--interval", type=int, default=30, help="Seconds between cycles and retry delay after errors.")
    worker_parser.add_argument("--max-age-days", type=int, default=3, help="Reply only to topics not older than this many days.")
    worker_parser.add_argument("--batch-limit", type=int, default=5, help="Maximum number of fresh topics to process per cycle.")

    daily_summary_parser = subparsers.add_parser(
        "publish-daily-summary",
        help="Build and publish a daily Taverna summary thread for topics from the last 24 hours.",
    )
    daily_summary_parser.add_argument("--lookback-hours", type=int, default=24, help="How many past hours to include.")
    daily_summary_parser.add_argument("--force", action="store_true", help="Publish even if today's summary run already exists.")

    daily_summary_worker_parser = subparsers.add_parser(
        "run-daily-summary-worker",
        help="Run a background worker that publishes one Taverna summary thread per day at the configured UI time.",
    )
    daily_summary_worker_parser.add_argument("--interval", type=int, default=30, help="Seconds between schedule checks.")
    daily_summary_worker_parser.add_argument("--lookback-hours", type=int, default=24, help="How many past hours to include.")

    daily_summary_schedule_parser = subparsers.add_parser(
        "set-daily-summary-schedule",
        help="Enable/disable daily summary schedule and set its publish time without using the UI.",
    )
    daily_summary_schedule_parser.add_argument(
        "--time",
        dest="schedule_time",
        help="Publish time in HH:MM format, for example 19:35.",
    )
    daily_summary_schedule_group = daily_summary_schedule_parser.add_mutually_exclusive_group()
    daily_summary_schedule_group.add_argument(
        "--enabled",
        action="store_true",
        help="Enable the daily summary schedule.",
    )
    daily_summary_schedule_group.add_argument(
        "--disabled",
        action="store_true",
        help="Disable the daily summary schedule.",
    )

    ui_parser = subparsers.add_parser(
        "run-ui",
        help="Run optional local web UI for command запуск, logs, and worker status.",
    )
    ui_parser.add_argument("--host", default="127.0.0.1", help="Bind host for the UI server.")
    ui_parser.add_argument("--port", type=int, default=8080, help="Bind port for the UI server.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "run-ui":
        try:
            run_ui_server(settings=settings, host=args.host, port=args.port)
            return 0
        except ForumBotError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1
        except KeyboardInterrupt:
            print("Stopped by user.")
            return 0

    client = Dota2ForumClient(base_url=settings.base_url, session_file=settings.session_file)
    db = Database(settings.db_settings())
    service = ForumSyncService(client=client, db=db)

    try:
        auth_mode = client.ensure_authenticated(
            settings.username,
            settings.password,
            remember=settings.remember_me,
        )
        if auth_mode == "restored":
            print(f"Session restored from {settings.session_file}. Session is authenticated.")
        else:
            print(f"Login successful. Session saved to {settings.session_file}.")

        if args.command == "send-test":
            if not settings.test_thread_url or not settings.test_message:
                raise ForumBotError(
                    "DOTA2_FORUM_TEST_THREAD_URL and DOTA2_FORUM_TEST_MESSAGE must be set for send-test."
                )

            result = client.send_message_to_thread(settings.test_thread_url, settings.test_message)
            print(f"Send-test completed: {result}")
        elif args.command == "scan-taverna":
            result = service.scan_taverna()
            print(
                f"Scanned Taverna: found={result.found}, "
                f"saved={result.inserted_or_updated}, new={result.new_topics}"
            )
        elif args.command == "sync-topic":
            topic_page = service.sync_topic(args.url)
            print(
                f"Synced topic {topic_page.topic.forum_topic_id}: "
                f"title={topic_page.topic.title!r}, starter_post={topic_page.first_post.forum_post_id}"
            )
        elif args.command == "list-new-topics":
            topics = service.list_new_topics(limit=args.limit)
            if not topics:
                print("No unreplied topics found.")
            else:
                for topic in topics:
                    print(f"{topic['forum_topic_id']}: {topic['title']} -> {topic['topic_url']}")
        elif args.command == "draft-new-topics":
            if not settings.test_conversation_url:
                raise ForumBotError("DOTA2_FORUM_TEST_CONVERSATION_URL must be set for draft-new-topics.")
            result = service.draft_new_topics_to_conversation(
                conversation_url=settings.test_conversation_url,
                limit=args.limit,
            )
            print(
                f"Draft processing finished: processed={result.processed}, "
                f"sent={result.sent}, failed={result.failed}"
            )
        elif args.command == "publish-drafted-topics":
            result = service.publish_drafted_topics(limit=args.limit)
            print(
                f"Publish finished: processed={result.processed}, "
                f"published={result.published}, failed={result.failed}"
            )
        elif args.command == "sync-yakim-posts":
            result = service.sync_user_profile_posts(max_pages=args.pages)
            print(
                f"Yakim38 posts synced: pages_scanned={result.pages_scanned}, "
                f"posts_saved={result.posts_saved}, total_pages={result.total_pages}"
            )
        elif args.command == "build-yakim-profile":
            result = service.build_yakim_style_profile(limit=args.limit)
            print(
                f"Yakim38 style profile built: forum_user_id={result.forum_user_id}, "
                f"posts_used={result.posts_used}, confidence={result.confidence_score}"
            )
        elif args.command == "llm-draft-new-topics":
            if not settings.test_conversation_url:
                raise ForumBotError("DOTA2_FORUM_TEST_CONVERSATION_URL must be set for llm-draft-new-topics.")
            llm = LLMClient(
                api_key=settings.deepseek_api_key,
                model=settings.deepseek_model,
                base_url=settings.deepseek_base_url,
            )
            result = service.draft_new_topics_with_llm(
                llm=llm,
                conversation_url=settings.test_conversation_url,
                limit=args.limit,
            )
            print(
                f"LLM draft processing finished: processed={result.processed}, "
                f"sent={result.sent}, failed={result.failed}"
            )
        elif args.command == "publish-llm-drafted-topics":
            result = service.publish_llm_drafted_topics(limit=args.limit)
            print(
                f"LLM publish finished: processed={result.processed}, "
                f"published={result.published}, failed={result.failed}"
            )
        elif args.command == "run-auto-reply-worker":
            llm = LLMClient(
                api_key=settings.deepseek_api_key,
                model=settings.deepseek_model,
                base_url=settings.deepseek_base_url,
            )
            print(
                f"Auto-reply worker started: interval={args.interval}s, "
                f"max_age_days={args.max_age_days}, batch_limit={args.batch_limit}"
            )
            service.run_auto_reply_worker(
                llm=llm,
                poll_interval_seconds=args.interval,
                max_age_days=args.max_age_days,
                batch_limit=args.batch_limit,
            )
        elif args.command == "publish-daily-summary":
            llm = LLMClient(
                api_key=settings.deepseek_api_key,
                model=settings.deepseek_model,
                base_url=settings.deepseek_base_url,
            )
            result = service.publish_daily_taverna_summary(
                llm=llm,
                lookback_hours=args.lookback_hours,
                force=args.force,
            )
            print(
                f"Daily summary finished: status={result.status}, "
                f"topics={result.topics_selected}, url={result.topic_url or '-'}"
            )
            for detail in result.details:
                print(detail)
        elif args.command == "run-daily-summary-worker":
            llm = LLMClient(
                api_key=settings.deepseek_api_key,
                model=settings.deepseek_model,
                base_url=settings.deepseek_base_url,
            )
            print(
                f"Daily summary worker started: interval={args.interval}s, "
                f"lookback_hours={args.lookback_hours}"
            )
            service.run_daily_summary_worker(
                llm=llm,
                poll_interval_seconds=args.interval,
                lookback_hours=args.lookback_hours,
            )
        elif args.command == "set-daily-summary-schedule":
            current = db.get_daily_summary_schedule()
            schedule_time = args.schedule_time or current.get("schedule_time") or "12:00"
            if args.enabled:
                enabled = True
            elif args.disabled:
                enabled = False
            else:
                enabled = bool(current.get("enabled"))
            db.set_daily_summary_schedule(enabled=enabled, schedule_time=schedule_time)
            updated = db.get_daily_summary_schedule()
            print(
                f"Daily summary schedule updated: enabled={updated['enabled']}, "
                f"time={updated['schedule_time']}"
            )

        return 0
    except ForumBotError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Stopped by user.")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
