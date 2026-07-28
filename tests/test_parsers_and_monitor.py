import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dota_forum_bot.parsers import extract_post_message_text
from dota_forum_bot.services import ForumSyncService


class PostMessageParsingTests(unittest.TestCase):
    def test_spoiler_images_are_preserved_as_bbcode(self) -> None:
        raw_html = """
        <p>Rate image</p>
        <div class="spoiler">
            <div class="spoiler-container">
                <span class="button-theme spoiler-btn"><abbr>Spoiler</abbr></span>
            </div>
            <div class="spoiler-content" style="display:none;">
                <p>
                    <img alt="one.jpg" data-src="https://i.example.test/one.jpg" />
                    <br>
                    <img alt="two.jpg" src="https://i.example.test/two.jpg" />
                </p>
            </div>
        </div>
        """

        text = extract_post_message_text(raw_html)

        self.assertIn("Rate image", text)
        self.assertIn('[SPOILER="Spoiler"]', text)
        self.assertIn("[IMG]https://i.example.test/one.jpg[/IMG]", text)
        self.assertIn("[IMG]https://i.example.test/two.jpg[/IMG]", text)
        self.assertIn("[/SPOILER]", text)

    def test_forum_smiles_are_kept_as_shortcuts_not_images(self) -> None:
        raw_html = """
        <p>Hello
        <img title=":lolpoppy:" src="https://dota2.ru/img/forum/emoticons/poppy.png"
             data-smile="1" data-shortcut=":lolpoppy:" alt="poppy.png" />
        </p>
        """

        text = extract_post_message_text(raw_html)

        self.assertIn(":lolpoppy:", text)
        self.assertNotIn("[IMG]https://dota2.ru/img/forum/emoticons/poppy.png[/IMG]", text)

    def test_page_tail_is_removed_after_moderation_controls_marker(self) -> None:
        raw_html = """
        <p>Actual starter text :PepeLove:</p>
        <p>Не игнорировать Игнорировать Жалоба Ответить Тема закрыта Отправить Кто смотрит тему</p>
        <script>Topic.id = 1634508;</script>
        """

        text = extract_post_message_text(raw_html)

        self.assertEqual("Actual starter text :PepeLove:", text)


class TopicMonitorMessageTests(unittest.TestCase):
    def test_topic_monitor_message_does_not_trim_content(self) -> None:
        long_content = "x" * 1600

        message = ForumSyncService._build_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Long topic",
                    "author_username": "Author",
                    "content_text": long_content,
                }
            ]
        )

        self.assertIn(long_content, message)
        self.assertNotIn("\u2026", message)

    def test_deleted_topic_monitor_message_does_not_trim_content(self) -> None:
        long_content = "y" * 1600

        message = ForumSyncService._build_deleted_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Deleted long topic",
                    "author_username": "Author",
                    "topic_url": "https://dota2.ru/forum/threads/deleted.1/",
                    "content_text": long_content,
                }
            ]
        )

        self.assertIn(long_content, message)
        self.assertNotIn("\u2026", message)

    def test_topic_monitor_message_renders_raw_html_when_available(self) -> None:
        message = ForumSyncService._build_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Picture topic",
                    "author_username": "Author",
                    "content_raw": """
                    <p>Rate image</p>
                    <div class="spoiler">
                        <div class="spoiler-container">
                            <span class="button-theme spoiler-btn"><abbr>Hidden</abbr></span>
                        </div>
                        <div class="spoiler-content">
                            <img data-src="https://i.example.test/picture.jpg" />
                        </div>
                    </div>
                    """,
                    "content_text": "Rate image Spoiler",
                }
            ]
        )

        self.assertIn('[SPOILER="Hidden"]', message)
        self.assertIn("[IMG]https://i.example.test/picture.jpg[/IMG]", message)

    def test_deleted_topic_monitor_message_formats_metadata(self) -> None:
        message = ForumSyncService._build_deleted_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Deleted formatted topic",
                    "author_user_id": 913643,
                    "author_username": "Zaloopiy",
                    "author_profile_url": "https://dota2.ru/forum/members/zaloopiy.913643/",
                    "topic_url": "https://dota2.ru/forum/threads/topic.1/",
                    "deletion_reason": "redirected_to:https://dota2.ru/forum/",
                    "content_text": "Full post text",
                }
            ]
        )

        self.assertIn(
            'Author: <a href="https://dota2.ru/forum/members/zaloopiy.913643/">Zaloopiy</a>;',
            message,
        )
        self.assertIn(
            '<img width="150" height="150" alt="913643.jpg" '
            'src="https://dota2.ru/img/forum/avatars/l/913/913643.jpg">',
            message,
        )
        self.assertIn('Original URL: <a href="https://dota2.ru/forum/threads/topic.1/">', message)
        self.assertIn("Reason: redirected_to:https://dota2.ru/forum/;", message)
        self.assertIn("----------------------------------------------<br>\nFull post text", message)

    def test_topic_monitor_message_cleans_stored_page_tail(self) -> None:
        message = ForumSyncService._build_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Broken stored topic",
                    "author_username": "Author",
                    "content_text": (
                        "Actual starter text Не игнорировать Игнорировать Жалоба "
                        "Ответить Тема закрыта Отправить $(document).ready(function() {})"
                    ),
                }
            ]
        )

        self.assertIn("Actual starter text", message)
        self.assertNotIn("Ответить", message)
        self.assertNotIn("$(document)", message)

    def test_deleted_topic_monitor_message_cleans_stored_page_tail(self) -> None:
        message = ForumSyncService._build_deleted_topic_monitor_message(
            [
                {
                    "forum_topic_id": 1,
                    "title": "Broken deleted topic",
                    "author_username": "Author",
                    "topic_url": "https://dota2.ru/forum/threads/deleted.1/",
                    "content_text": (
                        "Deleted starter text Не игнорировать Игнорировать Жалоба "
                        "Ответить Тема закрыта Отправить $(document).ready(function() {})"
                    ),
                }
            ]
        )

        self.assertIn("Deleted starter text", message)
        self.assertNotIn("Ответить", message)
        self.assertNotIn("$(document)", message)


if __name__ == "__main__":
    unittest.main()
