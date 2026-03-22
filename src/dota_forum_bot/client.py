from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from http.cookiejar import Cookie, CookieJar
from urllib.parse import urljoin
from urllib.request import HTTPCookieProcessor, Request, build_opener
from urllib.error import HTTPError, URLError

from .exceptions import AuthError, ForumBotError, MessageSendError


@dataclass
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str]
    text: str


@dataclass
class ParsedForm:
    action: str = ""
    inputs: dict[str, str] = field(default_factory=dict)
    textarea_name: str | None = None
    has_editor: bool = False
    has_submit: bool = False


class ReplyFormParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.forms: list[ParsedForm] = []
        self._current_form: ParsedForm | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        attr_map = dict(attrs)
        if tag == "form":
            self._current_form = ParsedForm(action=attr_map.get("action", ""))
            self.forms.append(self._current_form)
            return

        if self._current_form is None:
            return

        if tag == "textarea" and attr_map.get("name"):
            self._current_form.textarea_name = attr_map["name"]

        if tag == "input" and attr_map.get("name"):
            input_type = (attr_map.get("type") or "").lower()
            if input_type not in {"submit", "button", "file"}:
                self._current_form.inputs[attr_map["name"]] = attr_map.get("value", "")
            if input_type == "submit":
                self._current_form.has_submit = True

        if tag == "button":
            button_type = (attr_map.get("type") or "").lower()
            if button_type in {"", "submit"}:
                self._current_form.has_submit = True

        classes = attr_map.get("class", "")
        if "bbcode-editor" in classes.split():
            self._current_form.has_editor = True

    def handle_endtag(self, tag: str) -> None:
        if tag == "form":
            self._current_form = None


class Dota2ForumClient:
    def __init__(self, base_url: str, session_file: str = "session.json", timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.forum_base_url = urljoin(f"{self.base_url}/", "forum/")
        self.session_file = session_file
        self.timeout = timeout
        self.cookie_jar = CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))
        self.default_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/134.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
        }

    def ensure_authenticated(self, username: str, password: str, remember: bool = True) -> str:
        if self.load_session() and self.is_authenticated():
            return "restored"

        self.login(username, password, remember=remember)
        return "logged_in"

    def login(self, username: str, password: str, remember: bool = True) -> dict:
        if not username or not password:
            raise AuthError("Username and password must be set in the .env file.")

        login_page = self._request(urljoin(self.base_url, "/forum/login/"))

        payload = {
            "login": username,
            "password": password,
            "silent": False,
            "remember": remember,
            "referer": login_page.url,
        }

        response = self._request(
            urljoin(self.base_url, "/forum/api/user/auth"),
            method="POST",
            json_data=payload,
            headers={
                "Origin": self.base_url,
                "Referer": login_page.url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise AuthError(f"Login response is not valid JSON: {response.text[:300]}") from exc

        status = data.get("status")
        if status != "success":
            raise AuthError(f"Login failed with status={status!r}: {data}")

        if not self.is_authenticated():
            raise AuthError("Server reported successful login, but follow-up session check failed.")

        self.save_session()
        return data

    def is_authenticated(self) -> bool:
        response = self._request(self.forum_base_url)
        html = response.text

        matches = re.findall(r"function\s+isLogged\(\)\s*\{\s*return\s+'([01])'", html)
        if matches:
            return matches[-1] == "1"

        normalized = html.replace(" ", "").lower()
        if "utils.islogged=true" in normalized:
            return True
        if "utils.islogged=!0" in normalized:
            return True

        return False

    def fetch_page(self, url: str) -> HttpResponse:
        return self._request(url)

    def save_session(self) -> None:
        cookies = []
        for cookie in self.cookie_jar:
            cookies.append(
                {
                    "version": cookie.version,
                    "name": cookie.name,
                    "value": cookie.value,
                    "port": cookie.port,
                    "port_specified": cookie.port_specified,
                    "domain": cookie.domain,
                    "domain_specified": cookie.domain_specified,
                    "domain_initial_dot": cookie.domain_initial_dot,
                    "path": cookie.path,
                    "path_specified": cookie.path_specified,
                    "secure": cookie.secure,
                    "expires": cookie.expires,
                    "discard": cookie.discard,
                    "comment": cookie.comment,
                    "comment_url": cookie.comment_url,
                    "rest": dict(cookie._rest),
                    "rfc2109": cookie.rfc2109,
                }
            )

        with open(self.session_file, "w", encoding="utf-8") as file:
            json.dump({"cookies": cookies}, file, ensure_ascii=False, indent=2)

    def load_session(self) -> bool:
        if not os.path.exists(self.session_file):
            return False

        try:
            with open(self.session_file, "r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError):
            return False

        cookies = data.get("cookies")
        if not isinstance(cookies, list):
            return False

        self.cookie_jar = CookieJar()
        self.opener = build_opener(HTTPCookieProcessor(self.cookie_jar))

        for item in cookies:
            try:
                cookie = Cookie(
                    version=item.get("version", 0),
                    name=item["name"],
                    value=item["value"],
                    port=item.get("port"),
                    port_specified=item.get("port_specified", False),
                    domain=item["domain"],
                    domain_specified=item.get("domain_specified", False),
                    domain_initial_dot=item.get("domain_initial_dot", False),
                    path=item["path"],
                    path_specified=item.get("path_specified", True),
                    secure=item.get("secure", False),
                    expires=item.get("expires"),
                    discard=item.get("discard", False),
                    comment=item.get("comment"),
                    comment_url=item.get("comment_url"),
                    rest=item.get("rest", {}),
                    rfc2109=item.get("rfc2109", False),
                )
            except KeyError:
                continue

            self.cookie_jar.set_cookie(cookie)

        return len(list(self.cookie_jar)) > 0

    def send_message_to_thread(self, thread_url: str, message: str) -> str:
        if not message.strip():
            raise MessageSendError("Message text is empty.")

        page = self._request(thread_url)
        if page.status >= 400:
            raise MessageSendError(
                f"Failed to open thread page before replying. HTTP {page.status}: {page.text[:300]}"
            )
        self._ensure_thread_response(thread_url, page.url)

        form, endpoint = self._extract_reply_form(page.text, page.url)
        if form is None or endpoint is None:
            raise MessageSendError("Reply form was not found on the thread page.")

        conversation_id = self._extract_conversation_id(form.action)
        if conversation_id is not None:
            return self._send_conversation_message(page.url, conversation_id, message)

        topic_id = self._extract_topic_id(page.url, form.action)
        if topic_id is not None:
            return self._send_topic_reply(page.url, topic_id, message)

        payload = dict(form.inputs)
        message_field = self._resolve_message_field_name(form)
        payload[message_field] = message

        response = self._request(
            endpoint,
            method="POST",
            form_data=payload,
            headers={
                "Origin": self.base_url,
                "Referer": page.url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type:
            data = json.loads(response.text)
            status = data.get("status")
            if status not in {"success", "ok"}:
                raise MessageSendError(f"Forum rejected message: {data}")
            return json.dumps(data, ensure_ascii=False)

        if 200 <= response.status < 400 and message[:32] in response.text:
            return "Message text found in server response."

        if 200 <= response.status < 400 and response.url != page.url:
            return f"Message request completed with redirect to {response.url}"

        raise MessageSendError(
            f"Unable to confirm that the message was sent. HTTP {response.status}: {response.text[:300]}"
        )

    def _send_conversation_message(self, page_url: str, conversation_id: int, message: str) -> str:
        response = self._request(
            urljoin(self.base_url, "/forum/api/message/sendToConversation"),
            method="POST",
            json_data={"cid": conversation_id, "content": message},
            headers={
                "Origin": self.base_url,
                "Referer": page_url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise MessageSendError(
                f"Conversation send returned non-JSON response. HTTP {response.status}: {response.text[:300]}"
            ) from exc

        status = data.get("status")
        if status in {"success", "ok"}:
            return json.dumps(data, ensure_ascii=False)

        raise MessageSendError(f"Forum rejected conversation message: {data}")

    def _send_topic_reply(self, page_url: str, topic_id: int, message: str) -> str:
        response = self._request(
            urljoin(self.base_url, "/forum/api/forum/replyToTopic"),
            method="POST",
            json_data={"topic": topic_id, "content": message},
            headers={
                "Origin": self.base_url,
                "Referer": page_url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise MessageSendError(
                f"Topic reply returned non-JSON response. HTTP {response.status}: {response.text[:300]}"
            ) from exc

        status = data.get("status")
        if status in {"success", "ok", "moderation", "merged"}:
            return json.dumps(data, ensure_ascii=False)

        raise MessageSendError(f"Forum rejected topic reply: {data}")

    def create_topic(
        self,
        forum_id: int,
        title: str,
        content: str,
        subscribe: bool = True,
        prefix: int = -1,
        pinned: bool = False,
        poll_data: dict | None = None,
        referer_url: str | None = None,
    ) -> dict:
        if not title.strip():
            raise MessageSendError("Topic title is empty.")
        if not content.strip():
            raise MessageSendError("Topic content is empty.")

        response = self._request(
            urljoin(self.base_url, "/forum/api/forum/createForumTopic"),
            method="POST",
            json_data={
                "forum": forum_id,
                "title": title.strip(),
                "content": content.strip(),
                "pollData": poll_data or {"question": "", "variants": [], "multiple": False},
                "subscribe": 1 if subscribe else 0,
                "prefix": prefix,
                "pinned": pinned,
            },
            headers={
                "Origin": self.base_url,
                "Referer": referer_url or self.forum_base_url,
                "X-Requested-With": "XMLHttpRequest",
            },
        )

        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise MessageSendError(
                f"Topic creation returned non-JSON response. HTTP {response.status}: {response.text[:300]}"
            ) from exc

        status = data.get("status")
        if status in {"success", "moderation"}:
            return data
        raise MessageSendError(f"Forum rejected topic creation: {data}")

    def load_notifications(self) -> dict:
        response = self._request(
            urljoin(self.base_url, "/forum/api/notices/load"),
            method="POST",
            json_data={},
            headers={
                "Origin": self.base_url,
                "Referer": urljoin(self.base_url, "/forum/notifications/"),
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        return self._parse_json_response(response, "Notifications load")

    def preload_notifications(self, name: str, page: int = 1) -> dict:
        response = self._request(
            urljoin(self.base_url, "/forum/api/notices/preload"),
            method="POST",
            json_data={"name": name, "page": page},
            headers={
                "Origin": self.base_url,
                "Referer": urljoin(self.base_url, "/forum/notifications/"),
                "X-Requested-With": "XMLHttpRequest",
            },
        )
        return self._parse_json_response(response, "Notifications preload")

    def _extract_reply_form(self, html: str, page_url: str):
        parser = ReplyFormParser()
        parser.feed(html)

        for form in parser.forms:
            action = form.action.strip()

            has_textarea = form.textarea_name is not None
            has_editor = form.has_editor
            has_submit = form.has_submit
            if not (has_textarea or has_editor or has_submit):
                continue

            endpoint = page_url if action.lower().startswith("javascript:") else (urljoin(page_url, action) if action else page_url)
            return form, endpoint

        return None, None

    @staticmethod
    def _extract_thread_id_from_url(url: str) -> int | None:
        match = re.search(r"/forum/threads/[^/]+\.(\d+)(?:/|$|[?#])", url or "")
        if not match:
            return None
        return int(match.group(1))

    def _ensure_thread_response(self, requested_url: str, final_url: str) -> None:
        requested_topic_id = self._extract_thread_id_from_url(requested_url)
        if requested_topic_id is None:
            return

        final_topic_id = self._extract_thread_id_from_url(final_url)
        if final_topic_id != requested_topic_id:
            raise MessageSendError(
                f"Thread URL {requested_url} redirected to unexpected page {final_url}."
            )

    def _resolve_message_field_name(self, form: ParsedForm) -> str:
        if form.textarea_name:
            return form.textarea_name

        for candidate in ("message", "body", "text", "content", "post"):
            if candidate in form.inputs:
                return candidate

        return "message"

    @staticmethod
    def _extract_conversation_id(action: str) -> int | None:
        match = re.search(r"Conversation\.send\((\d+)\)", action or "")
        if not match:
            return None
        return int(match.group(1))

    @staticmethod
    def _extract_topic_id(page_url: str, action: str) -> int | None:
        match = re.search(r"Topic\.reply\((\d+)\)", action or "")
        if match:
            return int(match.group(1))

        match = re.search(r"/threads/[^/]+\.(\d+)/?$", page_url)
        if match:
            return int(match.group(1))

        return None

    def _request(
        self,
        url: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        json_data: dict | None = None,
        form_data: dict[str, str] | None = None,
    ) -> HttpResponse:
        body = None
        request_headers = dict(self.default_headers)
        if headers:
            request_headers.update(headers)

        if json_data is not None:
            body = json.dumps(json_data).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        elif form_data is not None:
            encoded = "&".join(
                f"{self._quote(key)}={self._quote(value)}" for key, value in form_data.items()
            )
            body = encoded.encode("utf-8")
            request_headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"

        request = Request(url=url, data=body, headers=request_headers, method=method)
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                raw = response.read()
                content_type = response.headers.get_content_charset() or "utf-8"
                text = raw.decode(content_type, errors="replace")
                return HttpResponse(
                    url=response.geturl(),
                    status=response.status,
                    headers=dict(response.headers.items()),
                    text=text,
                )
        except HTTPError as exc:
            raw = exc.read()
            content_type = exc.headers.get_content_charset() or "utf-8"
            text = raw.decode(content_type, errors="replace")
            return HttpResponse(
                url=exc.geturl(),
                status=exc.code,
                headers=dict(exc.headers.items()),
                text=text,
            )
        except URLError as exc:
            raise ForumBotError(f"Network request failed for {url}: {exc.reason}") from exc

    @staticmethod
    def _quote(value: str) -> str:
        from urllib.parse import quote_plus

        return quote_plus(str(value))

    @staticmethod
    def _parse_json_response(response: HttpResponse, label: str) -> dict:
        try:
            data = json.loads(response.text)
        except json.JSONDecodeError as exc:
            raise ForumBotError(
                f"{label} returned non-JSON response. HTTP {response.status}: {response.text[:300]}"
            ) from exc

        status = data.get("status")
        if status == "success":
            return data
        raise ForumBotError(f"{label} failed with status={status!r}: {data}")
