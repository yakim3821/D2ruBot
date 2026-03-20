from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from html.parser import HTMLParser
from http.cookiejar import CookieJar
from urllib.parse import urljoin
from urllib.request import HTTPCookieProcessor, Request, build_opener

from .exceptions import AuthError, MessageSendError


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
    def __init__(self, base_url: str, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.forum_base_url = urljoin(f"{self.base_url}/", "forum/")
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

    def send_message_to_thread(self, thread_url: str, message: str) -> str:
        if not message.strip():
            raise MessageSendError("Message text is empty.")

        page = self._request(thread_url)

        form, endpoint = self._extract_reply_form(page.text, page.url)
        if form is None or endpoint is None:
            raise MessageSendError("Reply form was not found on the thread page.")

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

        if response.ok and message[:32] in response.text:
            return "Message text found in server response."

        if response.ok and response.url != page.url:
            return f"Message request completed with redirect to {response.url}"

        raise MessageSendError(
            f"Unable to confirm that the message was sent. HTTP {response.status_code}: {response.text[:300]}"
        )

    def _extract_reply_form(self, html: str, page_url: str):
        parser = ReplyFormParser()
        parser.feed(html)

        for form in parser.forms:
            action = form.action.strip()
            if action.lower().startswith("javascript:"):
                continue

            has_textarea = form.textarea_name is not None
            has_editor = form.has_editor
            has_submit = form.has_submit
            if not (has_textarea or has_editor or has_submit):
                continue

            endpoint = urljoin(page_url, action) if action else page_url
            return form, endpoint

        return None, None

    def _resolve_message_field_name(self, form: ParsedForm) -> str:
        if form.textarea_name:
            return form.textarea_name

        for candidate in ("message", "body", "text", "content", "post"):
            if candidate in form.inputs:
                return candidate

        return "message"

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

    @staticmethod
    def _quote(value: str) -> str:
        from urllib.parse import quote_plus

        return quote_plus(str(value))
