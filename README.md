# DotaForumBot

Первый этап проекта: авторизация бота на `dota2.ru/forum` через логин и пароль из `.env` и базовая проверка, что сессия авторизована.

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
Copy-Item .env.example .env
```

Заполни `.env` своими данными.

## Проверка авторизации

```powershell
python bot.py login-check
```

Команда:
- открывает страницу логина;
- получает служебный токен и cookies;
- отправляет логин на `/forum/api/user/auth`;
- проверяет, что сервер считает сессию авторизованной.

## Тест отправки сообщения

```powershell
python bot.py send-test
```

Для этого нужно дополнительно указать в `.env`:
- `DOTA2_FORUM_TEST_THREAD_URL`
- `DOTA2_FORUM_TEST_MESSAGE`

Отправка реализована как best-effort через разбор reply-формы страницы темы. Если форум изменит HTML или использует отдельный AJAX-endpoint на странице темы, код под это можно будет доработать на следующем этапе.
