# DotaForumBot

Первый этап проекта: авторизация бота на `dota2.ru/forum` через логин и пароль из `.env` и базовая проверка, что сессия авторизована.

Бот сохраняет cookies в `session.json`, автоматически загружает их при старте и делает повторный логин только если сохраненная сессия уже протухла.

## PostgreSQL в Docker

В проект добавлен локальный PostgreSQL через Docker Compose.

Запуск:

```powershell
.\scripts\db-up.ps1
```

Остановка:

```powershell
.\scripts\db-down.ps1
```

Полный сброс тома БД и повторная инициализация схемы:

```powershell
.\scripts\db-reset.ps1
```

Схема таблиц создается автоматически из:

`docker/postgres/init/001_schema.sql`

## Setup

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Заполни `.env` своими данными.

## Проверка авторизации

```powershell
python bot.py login-check
```

## Команды работы с данными форума

Сканирование раздела `Таверна` и сохранение тем в PostgreSQL:

```powershell
python bot.py scan-taverna
```

Синхронизация одной темы и ее стартового поста:

```powershell
python bot.py sync-topic https://dota2.ru/forum/threads/fate-grand-order-thread.1340497/
```

Просмотр тем, где бот еще не отвечал:

```powershell
python bot.py list-new-topics
```

Отправка черновиков новых тем в безопасную тестовую переписку:

```powershell
python bot.py draft-new-topics --limit 3
```

Публикация уже подготовленных черновиков в реальные темы:

```powershell
python bot.py publish-drafted-topics --limit 1
```

Сбор постов `Yakim38` для будущего профиля личности:

```powershell
python bot.py sync-yakim-posts --pages 3
```

Построение базового style profile для `Yakim38`:

```powershell
python bot.py build-yakim-profile --limit 200
```

Команда:
- сначала пытается загрузить `session.json`;
- если сессия еще жива, использует ее без повторного логина;
- если сессия протухла, логинится заново;
- после успешного входа обновляет `session.json`.

## Тест отправки сообщения

```powershell
python bot.py send-test
```

Для этого нужно дополнительно указать в `.env`:
- `DOTA2_FORUM_TEST_THREAD_URL`
- `DOTA2_FORUM_TEST_MESSAGE`

Отправка реализована как best-effort через разбор reply-формы страницы темы. Если форум изменит HTML или использует отдельный AJAX-endpoint на странице темы, код под это можно будет доработать на следующем этапе.
