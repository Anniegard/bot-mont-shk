# SQLite Stage 5

## Что добавлено

- Operational review layer для unresolved raw-строк из `raw_yadisk_rows`.
- Admin-only Telegram-команды для очереди разбора, просмотра строки, кандидатов, ручной привязки, явного unlink, ignore и возврата в pending.
- Audit trail в `raw_review_actions`.

## Изменения в БД

В `raw_yadisk_rows` stage 5 мягко добавляет поля через `PRAGMA table_info(...)` + `ALTER TABLE ADD COLUMN`:

- `review_status TEXT NOT NULL DEFAULT 'pending'`
- `review_note TEXT`
- `reviewed_at TEXT`
- `reviewed_by TEXT`
- `manual_linked_at TEXT`

Новая таблица:

- `raw_review_actions`
  - `id INTEGER PRIMARY KEY AUTOINCREMENT`
  - `raw_row_id INTEGER NOT NULL`
  - `action TEXT NOT NULL`
  - `previous_case_id TEXT`
  - `new_case_id TEXT`
  - `actor_id TEXT`
  - `note TEXT`
  - `created_at TEXT NOT NULL`

Индексы:

- `idx_raw_yadisk_rows_review_status`
- существующий `idx_raw_yadisk_rows_matched_case_id` продолжает использоваться
- `idx_raw_review_actions_raw_row_id`

## Review flow

1. Stage 3 ingest продолжает писать raw-строки и safe matching результат в `raw_yadisk_rows`.
2. Все новые строки стартуют с `review_status='pending'`.
3. Admin в Telegram смотрит очередь unresolved строк через `/raw_queue`.
4. Для конкретной строки можно:
   - посмотреть детали `/raw_show <raw_id>`
   - посмотреть deterministic candidates `/raw_candidates <raw_id>`
   - вручную привязать к существующему `case_id` через `/raw_link <raw_id> <case_id>`
   - явно снять связь с кейсом через `/raw_unlink <raw_id> [note]`
   - пометить как ignored `/raw_ignore <raw_id> [note]`
   - вернуть обратно в pending `/raw_pending <raw_id> [note]`
5. Каждое ручное действие пишет запись в `raw_review_actions`.

## Семантика review-статусов

- `pending` — строка снова находится в очереди на разбор.
- `ignored` — оператор решил временно не разбирать строку.
- `linked` — строка вручную привязана к существующему `case_id`.

`/raw_pending` теперь только переоткрывает строку для review и не снимает существующую связь.

`/raw_unlink` — отдельное явное действие для снятия `matched_case_id`. Оно:

- очищает только review/link-поля raw-строки;
- возвращает строку в `pending`;
- пишет audit-запись `manual_unlink` в `raw_review_actions`;
- не изменяет `cases`, `case_versions` и данные кейсов из Google Sheets.

## Как работает manual link

- Manual link проверяет, что raw-строка существует.
- Проверяет, что целевой `case_id` уже существует в `cases`.
- Обновляет только связь raw-строки с уже существующим кейсом и review-метаданные.
- Поля бизнес-данных кейса из Google Sheets не меняются.
- Новые кейсы из raw-строк не создаются.

## Доступ

- Команды review доступны только admin IDs из `BOT_ADMIN_IDS`.
- Для совместимости поддерживается и старый `ADMIN_USER_ID`.
- Если admin IDs не настроены, команды возвращают понятное сообщение о том, что админ-доступ не сконфигурирован.

## Что намеренно не реализовано

- Нет fuzzy matching.
- Нет авто-создания новых кейсов из raw-строк.
- Нет записи manual review обратно в поля кейса Google Sheets.
- Нет удаления raw-строк из хранилища.
