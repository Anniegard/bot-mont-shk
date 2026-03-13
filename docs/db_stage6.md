# SQLite Stage 6

## Что добавлено

- Read-only search layer для администраторов прямо в Telegram.
- Универсальный поиск кейсов по `case_id`, `ШК`, `тара/передача`, `наименование`.
- Просмотр raw-строк, уже связанных с конкретным `case_id`.
- Поиск raw-строк по `ШК`, `тара/передача`, `наименование`.
- Отдельный service layer в `bot/services/search_service.py` без изменения existing sync/review flow.

## Новые Telegram-команды

- `/case <query>`
  - сначала ищет точный `case_id`
  - затем `ШК`
  - затем `тара/передача`
  - затем `наименование`
- `/case_raw <case_id>`
  - показывает связанные raw-строки для кейса
- `/raw_find <query>`
  - ищет raw-строки по `ШК`, `тара/передача`, `наименование`
- `/case_help`
  - краткая help-команда по новому search layer

Все команды используют тот же admin access gate, что и stage 5.

## Как работает поиск

Поиск намеренно детерминированный:

1. exact match
2. normalized exact match
3. для `наименование` дополнительно safe partial match

Нормализация:

- trim внешних пробелов
- collapse repeated spaces
- case-insensitive compare для normalized search
- без fuzzy matching и без сторонних библиотек

Приоритет поиска не смешивается:

- если query найден по более сильному полю, более слабые поля уже не используются
- это сохраняет предсказуемость ответа для оператора

## Вывод в Telegram

- `/case` при одном strong result показывает компактную карточку кейса
- `/case` при нескольких результатах показывает короткий список
- `/case_raw` показывает короткий linked raw list
- `/raw_find` возвращает короткий список raw-строк
- длинные выборки режутся лимитом, в сообщении явно пишется, что ответ сокращён

## Что search layer не делает

- не меняет `cases`
- не меняет Google Sheets
- не создаёт новые кейсы
- не делает fuzzy search
- не вмешивается в stage 3 safe matching или stage 5 review actions

Google Sheets по-прежнему остаётся master source для case fields.

## Service helpers

- `get_case_by_case_id(case_id)`
- `find_cases_by_shk(shk, limit=10)`
- `find_cases_by_tare_transfer(tare_transfer, limit=10)`
- `find_cases_by_item_name(item_name, limit=10)`
- `search_cases(query, limit=10)`
- `find_raw_rows_by_shk(shk, limit=10)`
- `find_raw_rows_by_tare_transfer(tare_transfer, limit=10)`
- `find_raw_rows_by_item_name(item_name, limit=10)`
- `search_raw_rows(query, limit=10)`
- `get_raw_rows_for_case(case_id, limit=20)`

## Тесты stage 6

Добавлены focused regression tests на:

- exact case lookup по `case_id`
- приоритет case search (`ШК` раньше `тары/передачи`)
- linked raw rows для кейса
- raw search по `ШК` и `таре/передаче`

Тесты остаются локальными:

- temp SQLite
- без живого Telegram API
- без Google Sheets
- без Yandex Disk
