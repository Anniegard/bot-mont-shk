Эта папка используется для временных и кэшированных данных бота (runtime), которые не коммитятся.

Автоматически появляются:
- last_24h_snapshot.json / last_24h_meta.json — snapshot файла «24 часа».
- last_no_move_map.json / last_no_move_meta.json — карта идентификатор → гофра из «Без движения».

В репозитории храним:
- block_ids.txt — whitelist ID Блока.
- block_ids.txt.example — пример.

Все другие last_*.json/.pkl, архивы и временные файлы должны игнорироваться git.
