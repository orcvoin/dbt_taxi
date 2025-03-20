### Проект - тестовое задание
* Знакомство с инструментом dbt

### Вывод результатов
* [Таблица с результатами docs.google.com](https://docs.google.com/spreadsheets/d/12ucCkK-_wGDcD-TaC_N31r2dtofAxk92z0yAA_2q0ds/edit?gid=0#gid=0)

### Особенности реализации:
* Для создания\хранения таблиц используется localhost бд clickhouse
* Датасет chicago taxi за 2018 год был загружен в бд скриптом через csv файл
* Версии dbt и адаптера: `pip install dbt-core==1.8.9 dbt-clickhouse==1.8.9`

### Для запуска:
* `run dbt`
* Выгрузка результатов в google spreadsheet `python3 upload_to_gsheets.py`