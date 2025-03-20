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
* В корневой папке должен лежать валидный сервисный ключ с неймингом `key.json` 

### Конфиг
* зависимости в `requirements.txt`
* show create сырой таблицы:

`CREATE TABLE default.trips_chicago ( `trip_id` String, `taxi_id` String, `trip_start_timestamp` DateTime, `trip_end_timestamp` DateTime, `trip_seconds` UInt32, `trip_miles` Float32, `pickup_census_tract` Nullable(UInt64), `dropoff_census_tract` Nullable(UInt64), `pickup_community_area` Nullable(UInt8), `dropoff_community_area` Nullable(UInt8), `fare` Float32, `tips` Float32, `tolls` Float32, `extras` Float32, `trip_total` Float32, `payment_type` String, `company` String, `pickup_centroid_latitude` Nullable(Float32), `pickup_centroid_longitude` Nullable(Float32), `dropoff_centroid_latitude` Nullable(Float32), `dropoff_centroid_longitude` Nullable(Float32) ) ENGINE = MergeTree ORDER BY trip_id SETTINGS index_granularity = 8192`
