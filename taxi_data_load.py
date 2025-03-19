import clickhouse_connect

"""
Этот датасет не подошел, т.к. отсутствует идентификатор такси taxi_id
"""
client = clickhouse_connect.get_client(host='localhost', port=8123, username='default', password='your_password')
try:
    create_table_query = """
    CREATE TABLE IF NOT EXISTS trips (
    trip_id             UInt32,
    pickup_datetime     DateTime,
    dropoff_datetime    DateTime,
    pickup_longitude    Nullable(Float64),
    pickup_latitude     Nullable(Float64),
    dropoff_longitude   Nullable(Float64),
    dropoff_latitude    Nullable(Float64),
    passenger_count     UInt8,
    trip_distance       Float32,
    fare_amount         Float32,
    extra               Float32,
    tip_amount          Float32,
    tolls_amount        Float32,
    total_amount        Float32,
    payment_type        Enum('CSH' = 1, 'CRE' = 2, 'NOC' = 3, 'DIS' = 4, 'UNK' = 5),
    pickup_ntaname      LowCardinality(String),
    dropoff_ntaname     LowCardinality(String)
    ) ENGINE = MergeTree PRIMARY KEY (pickup_datetime, dropoff_datetime)
    """
    client.command(create_table_query)
    insert_data_query = """
    INSERT INTO trips
    SELECT
        trip_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        tip_amount,
        tolls_amount,
        total_amount,
        payment_type,
        pickup_ntaname,
        dropoff_ntaname
    FROM gcs(
        'https://storage.googleapis.com/clickhouse-public-datasets/nyc-taxi/trips_{0..2}.gz',
        'TabSeparatedWithNames')
    """
    client.command(insert_data_query)
    print("Таблица успешно создана, данные загружены!")
except Exception as e:
    print(f'Операция завершилась с ошибкой:\n{e}')
