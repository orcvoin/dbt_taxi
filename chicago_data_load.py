import pandas as pd
from clickhouse_driver import Client
from tqdm.auto import tqdm

client = Client(
    host='localhost',
    user='default',
    password='your_password',
    database='default',
    settings={'use_numpy': True}
)
batch_size = 300000  # Размер чанков
rows = []
CSV_FILE = '/home/andy/my_files2/taxi.csv'
columns_to_keep = ['trip_id','taxi_id','trip_start_timestamp','trip_end_timestamp','trip_seconds','trip_miles',
                   'pickup_census_tract','dropoff_census_tract','pickup_community_area','dropoff_community_area',
                   'fare','tips','tolls','extras','trip_total','payment_type','company','pickup_centroid_latitude',
                   'pickup_centroid_longitude','dropoff_centroid_latitude','dropoff_centroid_longitude']

for chunk in tqdm(pd.read_csv(CSV_FILE, chunksize=batch_size, usecols=columns_to_keep)):
    client.insert_dataframe('INSERT INTO trips_chicago VALUES', chunk)