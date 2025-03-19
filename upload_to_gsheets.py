import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from clickhouse_driver import Client
import pandas as pd

click_client = Client(
    host='localhost',
    user='default',
    password='your_password',
    database='default',
    settings={'use_numpy': True}
)
SERVICE_ACCOUNT_FILE = "key.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# connection
client = gspread.authorize(creds)
spreadsheet = client.open("table1")
# Открываем лист 0
worksheet = spreadsheet.get_worksheet(0)

query = 'SELECT * from default.marts_final_top_tips'
df = click_client.query_dataframe(query)
# Вставляем DataFrame в Google Sheets начиная с первой строки и первого столбца
set_with_dataframe(worksheet, df)
print("DataFrame успешно вставлен в Google Sheets!")