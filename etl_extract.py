import duckdb
import pandas as pd
import os
import re
from datetime import datetime

try:
    with open("last_loaded.txt", "r") as f:
        last_date = f.read().strip()
        print(f"ostatnia znana data: {last_date}")
except FileNotFoundError:
    last_date = "1900-01-01"
    print("brak pliku loaded, wiec ladujemy wszystkie dane")

last_date_dt = datetime.strptime(last_date, "%Y-%m-%d")

data_dir = "data/"
csv_files = [f for f in os.listdir(data_dir) if f.endswith("_data.csv")]

date_pattern = re.compile(r"(\d{2}\.\d{2}\.\d{4})_data\.csv")
candidates = []

for file in csv_files:
    match = date_pattern.search(file)
    if match:
        file_date = datetime.strptime(match.group(1), "%d.%m.%Y")
        if file_date > last_date_dt:
            candidates.append((file_date, file))

if not candidates:
    print("Brak nowszych plików do załadowania")
    exit()

newest_file = max(candidates)[1]
print(f"Wybrano plik: {newest_file}")

con = duckdb.connect("baza.db")

query = f"""
    SELECT * FROM read_csv_auto('{data_dir}{newest_file}', HEADER=TRUE)
    WHERE CAST(date AS DATE) > DATE '{last_date}'
"""
df_new = con.execute(query).fetchdf()
print(f"Znaleziono {len(df_new)} nowych rekordów")

con.execute(f"""
    CREATE TABLE IF NOT EXISTS raw_data AS
    SELECT * FROM read_csv_auto('{data_dir}{newest_file}', HEADER=TRUE) WHERE 1=0
""")

if not df_new.empty:
    con.execute("INSERT INTO raw_data SELECT * FROM df_new")
    print("Nowe dane dodane do raw_data")

    df_new['date'] = pd.to_datetime(df_new['date'], errors='coerce')
    max_date = df_new['date'].max().date()
    with open("last_loaded.txt", "w") as f:
        f.write(str(max_date))
    print(f"Zaktualizowano last_loaded.txt: {max_date}")
else:
    print("Brak nowych danych")

con.close()
