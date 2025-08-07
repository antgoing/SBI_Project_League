import duckdb
import pandas as pd

con = duckdb.connect("baza.db")

#staging
df = con.execute("SELECT * FROM raw_data").fetchdf()
df['date'] = pd.to_datetime(df['date'], errors='coerce')
df['gamelength'] = pd.to_numeric(df['gamelength'], errors='coerce')
df['patch'] = df['patch'].astype(str)
df['teamname_cleaned'] = df['teamname'].str.strip().str.upper()

#wymiar czasu
df_dim_czas = df[['date']].dropna().drop_duplicates().copy()
df_dim_czas['rok'] = df_dim_czas['date'].dt.year
df_dim_czas['miesiac'] = df_dim_czas['date'].dt.month
df_dim_czas['kwartal'] = df_dim_czas['date'].dt.quarter
df_dim_czas['dzien'] = df_dim_czas['date'].dt.day
df_dim_czas['id_czasu'] = df_dim_czas.reset_index().index + 1
con.execute("CREATE OR REPLACE TABLE dim_czas AS SELECT * FROM df_dim_czas")

#wymiar zawodnika
df_dim_zawodnik = df[['playername', 'position']].copy()
df_dim_zawodnik['playername'] = df_dim_zawodnik['playername'].str.strip().str.lower()
df_dim_zawodnik = df_dim_zawodnik.drop_duplicates().dropna(subset=['playername'])
df_dim_zawodnik['id_zawodnik'] = df_dim_zawodnik.reset_index().index + 1
con.execute("CREATE OR REPLACE TABLE dim_zawodnik AS SELECT * FROM df_dim_zawodnik")

#wymiar postaci
df_dim_postac = df[['champion', 'patch']].copy()
df_dim_postac['champion'] = df_dim_postac['champion'].str.strip().str.title()
df_dim_postac['patch'] = df_dim_postac['patch'].astype(str).str.replace(r"[^0-9.]", "", regex=True)
df_dim_postac = df_dim_postac.drop_duplicates()
df_dim_postac['id_postaci'] = df_dim_postac.reset_index().index + 1
con.execute("CREATE OR REPLACE TABLE dim_postac AS SELECT * FROM df_dim_postac")

#wymiar meczu
df_dim_mecz = df[['gameid', 'gamelength', 'date']].drop_duplicates().copy()
df_dim_mecz['id_meczu'] = df_dim_mecz.reset_index().index + 1
con.execute("CREATE OR REPLACE TABLE dim_mecz AS SELECT * FROM df_dim_mecz")

#wymiar druzyny
df_dim_druzyna = df[['teamname_cleaned']].drop_duplicates().copy()
df_dim_druzyna = df_dim_druzyna.dropna(subset=['teamname_cleaned']) #usuwanie pustych nazw druzyn
df_dim_druzyna['id_druzyny'] = df_dim_druzyna.reset_index().index + 1
max_id_druzyny = df_dim_druzyna['id_druzyny'].max() if not df_dim_druzyna.empty else 0
unknown_team_df = pd.DataFrame([{'teamname_cleaned': 'UNKNOWN', 'id_druzyny': max_id_druzyny + 1}])
df_dim_druzyna = pd.concat([df_dim_druzyna, unknown_team_df], ignore_index=True)

con.execute("CREATE OR REPLACE TABLE dim_druzyna AS SELECT * FROM df_dim_druzyna")

#wymiar ligi
df_dim_liga = df[['league', 'split', 'playoffs', 'year']].drop_duplicates().copy()
df_dim_liga['league'] = df_dim_liga['league'].str.strip().str.upper()
df_dim_liga['split'] = df_dim_liga['split'].str.title()
df_dim_liga['playoffs'] = df_dim_liga['playoffs'].astype(bool)
df_dim_liga['id_sezon'] = df_dim_liga.reset_index().index + 1
con.execute("CREATE OR REPLACE TABLE dim_liga AS SELECT * FROM df_dim_liga")

#fakt
df['cs'] = df['total cs']
df['kda'] = (df['kills'] + df['assists']) / df['deaths'].replace(0, 1)
df['gpm'] = df['totalgold'] / (df['gamelength'] / 60)

#laczenie faktów z wymiarami
df = df.merge(df_dim_zawodnik, on=['playername', 'position']) 
df = df.merge(df_dim_postac, on=['champion', 'patch'], how='left')
df = df.merge(df_dim_mecz, on=['gameid', 'gamelength', 'date'], how='left')
df = df.merge(df_dim_czas, on='date', how='left')
df = df.merge(df_dim_druzyna, on='teamname_cleaned', how='left')
df = df.merge(df_dim_liga, on=['league', 'split', 'playoffs', 'year'], how='left')

#druzyny, ktore nie maja id_druzyny, dostaja id UNKNOWN
unknown_id = unknown_team_df['id_druzyny'].iloc[0] 
df['id_druzyny'] = df['id_druzyny'].fillna(unknown_id)
df['id_druzyny'] = df['id_druzyny'].astype(int) 

for col in ['id_zawodnik', 'id_postaci', 'id_meczu', 'id_czasu', 'id_druzyny', 'id_sezon']:
    if col in df.columns:
        df[col] = df[col].fillna(0).astype(int) #kazde id musi byc int

df_fakt = df[[
    'id_zawodnik', 'id_postaci', 'id_meczu', 'id_czasu',
    'id_druzyny', 'id_sezon', 'kda', 'cs', 'gpm'
]]

con.register("df_fakt", df_fakt)
con.execute("CREATE OR REPLACE TABLE fakt_udzial AS SELECT * FROM df_fakt")

print("transformacja danych zakonczona sukcesem")
print("liczba rekordów w fakt_udzial:", con.execute("SELECT COUNT(*) FROM fakt_udzial").fetchone()[0])

con.close()
