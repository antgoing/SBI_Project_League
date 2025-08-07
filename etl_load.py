import duckdb

con = duckdb.connect("baza.db")

tables = ['fakt_udzial', 'dim_zawodnik', 'dim_postac', 'dim_czas', 'dim_druzyna', 'dim_mecz', 'dim_liga']

for table in tables:
    con.execute(f"COPY {table} TO '{table}.csv' (HEADER, DELIMITER ',')")
