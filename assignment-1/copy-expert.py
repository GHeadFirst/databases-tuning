import os
import time
from db_connection import get_postgres_connection, get_mariadb_connection

def create_table(cursor, table_name, schema, conn):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        conn.commit()
        print(f" Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Warning: {e}")  
        conn.rollback()  

#  Base directory for cross-platform path handling
base_dir = os.path.dirname(os.path.abspath(__file__))

print("Running copy-expert")
#  File paths (compatible with both Windows & Linux)
auth_tsv_path = os.path.join(base_dir, "dblp", "auth.tsv")
publ_tsv_path = os.path.join(base_dir, "dblp", "publ.tsv")

#  PostgreSQL Setup
conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

#  Create Auth and Publ tables in PostgreSQL
create_table(cursor_postgres, "Auth", "name VARCHAR(49), pubID VARCHAR(149)", conn_postgres)
create_table(cursor_postgres, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)", conn_postgres)

# Bulk Insert for Auth Table (PostgreSQL)
start_time_postgres_auth = time.time()
try:
    with open(auth_tsv_path, "r", encoding="utf-8") as f:
        cursor_postgres.copy_expert("COPY Auth (name, pubID) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')", f)
    conn_postgres.commit()
    print(f" Successfully inserted data into Auth table (PostgreSQL).")
except Exception as e:
    print(f"⚠️ Error inserting into Auth (PostgreSQL): {e}")
    conn_postgres.rollback()
end_time_postgres_auth = time.time()

#  Bulk Insert for Publ Table (PostgreSQL)
start_time_postgres_publ = time.time()
try:
    with open(publ_tsv_path, "r", encoding="utf-8") as f:
        cursor_postgres.copy_expert("COPY Publ (pubID, type, title, booktitle, year, publisher) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')", f)
    conn_postgres.commit()
    print(f" Successfully inserted data into Publ table (PostgreSQL).")
except Exception as e:
    print(f"⚠️ Error inserting into Publ (PostgreSQL): {e}")
    conn_postgres.rollback()
end_time_postgres_publ = time.time()

#  Close PostgreSQL Connection
cursor_postgres.close()
conn_postgres.close()

#  MariaDB Setup
conn_mariadb = get_mariadb_connection()
cursor_mariadb = conn_mariadb.cursor()

#  Create Auth and Publ tables in MariaDB
create_table(cursor_mariadb, "Auth", "name VARCHAR(49), pubID VARCHAR(149)", conn_mariadb)
create_table(cursor_mariadb, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)", conn_mariadb)

#  Bulk Insert for Auth Table (MariaDB)
start_time_mariadb_auth = time.time()
try:
    with open(auth_tsv_path, "r", encoding="utf-8") as f:
        cursor_mariadb.copy_expert("LOAD DATA LOCAL INFILE '{}' INTO TABLE Auth FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'".format(auth_tsv_path))
    conn_mariadb.commit()
    print(f" Successfully inserted data into Auth table (MariaDB).")
except Exception as e:
    print(f"⚠️ Error inserting into Auth (MariaDB): {e}")
    conn_mariadb.rollback()
end_time_mariadb_auth = time.time()

#  Bulk Insert for Publ Table (MariaDB)
start_time_mariadb_publ = time.time()
try:
    with open(publ_tsv_path, "r", encoding="utf-8") as f:
        cursor_mariadb.copy_expert("LOAD DATA LOCAL INFILE '{}' INTO TABLE Publ FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'".format(publ_tsv_path))
    conn_mariadb.commit()
    print(f" Successfully inserted data into Publ table (MariaDB).")
except Exception as e:
    print(f"⚠️ Error inserting into Publ (MariaDB): {e}")
    conn_mariadb.rollback()
end_time_mariadb_publ = time.time()

#  Close MariaDB Connection
cursor_mariadb.close()
conn_mariadb.close()

#  Print Summary of Timings
elapsed_time_postgres_auth = end_time_postgres_auth - start_time_postgres_auth
elapsed_time_postgres_publ = end_time_postgres_publ - start_time_postgres_publ
elapsed_time_mariadb_auth = end_time_mariadb_auth - start_time_mariadb_auth
elapsed_time_mariadb_publ = end_time_mariadb_publ - start_time_mariadb_publ

print(f"Elapsed Time Postgres Auth Table: {elapsed_time_postgres_auth:.2f} seconds\n"
      f"Elapsed Time Postgres Publ Table: {elapsed_time_postgres_publ:.2f} seconds\n"
      f"Elapsed Time MariaDB Auth Table: {elapsed_time_mariadb_auth:.2f} seconds\n"
      f"Elapsed Time MariaDB Publ Table: {elapsed_time_mariadb_publ:.2f} seconds")
