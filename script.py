import psycopg2
import mysql.connector

# PostgreSQL Connection
try:
    postgres_conn = psycopg2.connect(
        dbname="mydb",
        user="user",
        password="password",
        host="postgres_db",
        port=5432
    )
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("SELECT version();")
    postgres_version = postgres_cursor.fetchone()
    print(f"✅ Connected to PostgreSQL: {postgres_version[0]}")
    postgres_cursor.close()
    postgres_conn.close()
except Exception as e:
    print(f"❌ PostgreSQL Connection Error: {e}")

# MariaDB Connection
try:
    mariadb_conn = mysql.connector.connect(
        host="mariadb_db",
        user="user",
        password="password",
        database="mydb",
        port=3306
    )
    mariadb_cursor = mariadb_conn.cursor()
    mariadb_cursor.execute("SELECT VERSION();")
    mariadb_version = mariadb_cursor.fetchone()
    print(f"✅ Connected to MariaDB: {mariadb_version[0]}")
    mariadb_cursor.close()
    mariadb_conn.close()
except Exception as e:
    print(f"❌ MariaDB Connection Error: {e}")
