import psycopg2
import mysql.connector
from db_connection import get_postgres_connection,get_mariadb_connection

conn_postgres, cursor_postgres = get_postgres_connection
conn_mariadb, cursor_mariadb = get_mariadb_connection


def create_table(cursor, table_name, schema, conn):
    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS %s (%s);",(table_name, schema))
        conn.commit()
        print(f"✅(Success) Tables was {table_name} was created with the following schema\n --> {schema}")
    except Exceptions as e:
        print(f"⚠️(Warning): {e}")
        conn.rollback()



