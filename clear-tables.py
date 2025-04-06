import psycopg2
import mysql.connector
from db_connection import get_postgres_connection, get_mariadb_connection

def clear_postgres():
    conn, cursor = get_postgres_connection()

    # Get existing tables
    cursor.execute("""
        SELECT tablename FROM pg_tables WHERE schemaname = 'public';
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if tables:
        cursor.execute(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;")
        conn.commit()
        print(f"✅ PostgreSQL: Cleared tables {tables}")
    else:
        print("⚠️ No tables found in PostgreSQL.")

    cursor.close()
    conn.close()

def clear_mariadb():
    conn, cursor = get_mariadb_connection()

    # Get existing tables
    cursor.execute("""
        SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if tables:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table};")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()
        print(f"✅ MariaDB: Cleared tables {tables}")
    else:
        print("⚠️ No tables found in MariaDB.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    clear_postgres()
    clear_mariadb()
