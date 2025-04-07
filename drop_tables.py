import psycopg2
import mysql.connector
from db_connection import get_postgres_connection, get_mariadb_connection


def drop_postgres():
    """Drops (deletes) all tables in the public schema."""
    conn, cursor = get_postgres_connection()
    cursor.execute("""
        SELECT tablename FROM pg_tables WHERE schemaname = 'public';
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if tables:
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        conn.commit()
        print(f"✅ PostgreSQL: Dropped tables {tables}")
    else:
        print("⚠️ No tables found in PostgreSQL.")
    
    cursor.close()
    conn.close()

def drop_mariadb():
    """Drops (deletes) all tables in the current database."""
    conn, cursor = get_mariadb_connection()
    cursor.execute("""
        SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE();
    """)
    tables = [row[0] for row in cursor.fetchall()]

    if tables:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        for table in tables:
            cursor.execute(f"DROP TABLE IF EXISTS {table};")
        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()
        print(f"✅ MariaDB: Dropped tables {tables}")
    else:
        print("⚠️ No tables found in MariaDB.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    drop_postgres()
    drop_mariadb()
