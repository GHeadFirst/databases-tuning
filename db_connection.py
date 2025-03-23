import psycopg2
import mysql.connector

def get_postgres_connection():
    """Returns a PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            dbname="mydb",
            user="user",
            password="password",
            host="postgres_db",  
            port=5432
        )
        return conn
    except Exception as e:
        print(f"❌ PostgreSQL Connection Error: {e}")
        return None

def get_mariadb_connection():
    """Returns a MariaDB connection."""
    try:
        conn = mysql.connector.connect(
            host="mariadb_db", 
            user="user",
            password="password",
            database="mydb",
            port=3306
        )
        return conn
    except Exception as e:
        print(f"❌ MariaDB Connection Error: {e}")
        return None
