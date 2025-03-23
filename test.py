from db_connection import get_postgres_connection,get_mariadb_connection

conn_postgres = get_postgres_connection()
conn_mariadb = get_mariadb_connection()

cursor_postgres = conn_postgres.cursor()
cursor_mariadb = conn_mariadb.cursor()


print("---------------PostgreSQL version ---------------")
cursor_postgres.execute("SELECT version();")
print(cursor_postgres.fetchone())

print("---------------Mariadb Version---------------")
cursor_mariadb.execute("SELECT version();")
print(cursor_mariadb.fetchone())

cursor_postgres.close()
conn_postgres.close()

cursor_mariadb.close()
conn_mariadb.close()

