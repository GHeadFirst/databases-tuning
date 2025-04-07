from db_connection import get_postgres_connection,get_mariadb_connection

conn_postgres, cursor_postgres = get_postgres_connection()
conn_mariadb, cursor_mariadb = get_mariadb_connection()



print("---------------PostgreSQL version ---------------")
cursor_postgres.execute("SELECT version();")
print(cursor_postgres.fetchone())

print("---------------Mariadb Version---------------")
cursor_mariadb.execute("SELECT version();")
print(cursor_mariadb.fetchone())

""" print("---------------Assignment-1 Start---------------")


cursor_postgres.execute("SELECT COUNT(*) FROM Auth;")
row_count = cursor_postgres.fetchone()[0]  # Get the count result
cursor_postgres.execute("SELECT COUNT(*) FROM Publ;")
row_count_publ = cursor_postgres.fetchone()[0]  # Get the count result

print(f"✅ Total records in Auth table: {row_count}")
print(f"✅ Total records in Publ table: {row_count_publ}")

print("---------------Assignment-1 Finish---------------") """


print("---------------Assignment-2 Start---------------")
print("---------------PostgreSQL Testing Start---------------")
cursor_postgres.execute("SELECT COUNT(*) FROM Employee;")
row_count_employee_postgresql = cursor_postgres.fetchone()[0]  # Get the count result
cursor_postgres.execute("SELECT COUNT(*) FROM Student;")
row_count_student_postgresql = cursor_postgres.fetchone()[0]  # Get the count result
cursor_postgres.execute("SELECT COUNT(*) FROM Student;")
row_count_techdept_postgresql = cursor_postgres.fetchone()[0]  # Get the count result


print(f"✅ Total records in Employee table: {row_count_employee_postgresql}")
print(f"✅ Total records in Student table: {row_count_student_postgresql}")
print(f"✅ Total records in Techdept table: {row_count_techdept_postgresql}")

print("---------------PostgreSQL Testing Finish---------------")

print("---------------MariaDB Testing Start---------------")
cursor_mariadb.execute("SELECT COUNT(*) FROM Employee;")
row_count_employee_mariaDB = cursor_mariadb.fetchone()[0]  # Get the count result
cursor_mariadb.execute("SELECT COUNT(*) FROM Student;")
row_count_student_mariaDB = cursor_mariadb.fetchone()[0]  # Get the count result
cursor_mariadb.execute("SELECT COUNT(*) FROM Student;")
row_count_techdept_mariaDB = cursor_mariadb.fetchone()[0]  # Get the count result


print(f"✅ Total records in Employee table: {row_count_employee_mariaDB}")
print(f"✅ Total records in Student table: {row_count_student_mariaDB}")
print(f"✅ Total records in Techdept table: {row_count_techdept_mariaDB}")

print("---------------MariaDB Testing Finish---------------")
print("---------------Assignment-2 Finish---------------")


cursor_postgres.close()
conn_postgres.close()

cursor_mariadb.close()
conn_mariadb.close()

