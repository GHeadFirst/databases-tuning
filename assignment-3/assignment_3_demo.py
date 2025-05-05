import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection

def create_table(cursor, table_name, schema, conn):
    try:
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        cursor.execute(sql)
        conn.commit()  # ✅ Fixed typo: commt() → commit()
        print(f"✅(Success) Table {table_name} was created with the following schema:\n --> {schema}")
    except Exception as e:
        print(f"⚠️(Warning): {e}")
        conn.rollback()

def insert_sample_data(cursor, conn, table_name):
    sample_data = [
        (100001, 'Alice Smith', 'HR', 50000),
        (100003, 'Charlie Lee', 'Engineering', 72000),
        (100010, 'Julia Fischer', 'HR', 53000),
        (100004, 'Diana Wang', 'Marketing', 60000),
        (100002, 'Bob Johnson', 'Engineering', 75000),
        (100006, 'Fiona Müller', 'Finance', 68000),
        (100007, 'George Brown', 'Engineering', 77000),
        (100005, 'Evan Patel', 'HR', 52000),
        (100009, 'Ivan Novak', 'Marketing', 61000),
        (100008, 'Hanna Meier', 'Finance', 66000)
    ]
    try:
        cursor.executemany(
            f"INSERT INTO {table_name} (ssnum, name, dept, salary) VALUES (%s, %s, %s, %s);",
            sample_data
        )
        conn.commit()
        print(f"✅ Inserted {len(sample_data)} rows into {table_name}")
    except Exception as e:
        print(f"⚠️(Insert Error): {e}")
        conn.rollback()

# --- MAIN EXECUTION ---
conn_postgres, cursor_postgres = get_postgres_connection()

table_employee_spec = "ssnum,name,dept,salary"
table_employee_name = "Employee"
table_employee_schema = "ssnum INT PRIMARY KEY, name VARCHAR(120) UNIQUE, dept VARCHAR(100), salary INT"

# Step 1: Create table
create_table(cursor_postgres, table_employee_name, table_employee_schema, conn_postgres)

# Step 2: Insert sample data
insert_sample_data(cursor_postgres, conn_postgres, table_employee_name)

# Step 3 & 4: Make the index and cluster
ssnum_index = "CREATE INDEX idx_ssnum ON Employee(ssnum);"
ssnum_cluster = "CLUSTER Employee USING idx_ssnum;"

name_index = "CREATE INDEX idx_name ON Employee(name);"
name_cluster = "CLUSTER Employee USING idx_name;"

# CREATE INDEX idx_ssnum ON Employee(ssnum);
# CLUSTER Employee USING idx_ssnum;
# CREATE INDEX idx_name ON Employee(name);
# CLUSTER Employee USING idx_name;

""" cursor_postgres.execute(ssnum_index) # Need to do SELECT * from Employee; before doing the cluster to show difference
cursor_postgres.execute(ssnum_cluster)
cursor_postgres.execute(name_index)
cursor_postgres.execute(name_cluster) """

