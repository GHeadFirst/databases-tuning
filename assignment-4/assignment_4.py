import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection
from drop_tables import drop_postgres
# Our connection and cursor for the postgresQL database


def create_table(conn, cursor, table_name, schema):
    try:
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        cursor.execute(sql)
        conn.commit() 
        print(f"✅(Success) Table {table_name} was created with the following schema:\n --> {schema}")
    except Exception as e:
        print(f"⚠️(Warning): {e}")
        conn.rollback()

def insert_data(conn, cursor, table_name, column_names, file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            cursor.copy_expert(f"Copy {table_name} ({column_names}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')",f)
        conn.commit()
        
        print(f"Successfully inserted data from {file_path} into {table_name} table \non columns {column_names}")
        
        print("✅ Inserted data successfully")
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} from data {file_path} on {column_names}\nException Error :{e}")
        conn.rollback()


def valid_index_type(type_of_index):
    if (
        type_of_index.strip() == "btree" or 
        type_of_index.strip() == "hash"
        ):
        return True
    return False

def table_exists(cursor, table_name):
    cursor.execute(f"""
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = %s
    );
""", (table_name,))

    result = cursor.fetchone()[0]
    return result

def index_exists(cursor, idx_name):
    cursor.execute("SELECT to_regclass(%s)", (f"public.{idx_name}",))
    exists = cursor.fetchone()[0] is not None
    return exists


# This function and valid index function needs refactoring
def create_index(conn, cursor, table_name, idx_name, type_of_index, column):
    if not table_exists(cursor, table_name):
        raise ValueError(f"Table {table_name} in {cursor} doesn't exist (create_index function)")

    if not valid_index_type(type_of_index):
        raise ValueError("Not a valid index type (create_index function)")

    # should probably check if column exists in table too
    try:
        if type_of_index == "btree":
            cursor.execute(f"CREATE INDEX {idx_name} ON {table_name}({column});")
        else:
            cursor.execute(f"CREATE INDEX {idx_name} USING {type_of_index} ON {table_name}({column});")

    except Exception as e:    
        raise ValueError(f"Invalid index type: {type_of_index}")
    
    # should probably test if index is actually created


def cluster_table(conn, cursor, table_name, idx_name):
    if not table_exists(cursor, table_name):
        raise ValueError(f"Table {table_name} in {cursor} doesn't exist (cluster_table function)"
)    
    # should test if idx_name exists in table
    if not index_exists(cursor, idx_name):
        raise ValueError(f"Index {idx_name} in Table {table_name} in {cursor} cursor does not exist (cluster_table fucntion)")
    cursor.execute(f"CLUSTER {table_name} USING {idx_name};")

    # should test if table actually got clustered

def drop_index(conn, cursor, idx_name):
    cursor.execute(f"DROP INDEX IF EXISTS {idx_name};")
    conn.commit()


def run_query_loop(conn, cursor, query_template, values):
    start = time.perf_counter()#

    for value in values:
    
        cursor.execute(query_template, (value,))
        
        cursor.fetchall()


    end = time.perf_counter()
    
    elapsed = end - start
    
    throughput = len(values) / elapsed 
    
    return throughput

def get_explain_analyze(conn, cursor, query_template, value):
    explain_query = "EXPLAIN ANALYZE " + query_template
    cursor.execute(explain_query, (value,))
    rows = cursor.fetchall()
    return "\n".join(row[0] for row in rows)  # one string with the full plan

def log_test_result(test_name, values_used, throughput, explain_output, filename="results.txt"):
    try:
        with open(filename, "a") as f:
            f.write(f"\n=== Test: {test_name} ===\n")
            f.write(f"Values tested: {len(values_used)}\n")
            f.write(f"Throughput: {throughput:.2f} queries/sec\n\n")
            f.write("Query Plan:\n")
            f.write(explain_output + "\n")
            f.write("---\n")
    except Exception as e:
        raise FileNotFoundError(
            f"File {filename} not found" 
            f"\nTest name: {test_name}" 
            f"\nValues used: {values_used}"
            f"\nExplain output: {explain_output}")

def sample_collector(cursor, table_name, column, limit=100):
    try:
        cursor.execute(
            f"SELECT DISTINCT {column} FROM {table_name} WHERE {column} IS NOT NULL LIMIT %s;",
            (limit,)
        )
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️ Error collecting samples from {column} in {table_name}: {e}")
        return []

def main():
    CONN, CURSOR = get_postgres_connection()

    # Edit the name of your tables and paths and column names here,
    # maybe I should just make into an array where 1 = name, 2 schema and so on
    AUTH_TABLE = "Auth"
    PUBL_TABLE =  "publ"

    # Schema of tables
    AUTH_SCHEMA = "name VARCHAR(49), pubID VARCHAR(129)"
    PUBL_SCHEMA = "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)"

    # Column names of tables
    PUBL_COLUMNS = "pubID, type, title, booktitle, year, publisher"
    
    # file path of data to be inserted into tables
    AUTH_FILE_PATH = "dblp/auth.tsv"
    PUBL_FILE_PATH = "dblp/publ.tsv"


    # First Index Setup Btree

    # We drop our tables
    drop_postgres()

    # We create our tables and insert our data
    create_table(CONN,CURSOR, PUBL_TABLE, PUBL_SCHEMA)
    
    insert_data(CONN,CURSOR,PUBL_TABLE, PUBL_COLUMNS,PUBL_FILE_PATH)


    # We create our index and cluster our table
    column_name_setup_btree = "pubID"


    create_index(CONN, CURSOR, PUBL_TABLE, "idx_pubid", "btree", column_name_setup_btree)
    cluster_table(CONN, CURSOR, PUBL_TABLE, "idx_pubid")

    print("✅ Created index successfully")
    # Gather sample test for our query loop and benchmark
    pubid_values = sample_collector(CURSOR, PUBL_TABLE, "pubID", limit=100)

    # Define our query template for Setup 1
    query_template = "SELECT * FROM Publ WHERE pubID = %s"

    # Running our tests and storing results
    throughput = run_query_loop(CONN, CURSOR, query_template, pubid_values)
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, pubid_values[0])

    log_test_result("Clustering B+ Tree - Point Query on pubID", pubid_values, throughput, plan_output)


    print(f"📈 Test complete - throughput: {throughput:.2f} queries/sec")

    # First Index Setup First Query Type Btree and Point Query

    
    # First Index Setup Second Query Type
    
    # First Index Setup Third Query Type
    
    # First Index Setup Fourth Query Type

    # Second Index Setup
    
    
    # Second Index Setup First Query Type

    # Second Index Setup Second Query Type

    # Second Index Setup Third Query Type

    # Second Index Setup Fourth Query Type


    # Third Index Setup

    # Third Index Setup First Query Type

    # Third Index Setup Second Query Type

    # Third Index Setup Third Query Type

    # Third Index Setup Fourth Query Type

    # Fourth Index Setup

    # Fourth Index Setup First Query Type

    # Fourth Index Setup Second Query Type

    # Fourth Index Setup Third Query Type

    # Fourth Index Setup Fourth Query Type



    

    

if __name__ == "__main__":
    main()