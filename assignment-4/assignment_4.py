import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection

# Our connection and cursor for the postgresQL database


def create_table(conn, cursor, table_name, schema):
    try:
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        cursor.execute(sql)
        conn.commit()  # ✅ Fixed typo: commt() → commit()
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
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} from data {file_path} on {column_names}")
        conn.rollback()


def valid_index_type(type_of_index):
    if (
        type_of_index.strip() == "b-tree" or 
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


def create_index(conn, cursor, table_name, idx_name, type_of_index, column):
    if not table_exists(cursor, table_name):
        raise ValueError(f"Table {table_name} in {cursor} doesn't exist (create_index function)")

    if not valid_index_type(type_of_index):
        raise ValueError("Not a valid index type (create_index function)")

    # should probably check if column exists in table too

    cursor.execute(f"CREATE INDEX {idx_name} USING {type_of_index} ON {table_name}({column});")
    
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

def get_explain_analyze(conn, cursor, query_template, value):


def main():
    CONN, CURSOR = get_postgres_connection()

    # Edit the name of your tables and paths and column names here,
    # maybe I should just make into an array where 1 = name, 2 schema and so on
    AUTH_TABLE = "Auth"
    PUBL_TABLE =  "Publ"

    # Schema of tables
    AUTH_SCHEMA = "name VARCHAR(49), pubID VARCHAR(129)"
    PUBL_SCHEMA = "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)"

    # Column names of tables

    
    # file path of data to be inserted into tables
    AUTH_FILE_PATH = "../dblp/auth.tsv"
    PUBL_FILE_PATH = "../dblp/publ.tsv"



    

    
    



if __name__ = "__main__":
    main()