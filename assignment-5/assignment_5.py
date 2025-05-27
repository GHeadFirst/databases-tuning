from db_connection import get_mariadb_connection, get_postgres_connection
import time
import os

def get_db_type(conn):
    module = conn.__class__.__module__
    if 'psycopg2' in module:
        return 'PostgreSQL'
    elif 'mysql.connector' in module:
        return 'MariaDB/MySQL'
    else:
        return 'Unknown'
    
def create_table(conn, cursor, table_name, schema):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        conn.commit()
        print(f" Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Warning: {e}")  
        conn.rollback()  


def copy_expert(conn, cursor, table_name, column_names, delimiter, file_path):
    if (get_db_type(conn) != 'PostgreSQL' ):
      raise RuntimeError("Cannot use COPY expert on non-PostgreSQL databases.")
 
    try:
        with open(file_path, "r", encoding = "utf-8") as f:
            cursor.copy_expert(f"Copy {table_name} ({column_names}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E '{delimiter}')",f)
        conn.commit()
        cursor.execute(f"ANALYZE {table_name};")
        print(f"Successfully inserted data from {file_path} into {table_name} table \non columns {column_names}")
        
        print("✅ Inserted data successfully")
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} from data {file_path} on {column_names}\nException Error :{e}")
        conn.rollback()

def create_index(conn, cursor, table_name, idx_name, type_of_index, column):
    if not table_exists(cursor, table_name):
        raise ValueError(f"Table {table_name} in {cursor} doesn't exist (create_index function)")

    # should probably check if column exists in table too
    try:
        if type_of_index == "btree":
            cursor.execute(f"CREATE INDEX {idx_name} ON {table_name}({column});")
        elif type_of_index == "hash":
            cursor.execute(f"CREATE INDEX {idx_name} ON {table_name} USING hash ({column});")
        else:
            raise ValueError(f"Unsupported index type: {type_of_index}")
        conn.commit()
        cursor.execute("ANALYZE publ;")

    except Exception as e:    
        raise ValueError(f"Invalid index type: {type_of_index}. Exception error {e}")

    
    # should probably test if index is actually created


def cluster_table(conn, cursor, table_name, idx_name):
    if not table_exists(cursor, table_name):
        raise ValueError(f"Table {table_name} in {cursor} doesn't exist (cluster_table function)"
)    
    # should test if idx_name exists in table
    if not index_exists(cursor, idx_name):
        raise ValueError(f"Index {idx_name} in Table {table_name} in {cursor} cursor does not exist (cluster_table fucntion)")
    cursor.execute(f"CLUSTER {table_name} USING {idx_name};")

    cursor.execute(f"ANALYZE {table_name}")

    # should test if table actually got clustered


def sample_collector(cursor, table_name, column, limit=100):
    try:
        query = f"""
          SELECT {column} 
            FROM (
              SELECT DISTINCT {column}
                FROM {table_name}
               WHERE {column} IS NOT NULL
            ) AS sub
           ORDER BY random()
           LIMIT %s;
        """
        cursor.execute(query, (limit,))
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"⚠️ Error collecting samples from {column} in {table_name}: {e}")
        cursor.connection.rollback()      # clear the aborted state
        return []
    
