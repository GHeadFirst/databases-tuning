
def get_db_type(conn):
    module = conn.__class__.__module__
    if 'psycopg2' in module:
        return 'PostgreSQL'
    elif 'mysql.connector' in module:
        return 'MariaDB/MySQL'
    else:
        return 'Unknown'

def drop_postgres(conn,cursor):
    """Drops (deletes) all tables in the public schema."""
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
    

def drop_mariadb(conn,cursor):
    """Drops (deletes) all tables in the current database."""
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
    

def clear_postgres(conn,cursor):

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

def clear_mariadb(conn,cursor):

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

def drop_all_indexes(conn, cursor):
    sql = """
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        FOR r IN (
            SELECT indexname
            FROM pg_indexes
            WHERE schemaname = 'public'
            AND tablename IN ('auth', 'publ')
            AND indexname NOT LIKE '%_pkey'
        ) LOOP
            EXECUTE 'DROP INDEX IF EXISTS ' || quote_ident(r.indexname);
        END LOOP;
    END$$;
    """
    cursor.execute(sql)
    conn.commit()



def drop_index(conn, cursor, idx_name):
    cursor.execute(f"DROP INDEX IF EXISTS {idx_name};")
    conn.commit()

def create_table(conn, cursor, table_name, schema):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        conn.commit()
        print(f" Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Warning: {e}")  
        conn.rollback()  


def copy_expert(conn, cursor, table_name, column_names, delimiter, file_path):
    if get_db_type(conn) != 'PostgreSQL':
        raise RuntimeError("Cannot use COPY expert on non-PostgreSQL databases.")

    try:
        # Escape backslashes for PostgreSQL (e.g., '\t' → '\\t')
        escaped_delim = delimiter.replace("\\", "\\\\")
        sql = (
            f"COPY {table_name} ({column_names}) "
            f"FROM STDIN WITH (FORMAT TEXT, DELIMITER E'{escaped_delim}')"
        )

        with open(file_path, "r", encoding="utf-8") as f:
            cursor.copy_expert(sql, f)

        conn.commit()
        cursor.execute(f"ANALYZE {table_name};")
        print(f"✅ Inserted data from {file_path} into '{table_name}' on columns {column_names}")
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} from {file_path} on {column_names}:\n{e}")
        conn.rollback()


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


def execute_query(conn,cursor, query):
    cursor.execute(query)
    conn.commit()

