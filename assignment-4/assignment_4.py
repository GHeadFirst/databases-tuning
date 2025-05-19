from os.path import isfile
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection
from drop_tables import drop_postgres
# Our connection and cursor for the postgresQL database


# A single source of truth for every test’s human-readable label
MODE_LABELS = {
    'clustered_btree_point_pubid':    "Clustered B+ Tree - Point Query on pubID",
    'clustered_btree_multipoint_booktitle': "Clustered B+ Tree - Multipoint Query on booktitle",
    'clustered_btree_in_pubid':       "Clustered B+ Tree - Multipoint Query with IN on pubID (3 values)",
    'clustered_btree_point_year':     "Clustered B+ Tree - Point Query on year",

    'nonclustered_btree_point_pubid':    "Non-Clustered B+ Tree - Point Query on pubID",
    'nonclustered_btree_multipoint_booktitle': "Non-Clustered B+ Tree - Multipoint Query on booktitle",
    'nonclustered_btree_in_pubid':       "Non-Clustered B+ Tree - Multipoint Query with IN on pubID (3 values)",
    'nonclustered_btree_point_year':     "Non-Clustered B+ Tree - Point Query on year",

    'nonclustered_hash_point_pubid':    "Non-Clustered Hash - Point Query on pubID",
    'nonclustered_hash_multipoint_booktitle': "Non-Clustered Hash - Multipoint Query on booktitle",
    'nonclustered_hash_in_pubid':       "Non-Clustered Hash - Multipoint Query with IN on pubID (3 values)",
    'nonclustered_hash_point_year':     "Non-Clustered Hash - Point Query on year",

    'table_scan_point_pubid':          "Table Scan - Point Query on pubID",
    'table_scan_multipoint_booktitle': "Table Scan - Multipoint Query on booktitle",
    'table_scan_in_pubid':             "Table Scan - Multipoint Query with IN on pubID (3 values)",
    'table_scan_point_year':           "Table Scan - Point Query on year",
}



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
        cursor.execute("ANALYZE publ;")
        
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
        raise ValueError("Not a valid index type:{type_of_index} Exception error: {e} (create_index function)")

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

def drop_index(conn, cursor, idx_name):
    cursor.execute(f"DROP INDEX IF EXISTS {idx_name};")
    conn.commit()


def run_query_loop(conn, cursor, query_template, values):
    
    start = time.perf_counter()
    total_queries = 0

    # Loop until we've spent at least min_duration seconds
    while True:
        for v in values:
            cursor.execute(query_template, (v,))
            _ = cursor.fetchall()
            total_queries += 1

        elapsed = time.perf_counter() - start
        if elapsed >= 60:
            break

    throughput = total_queries / elapsed
    return throughput, elapsed

def get_explain_analyze(conn, cursor, query_template, value):
    explain_query = "EXPLAIN ANALYZE " + query_template

    # Special case: if value is a list/tuple and query contains IN
    if isinstance(value, (list, tuple)) and "IN %s" in query_template:
        # Build a properly formatted IN clause manually
        value_list = "', '".join(str(v) for v in value)
        expanded_query = explain_query.replace("%s", f"('{value_list}')")
        cursor.execute(expanded_query)
    else:
        cursor.execute(explain_query, (value,))
    
    rows = cursor.fetchall()
    return "\n".join(row[0] for row in rows)

def log_test_result(test_name, values_used, throughput, elapsed, explain_output, filename="results.txt"):
    """
    Append to results.txt:
      - === Test: …
      - Values (first 10): … (+N more)
      - Throughput: XX.XX queries/sec
      - Total time elapsed: YY.YY sec
      - Query Plan:
      - (the EXPLAIN ANALYZE text)
    """
    try:
        with open(filename, "a") as f:
            f.write(f"\n=== Test: {test_name} ===\n")
            first10 = values_used[:10]
            f.write("Values (first 10): " + ", ".join(map(str, first10))
                    + f"; …(+{len(values_used)-10} more)\n")
            f.write(f"Throughput: {throughput:.2f} queries/sec\n")
            f.write(f"Total time elapsed: {elapsed:.2f} sec\n\n")
            f.write("Query Plan:\n")
            f.write(explain_output + "\n")
            f.write("-" * 40 + "\n")
    except Exception as e:
        raise FileNotFoundError(f"…") from e


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

    # Delete results.txt if it exists (This is done to reset our test results)
    if os.path.isfile("results.txt"):
        open('results.txt', 'w').close()


    # We drop our tables
    drop_postgres()

    # We create our tables and insert our data
    create_table(CONN,CURSOR, PUBL_TABLE, PUBL_SCHEMA)
    
    insert_data(CONN,CURSOR,PUBL_TABLE, PUBL_COLUMNS,PUBL_FILE_PATH)

    # Data sampling and random data creation
    print("----------------------------------Creating random Data to benchmark----------------------------------")

    pubid_values = sample_collector(CURSOR, PUBL_TABLE, "pubID", limit=100)
    booktitle_values = sample_collector(CURSOR, PUBL_TABLE, "booktitle", limit=100)
    grouped_values = [pubid_values[i:i+3] for i in range(0, len(pubid_values), 3) if len(pubid_values[i:i+3]) == 3]
    year_values = sample_collector(CURSOR, PUBL_TABLE, "year", limit=100)

    print("---------------------------------Finished creating random data to benchmark--------------------------------")


    # First Index Setup Btree
    print("==================================FIRST INDEX==================================")


    # We create our index and cluster our table
    column_name_setup_btree = "pubID"

    
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_pubid", "btree", column_name_setup_btree)
    cluster_table(CONN, CURSOR, PUBL_TABLE, "idx_pubid")

    print("✅ Created index successfully")

    # Define our query template for Setup 1
    query_template = "SELECT * FROM Publ WHERE pubID = %s"

    # Running our tests and storing results
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, pubid_values)
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, pubid_values[0])

    log_test_result(MODE_LABELS['clustered_btree_point_pubid'], pubid_values, throughput, time_elapsed,plan_output)


    print(f"📈 Test complete - throughput: {throughput:.2f} queries/sec")


    # First Index Setup Second Query Type
    drop_index(CONN, CURSOR, "idx_booktitle")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_booktitle", "btree", "booktitle")
    cluster_table(CONN, CURSOR, PUBL_TABLE, "idx_booktitle")
    
    print("B+Tree index and clustered table on booktitle created !")


    query_template_booktitle = "SELECT * FROM Publ WHERE booktitle = %s"

    throughput_booktitle, time_elapsed= run_query_loop(CONN, CURSOR, query_template_booktitle, booktitle_values)
    plan_output_booktitle = get_explain_analyze(CONN, CURSOR, query_template_booktitle, booktitle_values[0])

    log_test_result(MODE_LABELS['clustered_btree_multipoint_booktitle'], booktitle_values, throughput_booktitle, time_elapsed, plan_output_booktitle)

    print(f"Test complete (booktitle) - throughput: {throughput_booktitle:.2f} queries/sec")


    # First Index Setup Third Query Type
    drop_index(CONN, CURSOR, "idx_pubid")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_pubid", "btree", "pubID")
    cluster_table(CONN, CURSOR, PUBL_TABLE, "idx_pubid")
    print("B+Tree index and clustered table on pubID created (for IN-query)!")



    query_template_IN = "SELECT * FROM Publ WHERE pubID IN %s"

    def run_in_query_loop(conn, cursor, query_template, list_of_value_groups):
        start = time.perf_counter()
        for value_group in list_of_value_groups:
            cursor.execute(query_template, (tuple(value_group),))
            cursor.fetchall()
        end = time.perf_counter()
        elapsed = end - start
        throughput = len(list_of_value_groups) / elapsed
        return throughput, elapsed
    
    throughput_in, time_elapsed = run_in_query_loop(CONN, CURSOR, query_template_IN, grouped_values)
    plan_output_in = get_explain_analyze(CONN, CURSOR, query_template_IN, grouped_values[0])

    log_test_result(MODE_LABELS['clustered_btree_in_pubid'], grouped_values, throughput_in,time_elapsed ,plan_output_in)
    print(f"📈 Test complete (pubID IN) - throughput: {throughput_in:.2f} queries/sec")

    # First Index Setup Fourth Query Type
    drop_index(CONN, CURSOR, "idx_year")
    create_index (CONN, CURSOR, PUBL_TABLE, "idx_year", "btree", "year")
    cluster_table(CONN, CURSOR, PUBL_TABLE, "idx_year")
    print("B+Tree index and clustered table on year created !")


    query_template = "SELECT * FROM Publ WHERE year = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, year_values)


    plan_output = get_explain_analyze(CONN, CURSOR, query_template, year_values[0])

    log_test_result(MODE_LABELS["clustered_btree_point_year"], year_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")

    # Second Index Setup

    print("==================================SECOND INDEX==================================")

    
    # Second Index Setup First Query Type
    drop_index(CONN, CURSOR, "idx_pubid")
    create_index (CONN, CURSOR, PUBL_TABLE, "idx_pubid", "btree", "pubID")

    print("B+Tree index and non-clustered table on PubID created !")


    query_template = "SELECT * FROM Publ WHERE pubID = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, pubid_values)

    
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, pubid_values[0])

    log_test_result(MODE_LABELS['nonclustered_btree_point_pubid'], pubid_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")

    # Second Index Setup Second Query Type
    drop_index(CONN, CURSOR, "idx_booktitle")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_booktitle", "btree", "booktitle")
    
    print("B+Tree index and non-clustered table on booktitle created !")


    query_template_booktitle = "SELECT * FROM Publ WHERE booktitle = %s"

    throughput_booktitle, time_elapsed = run_query_loop(CONN, CURSOR, query_template_booktitle, booktitle_values)
    plan_output_booktitle = get_explain_analyze(CONN, CURSOR, query_template_booktitle, booktitle_values[0])

    log_test_result(MODE_LABELS['nonclustered_btree_multipoint_booktitle'], booktitle_values, throughput_booktitle, time_elapsed,plan_output_booktitle)

    print(f"Test complete (booktitle) - throughput: {throughput_booktitle:.2f} queries/sec")

    # Second Index Setup Third Query Type
    drop_index(CONN, CURSOR, "idx_pubid")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_pubid", "btree", "pubID")
    print("B+Tree index and non-clustered table on pubID created (for IN-query)!")



    query_template_IN = "SELECT * FROM Publ WHERE pubID IN %s"

    #def run_in_query_loop(conn, cursor, query_template, list_of_value_groups):
    #    start = time.perf_counter()
    #    for value_group in list_of_value_groups:
    #        cursor.execute(query_template, (tuple(value_group),))
    #        cursor.fetchall()
    #    end = time.perf_counter()
    #    elapsed = end - start
    #    throughput = len(list_of_value_groups) / elapsed
    #    return throughput 
    
    throughput_in, time_elapsed = run_in_query_loop(CONN, CURSOR, query_template_IN, grouped_values)
    plan_output_in = get_explain_analyze(CONN, CURSOR, query_template_IN, grouped_values[0])


    log_test_result(MODE_LABELS['nonclustered_btree_in_pubid'], grouped_values, throughput_in, time_elapsed,plan_output_in)
    print(f" Test complete (pubID IN) - throughput: {throughput_in:.2f} queries/sec")

    # Second Index Setup Fourth Query Type
    drop_index(CONN, CURSOR, "idx_year")
    create_index (CONN, CURSOR, PUBL_TABLE, "idx_year", "btree", "year")
    print("B+Tree index and non-clustered table on year created !")


    query_template = "SELECT * FROM Publ WHERE year = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, year_values)

    
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, year_values[0])

    log_test_result(MODE_LABELS['nonclustered_btree_point_year'], year_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")

    # Third Index Setup
    print("==================================THIRD INDEX==================================")
    
    # Third Index Setup First Query Type
    drop_index(CONN, CURSOR, "idx_pubid")
    create_index (CONN, CURSOR, PUBL_TABLE, "idx_pubid", "hash", "pubID")

    print("Hash index and non-clustered table on PubID created !")



    query_template = "SELECT * FROM Publ WHERE pubID = %s"
    throughput, time_elapsed= run_query_loop(CONN, CURSOR, query_template, pubid_values)

    
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, pubid_values[0])

    log_test_result(MODE_LABELS['nonclustered_hash_point_pubid'], pubid_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")



    # Third Index Setup Second Query Type
    drop_index(CONN, CURSOR, "idx_booktitle")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_booktitle", "hash", "booktitle")
    
    print("Hash index and non-clustered table on booktitle created !")


    query_template_booktitle = "SELECT * FROM Publ WHERE booktitle = %s"

    throughput_booktitle , time_elapsed= run_query_loop(CONN, CURSOR, query_template_booktitle, booktitle_values)
    plan_output_booktitle = get_explain_analyze(CONN, CURSOR, query_template_booktitle, booktitle_values[0])

    log_test_result(MODE_LABELS["nonclustered_hash_multipoint_booktitle"], booktitle_values, throughput_booktitle, time_elapsed,plan_output_booktitle)

    print(f"Test complete (booktitle) - throughput: {throughput_booktitle:.2f} queries/sec")


    # Third Index Setup Third Query Type
    drop_index(CONN, CURSOR, "idx_pubid")
    create_index(CONN, CURSOR, PUBL_TABLE, "idx_pubid", "hash", "pubID")
    print("Hash index and non-clustered table on pubID created (for IN-query)!")



    query_template_IN = "SELECT * FROM Publ WHERE pubID IN %s"

    #def run_in_query_loop(conn, cursor, query_template, list_of_value_groups):
    #    start = time.perf_counter()
    #    for value_group in list_of_value_groups:
    #        cursor.execute(query_template, (tuple(value_group),))
    #        cursor.fetchall()
    #    end = time.perf_counter()
    #    elapsed = end - start
    #    throughput = len(list_of_value_groups) / elapsed
    #    return throughput 
    
    throughput_in, time_elapsed = run_in_query_loop(CONN, CURSOR, query_template_IN, grouped_values)
    plan_output_in = get_explain_analyze(CONN, CURSOR, query_template_IN, grouped_values[0])


    log_test_result(MODE_LABELS['nonclustered_hash_in_pubid'], grouped_values, throughput_in, time_elapsed,plan_output_in)
    print(f" Test complete (pubID IN) - throughput: {throughput_in:.2f} queries/sec")



    # Third Index Setup Fourth Query Type
    drop_index(CONN, CURSOR, "idx_year")
    create_index (CONN, CURSOR, PUBL_TABLE, "idx_year", "hash", "year")
    print("Hash index and non-clustered table on year created !")


    query_template = "SELECT * FROM Publ WHERE year = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, year_values)

    plan_output = get_explain_analyze(CONN, CURSOR, query_template, year_values[0])

    log_test_result(MODE_LABELS['nonclustered_hash_point_year'], year_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")


    # Fourth Index Setup
    print("==================================FOURTH INDEX==================================")
    CURSOR.execute("SET enable_indexscan = OFF;")
    CURSOR.execute("SET enable_bitmapscan = OFF;")

    #drop_index(CONN, CURSOR, "idx_pubid")
    #drop_index(CONN, CURSOR, "idx_booktitle")
    #drop_index(CONN, CURSOR, "idx_year")


    # Fourth Index Setup First Query Type
    query_template = "SELECT * FROM Publ WHERE pubID = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, pubid_values)
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, pubid_values[0])
    log_test_result(MODE_LABELS["table_scan_point_pubid"], pubid_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")



    # Fourth Index Setup Second Query Type
    query_template = "SELECT * FROM Publ WHERE booktitle = %s"
    throughput_booktitle, time_elapsed = run_query_loop(CONN, CURSOR, query_template, booktitle_values)
    plan_output_booktitle = get_explain_analyze(CONN, CURSOR, query_template, booktitle_values[0])
    log_test_result(MODE_LABELS["table_scan_multipoint_booktitle"], booktitle_values, throughput_booktitle, time_elapsed,plan_output_booktitle)

    print(f"Test complete (booktitle) - throughput: {throughput_booktitle:.2f} queries/sec")


    # Fourth Index Setup Third Query Type
    query_template_IN = "SELECT * FROM Publ WHERE pubID IN %s"
    throughput_in, time_elapsed = run_in_query_loop(CONN, CURSOR, query_template_IN, grouped_values)
    plan_output_in = get_explain_analyze(CONN, CURSOR, query_template_IN, grouped_values[0])
    log_test_result(MODE_LABELS["table_scan_in_pubid"], grouped_values, throughput_in, time_elapsed,plan_output_in)
    print(f" Test complete (pubID IN) - throughput: {throughput_in:.2f} queries/sec")


    # Fourth Index Setup Fourth Query Type
    query_template = "SELECT * FROM Publ WHERE year = %s"
    throughput, time_elapsed = run_query_loop(CONN, CURSOR, query_template, year_values)
    plan_output = get_explain_analyze(CONN, CURSOR, query_template, year_values[0])
    log_test_result(MODE_LABELS["table_scan_point_year"], year_values, throughput, time_elapsed,plan_output)

    print(f"Test complete, throughput: {throughput:.2f} queries/sec")



    CURSOR.execute("SET enable_indexscan = ON;")
    CURSOR.execute("SET enable_bitmapscan = ON;")

    

    

if __name__ == "__main__":
    main()