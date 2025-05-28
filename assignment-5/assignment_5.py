from os.path import isfile
import sys
import os
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import utilities
from db_connection import get_mariadb_connection, get_postgres_connection

def log_to_file(content, file_name="merge-log.txt"):
    try:
        with open(file_name,'a') as f:
            f.write(content + "\n")
    except Exception as e:
        raise FileNotFoundError(f"…") from e
        

def run_test_case(setup_number,conn,cursor,queries):
# === Test: Clustered B+ Tree - Point Query on pubID ===
    log_to_file(f"\n=============== Running Test Case {setup_number}: {SETUPS[setup_number]} ===============")

    join = SETUPS[setup_number]["join"]

    cursor.execute("SET enable_hashjoin TO %s;" % ('true' if join == 'hash' else 'false'))
    cursor.execute("SET enable_mergejoin TO %s;" % ('true' if join == 'merge' else 'false'))
    cursor.execute("SET enable_nestloop TO %s;" % ('true' if join == 'nested' else 'false'))

    utilities.drop_all_indexes(conn,cursor)

    index = SETUPS[setup_number]["index"]
    if index == "nonclust_publ":
        cursor.execute("CREATE INDEX publ_pubid_idx ON Publ(pubID);")
    elif index == "nonclust_auth":
        cursor.execute("CREATE INDEX auth_pubid_idx ON Auth(pubID);")
    elif index == "nonclust_both":
        cursor.execute("CREATE INDEX publ_pubid_idx ON Publ(pubID);")
        cursor.execute("CREATE INDEX auth_pubid_idx ON Auth(pubID);")
    elif index == "clust_both":
        cursor.execute("CREATE INDEX publ_pubid_idx ON Publ(pubID);")
        cursor.execute("CLUSTER Publ USING publ_pubid_idx;")
        cursor.execute("CREATE INDEX auth_pubid_idx ON Auth(pubID);")
        cursor.execute("CLUSTER Auth USING auth_pubid_idx;")

    contents = f"=== Test Case:{setup_number} ===\n The following is being tested: {join} with index:{index}"
    conn.commit()

    for label, query in queries:
        log_to_file(f"🧪 {label} (Setup {setup_number})")
        cursor.execute("EXPLAIN ANALYZE " + query)
        for row in cursor.fetchall():
            log_to_file(row[0])





SETUPS = {
1: {"join": "hash",   "index": "none"},
2: {"join": "merge",  "index": "none"},
3: {"join": "merge",  "index": "nonclust_both"},
4: {"join": "merge",  "index": "clust_both"},
5: {"join": "nested", "index": "nonclust_publ"},
6: {"join": "nested", "index": "nonclust_auth"},
7: {"join": "nested", "index": "nonclust_both"},
}



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
    AUTH_COLUMNS = "name, pubID"
    PUBL_COLUMNS = "pubID, type, title, booktitle, year, publisher"
    
    # file path of data to be inserted into tables
    AUTH_FILE_PATH = "dblp/auth.tsv"
    PUBL_FILE_PATH = "dblp/publ.tsv"

    # We drop our tables
    utilities.drop_postgres(CONN, CURSOR)

    # We create our tables and insert our data
    utilities.create_table(CONN,CURSOR, AUTH_TABLE, AUTH_SCHEMA)
    utilities.create_table(CONN,CURSOR, PUBL_TABLE, PUBL_SCHEMA)
    
    utilities.copy_expert(CONN,CURSOR,PUBL_TABLE, PUBL_COLUMNS,"\t",PUBL_FILE_PATH)
    utilities.copy_expert(CONN,CURSOR,AUTH_TABLE, AUTH_COLUMNS,"\t",AUTH_FILE_PATH)

    QUERIES = [
    ("Q1", "SELECT name, title FROM Auth, Publ WHERE Auth.pubID = Publ.pubID;"),
    ("Q2", "SELECT title FROM Auth, Publ WHERE Auth.pubID = Publ.pubID AND Auth.name = 'Divesh Srivastava';")
    ]


    QUERY_1 = "SELECT name , title FROM Auth , Publ WHERE Auth . pubID = Publ . pubID;"
    QUERY_2 = "SELECT title FROM Auth , Publ WHERE Auth . pubID = Publ . pubID AND Auth . name = ’ Divesh Srivastava ’"

    for setup in SETUPS:
        if (SETUPS[setup]["index"] == "clust_both"):
            utilities.drop_postgres(CONN,CURSOR)
            utilities.create_table(CONN,CURSOR, AUTH_TABLE, AUTH_SCHEMA)
            utilities.create_table(CONN,CURSOR, PUBL_TABLE, PUBL_SCHEMA)
            
            utilities.copy_expert(CONN,CURSOR,PUBL_TABLE, PUBL_COLUMNS,"\t",PUBL_FILE_PATH)
            utilities.copy_expert(CONN,CURSOR,AUTH_TABLE, AUTH_COLUMNS,"\t",AUTH_FILE_PATH)
            
        run_test_case(setup,CONN,CURSOR,QUERIES)
    
    

if __name__ == "__main__":
    main()








