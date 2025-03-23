import re
import time
import csv
from db_connection import get_postgres_connection, get_mariadb_connection
from straightforward_implementation.py import parseDataAuth, parseDataPubl, openFile, create_table



# we read our files once then pass them to our parseData function to avoid reading them twice
fileAuth = openFile("dblp/auth.tsv")
filePubl = openFile("dblp/publ.tsv")

if fileAuth:
    myauthor, mybook = parseDataAuth(fileAuth)

if filePubl:
    pubID, mytype, mytitle, mybooktitle, myyear, mypublisher = parseDataPubl(filePubl)

print(f"✅ Length of myauthor: {len(myauthor)}") # to check the length of our authors
print(f"✅ Length of mybook: {len(mybook)}")

data_to_insert = [(myauthor[i], mybook[i]) for i in range(len(myauthor))]

# connects to our database by getting the function return from our db_connection.py script
conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

create_table(cursor_postgres, "Auth", "name VARCHAR(49), pubID VARCHAR(149)", conn_postgres)

create_table(cursor_postgres, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)", conn_postgres)


csv_file = "data_to_insert.csv"
with open(csv_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerows(data_to_insert)

start_time = time.time()

with open(csv_file, "r") as f:  
    try:
        cursor_postgres.copy_expert("COPY Auth (name, pubID) FROM STDIN WITH CSV HEADER", f)  
        print(f"✅ Successfully inserted batch {len(data_to_insert)} records into Auth table.")  
    except Exception as e:
        print(f"⚠️ Error during Bulk-insert: {e}")
        conn_postgres.rollback()

end_time = time.time()

elapsed_time = end_time - start_time

print(f"🕒 Time taken to insert {len(data_to_insert)} records into Auth table: {elapsed_time:.2f} seconds")

print(f"Inserted {len(data_to_insert)} records successfully.")

cursor_postgres.close()
conn_postgres.close()