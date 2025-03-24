import re
import time
import csv
from db_connection import get_postgres_connection, get_mariadb_connection

# Open file and read content
def openFile(path):
    try:
        with open(path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"❌ Error: File '{path}' not found.")
        return None

content = openFile("dblp/auth.tsv")
if content is None:
    exit()

# Remove empty entries
content = list(filter(None, re.split(r'[\t\n]', content)))

# at first we used modulo to sort whether the element belong to myauthor or to mybook, if it was odd it was mybook, even then myauthor
# now we changed it to using a for loop where it increments twice
myauthor = []
mybook = []

for i in range(0, len(content) - 1, 2):
    myauthor.append(content[i])
    mybook.append(content[i + 1])

# our author often had an extra element so in case that happens we add an unknown
if len(myauthor) > len(mybook):
    mybook.append("UNKNOWN")

print(f"✅ Length of myauthor: {len(myauthor)}")  # to check the length of our authors
print(f"✅ Length of mybook: {len(mybook)}")

data_to_insert = [(myauthor[i], mybook[i]) for i in range(len(myauthor))]

# connects to our database by getting the function return from our db_connection.py script
conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

try:
    cursor_postgres.execute("CREATE TABLE IF NOT EXISTS Auth (name TEXT, pubID TEXT);")
    conn_postgres.commit()
except Exception as e:
    print(f"⚠️ Warning: {e}")
    conn_postgres.rollback()

tsv_file = "data_to_insert.tsv"
with open(tsv_file, "w", newline="") as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerows(data_to_insert)

start_time = time.time()

with open(tsv_file, "r") as f:
    try:
        cursor_postgres.copy_expert("COPY Auth (name, pubID) FROM STDIN WITH DELIMITER E'\t'", f)
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
