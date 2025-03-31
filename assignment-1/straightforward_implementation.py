import re
import time
import os
from db_connection import get_postgres_connection, get_mariadb_connection  

base_dir = os.path.dirname(os.path.abspath(__file__)) 

# Open file and read content
def openFile(path):
    try:
        full_path = os.path.join(base_dir, path)  # 
        with open(path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        print(f"❌ Error: File '{path}' not found.")
        return None

# at first we used modulo to sort whether the element belong to myauthor or to mybook, if it was odd it was mybook, even then myauthor
# now we use list slicing
def parseDataAuth(string):
    string = list(filter(None, re.split(r'[\t\n]', string)))
    myauthor = string[0::2]  # Even indices → authors
    mybook = string[1::2]  # Odd indices → books
    
    # our author often had an extra element so incase that happens we add an unkown
    if len(myauthor) > len(mybook):
        mybook.append("UNKNOWN")

    print(f" Length of myauthor: {len(myauthor)}") # to check the length of our authors
    print(f"✅ Length of mybook: {len(mybook)}")
    return myauthor, mybook

#  Publ(pubID(129),type(13),title(700),booktitle(132),year(4),publisher(196))
def parseDataPubl(string):
    lines = string.splitlines()  # Split into rows
    pubID, mytype, mytitle, mybooktitle, myyear, mypublisher = [], [], [], [], [], []
    
    for line in lines:
        fields = line.split("\t")  # Split by tab (since it's TSV)
        
        if len(fields) != 6:  # ✅ Ensure each row has exactly 6 columns
            print(f"⚠️ Skipping malformed row: {fields}")
            continue  # ✅ Skip bad rows
        
        pubID.append(fields[0])
        mytype.append(fields[1])
        mytitle.append(fields[2])
        mybooktitle.append(fields[3])
        myyear.append(fields[4])
        mypublisher.append(fields[5])

    print(f"✅ Length of pubID: {len(pubID)}") 
    print(f"✅ Length of mytype: {len(mytype)}")
    print(f"✅ Length of mytitle: {len(mytitle)}") 
    print(f"✅ Length of mybooktitle: {len(mybooktitle)}")
    print(f"✅ Length of myyear: {len(myyear)}") 
    print(f"✅ Length of mypublisher: {len(mypublisher)}")

    return pubID, mytype, mytitle, mybooktitle, myyear, mypublisher

# connects to our database by getting the function return from our db_connection.py script
def create_table(cursor, table_name, schema, conn):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        conn.commit()
        print(f"✅ Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Warning: {e}")  
        conn.rollback()  

# we read our files once then pass them to our parseData function to avoid reading them twice
fileAuth = openFile(os.path.join(base_dir, "dblp", "auth.tsv") )
filePubl = openFile(os.path.join(base_dir, "dblp", "publ.tsv") )

if fileAuth:
    myauthor, mybook = parseDataAuth(fileAuth)

if filePubl:
    pubID, mytype, mytitle, mybooktitle, myyear, mypublisher = parseDataPubl(filePubl)

# 🔹 PostgreSQL Part  


conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

# we create our two tables and have an exception in case they exist or if another error occurs
create_table(cursor_postgres, "Auth", "name VARCHAR(49), pubID VARCHAR(149)", conn_postgres)
create_table(cursor_postgres, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)", conn_postgres)

start_time_postgres_auth = time.time()  # records the time needed, I had my laptop at performance mode and it took 🕒 inserted 3,095,201 records: 84.59 seconds

# insert data into database for PostgreSQL
for index in range(len(myauthor)):
    cursor_postgres.execute(
    "INSERT INTO Auth VALUES (%s, %s)", (myauthor[index], mybook[index]) # here we use paremetized queries to prevent SQL injections, we used
    # normal .format or string literals but it can break the code in case of O'Reiley, which by the fourth row I think there was a case of
)

conn_postgres.commit() # Saves everything to PostgreSQL
end_time_postgres_auth = time.time()  # ⏳ Stop timing

elapsed_time_postgres_auth = end_time_postgres_auth - start_time_postgres_auth
print(f"🕒 Time taken to insert {len(myauthor)} records: {elapsed_time_postgres_auth:.2f} seconds for Auth table PostgreSQL") # 🕒 Time taken to insert 3095201 records: 84.59 seconds for Auth table PostgreSQL

print(" in our test.py we have a test to see how many records got added")

start_time_postgres_publ = time.time()  
# insert data for Publ in PostgreSQL
for index in range(len(pubID)):
    print(f"📝 Inserting Row {index + 1}:")

    cursor_postgres.execute(
        "INSERT INTO Publ VALUES (%s,%s,%s,%s,%s,%s)", 
        (pubID[index].strip(), mytype[index].strip(), mytitle[index].strip(), 
         mybooktitle[index].strip(), myyear[index].strip(), mypublisher[index].strip()) # here we use paremetized queries to prevent SQL injections
    )

conn_postgres.commit() #  Saves everything to PostgreSQL
end_time_postgres_publ = time.time()  # ⏳ Stop timing

elapsed_time_postgres_publ = end_time_postgres_publ - start_time_postgres_publ
print(f"🕒 Time taken to insert {len(pubID)} records: {elapsed_time_postgres_publ:.2f} seconds for Publ table PostgreSQL") # Time taken to insert 1233214 records: 66.54 seconds for Publ table PostgreSQL

print(" in our test.py we have a test to see how many records got added")

# 🔹 MariaDB Part  
print(" The same as above but now with MariaDB")

conn_mariadb = get_mariadb_connection()
cursor_mariadb = conn_mariadb.cursor()

# we create our two tables and have an exception in case they exist or if another error occurs
create_table(cursor_mariadb, "Auth", "name VARCHAR(49), pubID VARCHAR(149)", conn_mariadb)
create_table(cursor_mariadb, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)", conn_mariadb)

start_time_mariadb_auth = time.time()  

# insert data into database for MariaDB
for index in range(len(myauthor)):
    cursor_mariadb.execute(
    "INSERT INTO Auth VALUES (%s, %s)", (myauthor[index], mybook[index])
)

conn_mariadb.commit() #  Saves everything to MariaDB
end_time_mariadb_auth = time.time()  

elapsed_time_mariadb_auth = end_time_mariadb_auth - start_time_mariadb_auth
print(f"🕒 Time taken to insert {len(myauthor)} records: {elapsed_time_mariadb_auth:.2f} seconds for Auth table MariaDB") # 🕒 Time taken to insert 3095201 records: 132.10 seconds for Auth table MariaDB

start_time_mariadb_publ = time.time()  
# insert data for Publ in MariaDB
for index in range(len(pubID)):
    print(f"📝 Inserting Row {index + 1}:")

    cursor_mariadb.execute(
        "INSERT INTO Publ VALUES (%s,%s,%s,%s,%s,%s)", 
        (pubID[index].strip(), mytype[index].strip(), mytitle[index].strip(), 
         mybooktitle[index].strip(), myyear[index].strip(), mypublisher[index].strip())
    )

conn_mariadb.commit() #  Saves everything to MariaDB
end_time_mariadb_publ = time.time()  

elapsed_time_mariadb_publ = end_time_mariadb_publ - start_time_mariadb_publ
print(f"🕒 Time taken to insert {len(pubID)} records: {elapsed_time_mariadb_publ:.2f} seconds for Publ table MariaDB") # Time taken to insert 1233214 records: 86.90 seconds for Publ table MariaDB

print(" in our test.py we have a test to see how many records got added")

print(f"Elapsed Time Postgres Auth Table: {elapsed_time_postgres_auth:.2f} seconds\n"
      f"Elapsed Time Postgres Publ Table: {elapsed_time_postgres_publ:.2f} seconds\n"
      f"Elapsed Time MariaDB Auth Table: {elapsed_time_mariadb_auth:.2f} seconds\n"
      f"Elapsed Time MariaDB Publ Table: {elapsed_time_mariadb_publ:.2f} seconds")


""" Elapsed Time Postgres Auth Table: 84.73 seconds
Elapsed Time Postgres Publ Table: 65.60 seconds
Elapsed Time MariaDB Auth Table: 120.69 seconds
Elapsed Time MariaDB Publ Table: 81.10 seconds
root@cab69331de81:/app# 
 """

if __name__ == "__main__":
    run_straightforward_approach() 
