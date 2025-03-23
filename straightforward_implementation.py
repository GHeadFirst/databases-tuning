import re
import time
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

# our author often had an extra element so incase that happens we add an unkown
if len(myauthor) > len(mybook):
    mybook.append("UNKNOWN")  

print(f"✅ Length of myauthor: {len(myauthor)}") # to check the length of our authors
print(f"✅ Length of mybook: {len(mybook)}")

# connects to our database by getting the function return from our db_connection.py script
conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

try:
    cursor_postgres.execute("CREATE TABLE Auth (name VARCHAR(49), pubID VARCHAR(149));")
    conn_postgres.commit()
except Exception as e:
    print(f"⚠️ Warning: {e}")  
    conn_postgres.rollback()  

try:
    cursor_postgres.execute("CREATE TABLE Publ(pubID VARCHAR(129),type VARCHAR(13),title VARCHAR(700),booktitle VARCHAR(132),year VARCHAR(4),publisher VARCHAR(196));")
    conn_postgres.commit()
except Exception as e:
    print(f"⚠️ Warning: {e}") 
    conn_postgres.rollback() 

start_time = time.time()  # records the time needed, I had my laptop at performance mode and it took 🕒 inserted 3,095,201 records: 84.59 seconds


# insert data into database
for index in range(len(myauthor)):
    cursor_postgres.execute(
        "INSERT INTO Auth VALUES (%s, %s)", (myauthor[index], mybook[index]) # here we use paremetized queries to prevent SQL injections, we used
        # normal .format or string literals but it can break the code in case of O'Reiley, which by the fourth row I think there was a case of
    )

conn_postgres.commit() # this saves everything to our database and keeps the data there
end_time = time.time()  # ⏳ Stop timing


elapsed_time = end_time - start_time
print(f"🕒 Time taken to insert {len(myauthor)} records: {elapsed_time:.2f} seconds for Auth table")
print(" in our test.py we have a test to see how many records got added")