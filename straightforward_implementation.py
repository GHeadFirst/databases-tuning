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


# at first we used modulo to sort whether the element belong to myauthor or to mybook, if it was odd it was mybook, even then myauthor
# now we use list slicing
def parseDataAuth(string):
    string = list(filter(None, re.split(r'[\t\n]', string)))
    myauthor = string[0::2]  # Even indices → authors
    mybook = string[1::2]  # Odd indices → books
    
    # our author often had an extra element so incase that happens we add an unkown
    if len(myauthor) > len(mybook):
        mybook.append("UNKNOWN")

    print(f"✅ Length of myauthor: {len(myauthor)}") # to check the length of our authors
    print(f"✅ Length of mybook: {len(mybook)}")
    return myauthor, mybook

#  Publ(pubID(129),type(13),title(700),booktitle(132),year(4),publisher(196))
def parseDataPubl(string):
    lines = string.splitlines()  # ✅ Split into rows
    pubID, mytype, mytitle, mybooktitle, myyear, mypublisher = [], [], [], [], [], []
    
    for line in lines:
        fields = line.split("\t")  # ✅ Split by tab (since it's TSV)
        
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
def create_table(cursor, table_name, schema):
    try:
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        conn_postgres.commit()
        print(f"✅ Table '{table_name}' is ready.")
    except Exception as e:
        print(f"⚠️ Warning: {e}")  
        conn_postgres.rollback()  

# we read our files once then pass them to our parseData function to avoid reading them twice
fileAuth = openFile("dblp/auth.tsv")
filePubl = openFile("dblp/publ.tsv")

if fileAuth:
    myauthor, mybook = parseDataAuth(fileAuth)

if filePubl:
    pubID, mytype, mytitle, mybooktitle, myyear, mypublisher = parseDataPubl(filePubl)

# connects to our database by getting the function return from our db_connection.py script
conn_postgres = get_postgres_connection()
cursor_postgres = conn_postgres.cursor()

# we create our two tables and have an exception in case they exist or if another error occurs
create_table(cursor_postgres, "Auth", "name VARCHAR(49), pubID VARCHAR(149)")
create_table(cursor_postgres, "Publ", "pubID VARCHAR(129), type VARCHAR(13), title VARCHAR(700), booktitle VARCHAR(132), year VARCHAR(4), publisher VARCHAR(196)")

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

pubID_start = time.time()  
# insert data for publ
for index in range(len(pubID)):
    print(f"📝 Inserting Row {index + 1}:")

    cursor_postgres.execute(
        "INSERT INTO Publ VALUES (%s,%s,%s,%s,%s,%s)", (pubID[index].strip(), mytype[index].strip(),mytitle[index].strip(),mybooktitle[index].strip(),myyear[index].strip(),mypublisher[index].strip()) # here we use paremetized queries to prevent SQL injections, we used
        # normal .format or string literals but it can break the code in case of O'Reiley, which by the fourth row I think there was a case of
    )

conn_postgres.commit() # this saves everything to our database and keeps the data there
pubID_end = time.time()  # ⏳ Stop timing

elapsed_time = pubID_end - pubID_start
print(f"🕒 Time taken to insert {len(pubID)} records: {elapsed_time:.2f} seconds for Publ table") # Time taken to insert 1233214 records: 66.54 seconds for Publ table

print(" in our test.py we have a test to see how many records got added")


"""     print(f"pubID: {repr(pubID[index])} (Length: {len(pubID[index])})")
    print(f"mytype: {repr(mytype[index])} (Length: {len(mytype[index])})")
    print(f"mytitle: {repr(mytitle[index])} (Length: {len(mytitle[index])})")
    print(f"mybooktitle: {repr(mybooktitle[index])} (Length: {len(mybooktitle[index])})")
    print(f"myyear: {repr(myyear[index])} (Length: {len(myyear[index])})")
    print(f"mypublisher: {repr(mypublisher[index])} (Length: {len(mypublisher[index])})") """