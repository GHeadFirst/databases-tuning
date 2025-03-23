from db_connection import get_postgres_connection, get_mariadb_connection
import re


""" This is for referencing, since we can use the variables from the other file
    postgres_conn = psycopg2.connect(
        dbname="mydb",
        user="user",
        password="password",
        host="postgres_db",
        port=5432
    )
    postgres_cursor = postgres_conn.cursor()
    postgres_cursor.execute("SELECT version();")
    postgres_version = postgres_cursor.fetchone()
    print(f"✅ Connected to PostgreSQL: {postgres_version[0]}")
    postgres_cursor.close()
    postgres_conn.close()
 """

""" connection = psycopg2.connect("dbname=mydb user=user") # Establishes a connection

cur = conn.cursor()


cur.execute("CREATE TABLE Auth (name varchar(49), pubID varchar(149));")
 """

def openFile(path):
    try:
        file = open(path,'r')
        if file is None:
            print("Error with file, file either doesn't exist or wrong path")
        content = file.read()
        return content
    finally:
        file.close()

def parseData(string):
    if (string != string):
        print("parseData recieved a non string as input")

content = openFile("dblp/auth.tsv")
content = re.split(r'[\t\n]',content)


i = 0
myauthor = []
mybook = []
for x in content:
    module = i%2 
    if (module== 0):
        myauthor.append(x)
    else: 
        mybook.append(x)
    i = i + 1

print(content[0])
print(content[1])
print(content[2])
print(content[3])
print(content[4])
print(content[5])
print(content[6])
print(content[7])

print("-------------authors-------------")
print(myauthor[0])
print(myauthor[1])
print(myauthor[2])
print(myauthor[3])

print("------------books-------------")
print(mybook[0])
print(mybook[1])
print(mybook[2])
print(mybook[3])

for x in myauthor:
    myinsert = f"(INSERT INTO Auth VALUES {myauthor[i]}, {mybook[i]})"





