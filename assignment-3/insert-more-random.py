import random
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection

def insert_many_employees(cursor, conn, count=5000):
    departments = ["HR", "Engineering", "Marketing", "Finance"]
    
    # Generate unique, random ssnums
    ssnums = random.sample(range(200000, 1000000), count)
    
    for i in range(count):
        ssnum = ssnums[i]
        name = f"TestUser_{i}"
        dept = random.choice(departments)
        salary = random.randint(40000, 90000)

        cursor.execute(
            "INSERT INTO Employee (ssnum, name, dept, salary) VALUES (%s, %s, %s, %s);",
            (ssnum, name, dept, salary)
        )
    
    conn.commit()
    print(f"✅ Inserted {count} random rows into Employee.")

if __name__ == "__main__":
    conn, cursor = get_postgres_connection()
    insert_many_employees(cursor, conn)