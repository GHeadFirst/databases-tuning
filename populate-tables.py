import uuid
import random
import time
import os
from random import shuffle, randint
# from create_table import create_table
from db_connection import get_postgres_connection, get_mariadb_connection


def create_table(cursor, table_name, schema, conn):
    try:
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});"
        cursor.execute(sql)
        conn.commit()
        print(f"✅(Success) Table {table_name} was created with the following schema:\n --> {schema}")
    except Exception as e:
        print(f"⚠️(Warning): {e}")
        conn.rollback()

# Setup of our connections and cursors
conn_postgres, cursor_postgres = get_postgres_connection()
conn_mariadb, cursor_mariadb = get_mariadb_connection()

# Some definitions for later
table_employee_spec = "ssnum, name, manager , dept, salary, numfriends"
table_student_spec = "ssnum, name, course, grade"
table_techdept_spec = "dept, manager, location"



# Table creation
table_employee_name = "Employee"
table_employee_schema = "ssnum INT PRIMARY KEY, name VARCHAR(120) UNIQUE, manager VARCHAR(120), dept VARCHAR(100), salary INT, numfriends INT"

table_student_name = "Student"
table_student_schema  = "ssnum INT PRIMARY KEY, name VARCHAR(120) UNIQUE, course VARCHAR(100), grade FLOAT"

table_techdept_name = "Techdept"
table_techdept_schema = "dept VARCHAR(100), manager VARCHAR(120), location VARCHAR(70)"

# Table creation PostgreSQL
create_table(cursor_postgres,table_employee_name, table_employee_schema, conn_postgres)
create_table(cursor_postgres,table_student_name, table_student_schema, conn_postgres)
create_table(cursor_postgres,table_techdept_name, table_techdept_schema, conn_postgres)

# Table creation MariaDB
create_table(cursor_mariadb,table_employee_name, table_employee_schema, conn_mariadb)
create_table(cursor_mariadb,table_student_name, table_student_schema, conn_mariadb)
create_table(cursor_mariadb,table_techdept_name, table_techdept_schema, conn_mariadb)



#  Base directory for cross-platform path handling
base_dir = os.path.dirname(os.path.abspath(__file__))
employee_tsv_path = os.path.join(base_dir, "assignment-2", "employee.tsv")
student_tsv_path = os.path.join(base_dir, "assignment-2", "student.tsv")
techdept_tsv_path = os.path.join(base_dir, "assignment-2", "techdept.tsv")


number_of_employees = 100000
number_of_tech_employees = int(number_of_employees * 0.1)
number_of_worker_students = int(number_of_employees * 0.05)  # because I want it so that 5% of students work
number_of_students  = 100000 - number_of_worker_students
numer_of_tech_dept  = 10

location = ["Salzburg", "Berlin", "Lisbon"]

departments = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration", "Human Resources", "Marketing", "Finance", "Legal", "Sales", "Customer Service", "Public Relations", "Procurement", "Administration", "Training and Development"]

courses = ["COMP101, Intro to Computing", "ENVS202, Climate Change Policy", "ART105, Modern Art History", "ML310, Machine Learning Basics", "CYBR201, Cybersecurity Principles", "ANTH220, Food & Culture", "GDEV150, Intro to Game Design", "NEURO330, Brain & Behavior", "FILM210, Global Cinema", "BCN401, Blockchain Systems"]

unmanaged_tech_dept = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration"]

first_names = [
    "Kais", "Fares", "Adnan", "Martin", "Max", "Moritz", "Amreen", "Amsal", "Mostafa", "Zoha", "Manizheh",
    "Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant",
    "Parker", "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell",
    "Rhodes", "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane",
    "Doyle", "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt",
    "Ford", "Violet", "Nathan", "Stella", "Owen", "Hazel", "Leo", "Aurora", "Miles", "Naomi", "Eli",
    "Claire", "Asher", "Savannah", "Hudson", "Lucy", "Isaiah", "Bella", "Hunter", "Camila", "Xavier", "Jeff",
    "Eliana", "Luca", "Sienna", "Mateo", "Layla", "Ezra", "Isla", "Julian", "Nova", "Zion",
    "Aaliyah", "Theo", "Maya", "Enzo", "Ruby", "Aria", "Caleb", "Freya", "Kai", "Leilani"
]

last_names = [
    "Schäler", "Khalifa", "Albakri", "Hosseini", "Hofmann", "Kondmann", "Memic", "Schneider", "Schmidt",
    "Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant", "Parker",
    "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell", "Rhodes",
    "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane", "Doyle",
    "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt", "Ford",
    "Greene", "Ramirez", "Henderson", "Watts", "Delaney", "Klein", "Fleming", "Acosta", "Travis", "Brady",
    "Wheeler", "McCoy", "Rowe", "Schwartz", "Finley", "Arias", "Dalton", "Mayer", "Carver", "Boone", "Bezos",
    "Rosales", "Nash", "Callahan", "Griffin", "Shepherd", "Madden", "Montoya", "Lang", "Hinton", "Crane",
    "Salazar", "Gentry", "Knox", "Merritt", "Avery", "Glass", "Landry", "Pruitt", "Benton", "Forbes"
]


all_ids = []
def generate_unique_ids():
    global all_ids
    all_ids = list(range(100000, 1000000))
    shuffle(all_ids)
    
generate_unique_ids()

# Employee(ssnum, name, manager, dept, salary, numfriends)
employee_table = [f"{all_ids.pop()}\tMartin Schäler\tN/A\tCEO\t1000000\t150\n"]

# Student(ssnum, name, course, grade)
student_table = []

# Techdept(dept, manager, location)
tech_dept_table = []

tech_dept = []
tech_managers = []

current_department = 0
def make_manager():
    global current_department
    m_id = all_ids.pop()  # Use this unique ID for both the record and name
    manager_name = (
        first_names[randint(0, len(first_names) - 1)]
        + " "
        + last_names[randint(0, len(last_names) - 1)]
        + " " + str(m_id)
    )
    manager_dept = departments[current_department]
    manager_string = f"{m_id}\t{manager_name}\tMartin Schäler\t{manager_dept}\t{randint(120000, 200000)}\t{randint(0, 5)}"
    employee_table.append(manager_string + "\n")
    
    return manager_name, manager_dept




def make_employee(manager, dept, id=None, name=None, salary=None, numfriends=None):
    if id is None:
        id = all_ids.pop()
    if name is None:
        name = (
            first_names[randint(0, len(first_names) - 1)]
            + " "
            + last_names[randint(0, len(last_names) - 1)]
            + " " + str(id)
        )
    if salary is None:
        salary = randint(100000, 150000)
    if numfriends is None:
        numfriends = randint(2, 100)

    employee_string = f"{id}\t{name}\t{manager}\t{dept}\t{salary}\t{numfriends}"
    employee_table.append(employee_string + "\n")


def make_student_employee(manager, dept):
    student_employee_id = all_ids.pop()
    student_employee_name = (
        first_names[randint(0, len(first_names) - 1)]
        + " "
        + last_names[randint(0, len(last_names) - 1)]
        + " " + str(student_employee_id)
    )
    student_employee_salary = randint(20000, 50000)
    student_employee_numfriends = randint(100, 150)
    student_employee_course = courses[randint(0, len(courses) - 1)]
    student_employee_grade = round(random.uniform(1.0, 4.0), 1)

    make_employee(
        manager=manager,
        dept=dept,
        id=student_employee_id,
        name=student_employee_name,
        salary=student_employee_salary,
        numfriends=student_employee_numfriends
    )

    make_student(
        id=student_employee_id,
        name=student_employee_name,
        course=student_employee_course,
        grade=student_employee_grade
    )



def make_student(id=None, name=None, course=None, grade=None):
    if id is None or name is None or course is None or grade is None:
        student_id = all_ids.pop()
        student_name = (
            first_names[randint(0, len(first_names) - 1)]
            + " "
            + last_names[randint(0, len(last_names) - 1)]
            + " " + str(student_id)
        )
        student_course = courses[randint(0, len(courses) - 1)]
        student_grade = round(random.uniform(1.0, 4.0), 1)
        make_student_array(student_id, student_name, student_course, student_grade)
    else:
        make_student_array(id, name, course, grade)



def make_student_array(id, name, course, grade):
    student_string = f"{id}\t{name}\t{course}\t{grade}"
    student_table.append(student_string + "\n")

def make_techdept():
    for i in range(len(tech_dept)):
        tech_dept_string = tech_dept[i] + "\t" + tech_managers[i] + "\t" + location[randint(0, 2)]
        tech_dept_table.append(tech_dept_string + "\n")


def generate_record():
    global current_department
    current_department = 0
    manager_name, manager_dept = make_manager()
    tech_managers.append(manager_name)
    tech_dept.append(manager_dept)
    non_tech_employees = number_of_employees - number_of_tech_employees
    non_tech_dept_size = int(non_tech_employees / (len(departments) - 10))
    for employee in range(number_of_tech_employees):
        if (employee % 1000 == 0 and employee != 0):
            current_department += 1
            manager_name, manager_dept = make_manager()
            tech_managers.append(manager_name)
            tech_dept.append(manager_dept)
        if (employee % 20 == 0 and employee < number_of_worker_students):
            make_student_employee(manager_name, manager_dept)
            continue
        make_employee(manager_name, manager_dept)
    for employee in range(non_tech_employees):
        if (employee % non_tech_dept_size == 0):
            manager_name, manager_dept = make_manager()
            current_department += 1
        if (employee % 20 == 0):
            make_student_employee(manager_name, manager_dept)
            continue
        make_employee(manager_name, manager_dept)
    
    for student in range(int(number_of_students)):
        make_student()


def generate_tsv_file():
    with open("assignment-2/employee.tsv", "w") as f:
        f.writelines(employee_table)
    with open("assignment-2/student.tsv", "w") as f:
        f.writelines(student_table)
    with open("assignment-2/techdept.tsv", "w") as f:
        f.writelines(tech_dept_table)

def bulk_insert_postgres(file_path, table_name, table_spec):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Ensure the column list is in parentheses.
            copy_sql = f"COPY {table_name} ({table_spec}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')"
            cursor_postgres.copy_expert(copy_sql, f)
        conn_postgres.commit()
        print(f"Successfully inserted data into {table_name} (columns: {table_spec}) (PostgreSQL).")
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} (PostgreSQL): {e}")
        conn_postgres.rollback()



def bulk_insert_mariadb(file_path, table_name, table_spec):
    try:
        load_data_sql = (
            f"LOAD DATA LOCAL INFILE '{file_path}' INTO TABLE {table_name} "
            f"FIELDS TERMINATED BY '\\t' "
            f"LINES TERMINATED BY '\\n' "
            f"({table_spec})"
        )
        cursor_mariadb.execute(load_data_sql)
        conn_mariadb.commit()
        print(f"Successfully inserted data into {table_name} (columns: {table_spec}) (MariaDB).")
    except Exception as e:
        print(f"⚠️ Error inserting into {table_name} (MariaDB): {e}")
        conn_mariadb.rollback()



if __name__ == "__main__":
    generate_record()
    make_techdept()
    generate_tsv_file()
    # expert copy into PostgreSQL
    bulk_insert_postgres(employee_tsv_path, table_employee_name, table_employee_spec)
    bulk_insert_postgres(student_tsv_path, table_student_name, table_student_spec)
    bulk_insert_postgres(techdept_tsv_path, table_techdept_name, table_techdept_spec)

    # loading into MariaDB
    bulk_insert_mariadb(employee_tsv_path, table_employee_name, table_employee_spec)
    bulk_insert_mariadb(student_tsv_path, table_student_name, table_student_spec)
    bulk_insert_mariadb(techdept_tsv_path, table_techdept_name, table_techdept_spec)


