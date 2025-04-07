import uuid
import random
import time
from random import shuffle, randint
# from db_connection import get_postgres_connection, get_mariadb_connection

number_of_employees = 100000
number_of_tech_employees = int(number_of_employees * 0.1)
number_of_worker_students = int(number_of_employees * 0.05)  # because I want it so that 5% of students work
number_of_students  = 100000 - number_of_worker_students
numer_of_tech_dept  = 10

location = ["Salzburg", "Berlin", "Lisbon"]

departments = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration", "Human Resources", "Marketing", "Finance", "Legal", "Sales", "Customer Service", "Public Relations", "Procurement", "Administration", "Training and Development"]

courses = ["COMP101, Intro to Computing", "ENVS202, Climate Change Policy", "ART105, Modern Art History", "ML310, Machine Learning Basics", "CYBR201, Cybersecurity Principles", "ANTH220, Food & Culture", "GDEV150, Intro to Game Design", "NEURO330, Brain & Behavior", "FILM210, Global Cinema", "BCN401, Blockchain Systems"]

unmanaged_tech_dept = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration"]


first_names = ["Kais", "Fares", "Adnan", "Martin", "Max", "Moritz", "Amreen", "Amsal", "Mostafa", "Zoha", "Manizheh","Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant", "Parker", "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell", "Rhodes", "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane", "Doyle", "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt", "Ford", "Violet", "Nathan", "Stella", "Owen", "Hazel", "Leo", "Aurora", "Miles", "Naomi", "Eli", "Claire", "Asher", "Savannah", "Hudson", "Lucy", "Isaiah", "Bella", "Hunter", "Camila", "Xavier", "Jeff"
]

last_names = ["Schäler", "Khalifa", "Albakri", "Hosseini", "Hofmann", "Kondmann", "Memic", "Schneider", "Schmidt", "Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant", "Parker", "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell", "Rhodes", "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane", "Doyle", "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt", "Ford", "Greene", "Ramirez", "Henderson", "Watts", "Delaney", "Klein", "Fleming", "Acosta", "Travis", "Brady", "Wheeler", "McCoy", "Rowe", "Schwartz", "Finley", "Arias", "Dalton", "Mayer", "Carver", "Boone", "Bezos"
]

all_ids = []
def generate_unique_ids():
    global all_ids
    all_ids = list(range(100000,1000000))
    shuffle(all_ids)
    
generate_unique_ids()

# Employee(ssnum,name,manager,dept,salary,numfriends)
employee_table = [f"{all_ids.pop()}\tMartin Schäler\tN/A\tCEO\t1000000\t150"]

# Student(ssnum,name,course,grade)
student_table = []

# Techdept(dept,manager,location)
tech_dept_table = []

tech_dept = []
tech_managers = []



current_department = 0
def make_manager():
    manager_name = first_names[randint(0, len(first_names) - 1)] + " " + last_names[randint(0, len(last_names) - 1)]
    manager_dept = departments[current_department]
    
    manager_string = f"{all_ids.pop()}\t{manager_name}\tMartin Schäler\t{manager_dept}\t{randint(120000, 200000)}\t{randint(0, 5)}"
    employee_table.append(manager_string + "\n")
    
    return manager_name, manager_dept


def make_employee(manager, dept, id=None, name=None, salary=None, numfriends=None):
    if id is None:
        id = all_ids.pop()
    if name is None:
        name = first_names[randint(0, len(first_names) - 1)] + " " + last_names[randint(0, len(last_names) - 1)]
    if salary is None:
        salary = randint(100000, 150000)
    if numfriends is None:
        numfriends = randint(2, 100)

    employee_string = f"{id}\t{name}\t{manager}\t{dept}\t{salary}\t{numfriends}"
    employee_table.append(employee_string + "\n")

def make_student_employee(manager, dept):
    student_employee_id = all_ids.pop()
    student_employee_name = first_names[randint(0, len(first_names) - 1)] + " " + last_names[randint(0, len(last_names) - 1)]
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


def make_student(id = None, name = None, course = None, grade = None):
    if (id is None or name is None or course is None or grade is None):
        student_id = all_ids.pop()
        student_name = first_names[randint(0,len(first_names) - 1)] + " " + last_names[randint(0,len(last_names) - 1)]
        student_course = courses[randint(0,len(courses) - 1)]
        student_grade = round(random.uniform(1.0,4.0),1)
        make_student_array(student_id,student_name,student_course,student_grade)
    else:
        make_student_array(id,name,course,grade)


def make_student_array(id,name,course,grade): # might be able to just remove this function
    student_string = id + "\t" + name + "\t" + course + "\t" + grade
    student_table.append(student_string + "\n")

def make_techdept():
    for i in range(len(tech_dept)):
        tech_dept_string = tech_dept[i] + "\t" + tech_managers[i] + "\t" + location[randint(0,2)]
        tech_dept_table.append(tech_dept_string + "\n")




# ssnum, name firstname[random(i)] lastname[random], manager 
# for a unique id 6 digits should be enough for 200k people
def generate_record():
    current_department = 0
    manager_name, manager_dept = make_manager()
    tech_managers.append(manager_name)
    tech_dept.append(manager_dept)
    employees_without_managers = number_of_employees # We can just skip the index after making the manager so we do employee += 1 I think
    non_tech_employees = number_of_employees - number_of_tech_employees
    non_tech_dept_size = int(non_tech_employees / (len(departments) - 10))
    for employee in range(number_of_tech_employees):
        if (employee % 1000 == 0 and employee != 0): # I can change this condition if I want a manager to be the manager for two dept for example
                    current_department += 1
                    manager_name, manager_dept = make_manager() # everytime a manager is made we need to put the manager name in an array and with the dept name
                    tech_managers.append(manager_name)
                    tech_dept.append(manager_dept)
        if (employee % 20 == 0 and employee < number_of_worker_students ): # for every 20 employees we get a student employee
            make_student_employee(manager_name, manager_dept)
            continue
        make_employee(manager_name, manager_dept)
    for employee in range(non_tech_employees):
        if (employee % non_tech_dept_size == 0):
            manager_name, manager_dept = make_manager()  # everytime a manager is made we need to put the manager name in an array and with the dept name
            current_department += 1
        if (employee % 20 == 0):
            make_student_employee(manager_name, manager_dept)
            continue
        make_employee(manager_name, manager_dept)
    
    for student in range(number_of_students - (number_of_students * 0.05)):
        make_student()

make_techdept()



def generate_tsv_file():
    with open("employee.tsv", "w") as f:
        f.writelines(employee_table)
    with open("student.tsv", "w") as f:
        f.writelines(student_table)
    with open("techdept.tsv", "w") as f:
        f.writelines(tech_dept_table)



if __name__ == "__main__":
    generate_record()
    generate_tsv_file()
    print(employee_table)
    print("will wait 10 secs")
    time.sleep(10)
    print("waited 10 secs")
    print(student_table)
    time.sleep(10)
    print("waited 10 secs")
    print(tech_dept_table)

        
