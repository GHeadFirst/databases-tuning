import uuid
import random
from random import shuffle

from db_connection import get_postgres_connection, get_mariadb_connection

number_of_employees = 100000
numer_of_worker_students = number_of_employees * 0.05 # because I want it so that 5% of students work
number_of_students  = 100000 - number_of_worker_students
numer_of_tech_dept  = 10

location = ["San Francsico", "Salzburg", "New York", "Berlin", "Hamburg", "Lisbon"]

departments = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration", "Human Resources", "Marketing", "Finance", "Legal", "Sales", "Customer Service", "Public Relations", "Procurement", "Administration", "Training and Development"]

tech_dept = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration"]

courses = ["COMP101, Intro to Computing", "ENVS202, Climate Change Policy", "ART105, Modern Art History", "ML310, Machine Learning Basics", "CYBR201, Cybersecurity Principles", "ANTH220, Food & Culture", "GDEV150, Intro to Game Design", "NEURO330, Brain & Behavior", "FILM210, Global Cinema", "BCN401, Blockchain Systems"]

unmanaged_tech_dept = ["Software Development", "IT Support / Help Desk", "Network Engineering", "Cybersecurity", "Data Science", "DevOps", "Quality Assurance (QA)", "Cloud Engineering", "Artificial Intelligence", "Database Administration"]


first_names = ["Kais", "Fares", "Adnan", "Martin", "Max", "Moritz", "Amreen", "Amsal", "Mostafa", "Zoha", "Manizheh","Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant", "Parker", "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell", "Rhodes", "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane", "Doyle", "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt", "Ford", "Violet", "Nathan", "Stella", "Owen", "Hazel", "Leo", "Aurora", "Miles", "Naomi", "Eli", "Claire", "Asher", "Savannah", "Hudson", "Lucy", "Isaiah", "Bella", "Hunter", "Camila", "Xavier", "Jeff"
]

last_names = ["Schäler", "Khalifa", "Albakri", "Hosseini", "Hofmann", "Kondmann", "Memic", "Schneider", "Schmidt", "Bennett", "Sullivan", "Carter", "Hayes", "Mitchell", "Brooks", "Reed", "Cooper", "Bryant", "Parker", "Lawson", "Dean", "Ward", "Andrews", "Pierce", "Grant", "Palmer", "Blake", "Maxwell", "Rhodes", "Foster", "Vaughn", "Jennings", "Barrett", "Walsh", "Malone", "Franklin", "Bishop", "Lane", "Doyle", "Chandler", "Sutton", "Sinclair", "Pratt", "Steele", "Benson", "Ramsey", "Harmon", "Holt", "Ford", "Greene", "Ramirez", "Henderson", "Watts", "Delaney", "Klein", "Fleming", "Acosta", "Travis", "Brady", "Wheeler", "McCoy", "Rowe", "Schwartz", "Finley", "Arias", "Dalton", "Mayer", "Carver", "Boone", "Bezos"
]

all_ids
def generate_unique_ids():
    all_ids = list(range(100000,1000000))
    shuffle(all_ids)
    

# Employee(ssnum,name,manager,dept,salary,numfriends)
employee = [all_ids.pop()+"\tMartin Schäler\tN/A\tCEO\t1000000\t150"]

# Student(ssnum,name,course,grade)
student = []

# Techdept(dept,manager,location)
tech_dept_table = []

manager_names = []


manager_name, manager_dept

current_department = 0
def make_manager(): # we can delegate here to call the make_employee method instead of directly adding to employee array
    manager_name = first_names[randint(0,len(first_names) - 1)] + " " + last_names[randint(0,len(last_names) - 1)]
    manager_dept = departments[current_department]
    manager_string == all_ids.pop() + "\t" + manager_name + "\t" + "Martin Schäler" + "\t" + manager_dept + "\t" + randint(120000,200000) + "\t" + randint(0,5) + "\n"
    employee.append(manager_string)
    return  manager_name 

def make_employee(id,name,manager,dept,salary,numfriends):
    employee_string = id + "\t" + name + "\t" + manager + "\t" + dept + "\t" + salary + "\t" + numfriends + "\t" + "\n"

def make_employee(manager,dept):
    make_employee(all_ids.pop(), first_names[randint(0,len(first_names) - 1)] + " " + last_names[randint(0,len(last_names) - 1)], manager, dept,randint(100000,150000), randint(2,100))


def make_student_employee():
    student_employee_id = all_ids.pop()
    student_employee_name = first_names[randint(0,len(first_names) - 1)] + " " + last_names[randint(0,len(last_names) - 1)]
    student_employee_manager = manager_name
    student_employee_dept = departments[current_department]
    student_employee_salary = randint(20000,50000)
    student_employee_numfriends = randint(100,150)
    student_employee_course = courses[randint(0,len(courses) - 1)]
    student_employee_grade = round(random.uniform(1.0,4.0),1)
    make_employee(student_employee_id, student_employee_name, student_employee_manager, student_employee_dept, student_employee_salary, student_employee_numfriends)
    make_student(student_employee_id, student_employee_name, student_employee_course, student_employee_grade)

def make_student():
    student_id = all_ids.pop()
    student_name = first_names[randint(0,len(first_names) - 1)] + " " + last_names[randint(0,len(last_names) - 1)]
    student_course = courses[randint(0,len(courses) - 1)]
    student_grade = round(random.uniform(1.0,4.0),1)
    make_student(student_id,student_name,student_course,student_grade)

def make_student (id,name,course,grade):
    student_string = id + "\t" + name + "\t" + course + "\t" + grade

def make_techdept():
    dept = 


    


# ssnum, name firstname[random(i)] lastname[random], manager 
# for a unique id 6 digits should be enough for 200k people
def generate_record():
    employees_without_managers = number_of_employees - 20 # 20 because that is how many managers we have
    non_tech_employees = employees_without_managers * 0.1
    for employee in range[non_tech_employees]
        while (employee < 10000) # 0.1 because only 10% need to be in a technical department
            int i = 0
                if ( i % 1000 == 0)
                    current_department += 1
                    manager_name = make_manager()
                if (i % 20 == 0 ) # every 5th thousand employee I want a student employee
                    make_student_employee()
                    i += 1
                    continue
            make_employee(manager_name,departments[current_department])
            i += 1
        
        if (employee % 1000 == 0)
            current_department += 1
            manager_name = make_manager()
        if (employee % 20 == 0)
            make_student_employee()
        make_employee(manager_name, department[current_department])
    
    for students in range(number_of_students)




def generate_tsv_file:
    with open("employee.tsv","w") as f:
        
