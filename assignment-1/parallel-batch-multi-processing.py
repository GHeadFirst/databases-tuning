import re
import time
import csv
import multiprocessing
from db_connection import get_postgres_connection

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

myauthor = []
mybook = []

for i in range(0, len(content) - 1, 2):
    myauthor.append(content[i])
    mybook.append(content[i + 1])

# If there is an extra author without a corresponding book, add "UNKNOWN" as a placeholder
if len(myauthor) > len(mybook):
    mybook.append("UNKNOWN")

print(f"✅ Length of myauthor: {len(myauthor)}")
print(f"✅ Length of mybook: {len(mybook)}")

data_to_insert = [(myauthor[i], mybook[i]) for i in range(len(myauthor))]

# Split the data into smaller parts
def split_data(input_data, num_parts):
    part_size = len(input_data) // num_parts
    file_parts = []
    
    for i in range(num_parts):
        start = i * part_size
        end = None if i == num_parts - 1 else (i + 1) * part_size
        file_part = f"data_part_{i + 1}.tsv"
        file_parts.append(file_part)

        with open(file_part, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')  # Use tab delimiter for TSV
            writer.writerow(['name', 'pubID'])  # write header
            writer.writerows(input_data[start:end])

    return file_parts

# Save the data in TSV format and split it
split_parts = split_data(data_to_insert, num_parts=8)

# Function to insert data into the database
def load_data(file_part):
    """Load data from TSV to PostgreSQL."""
    try:
        conn_postgres = get_postgres_connection()
        cursor_postgres = conn_postgres.cursor()
        
        with open(file_part, 'r') as f:
            cursor_postgres.copy_expert("COPY Auth (name, pubID) FROM STDIN WITH CSV HEADER DELIMITER '\t'", f)
        
        conn_postgres.commit()
        cursor_postgres.close()
        conn_postgres.close()
        print(f"✅ Successfully loaded {file_part}")
    except Exception as e:
        print(f"❌ Error loading {file_part}: {e}")

# Parallel insertion using multiprocessing
start_time = time.time()

if __name__ == '__main__':
    with multiprocessing.Pool(processes=8) as pool:
        pool.map(load_data, split_parts)

end_time = time.time()

elapsed_time = end_time - start_time

print(f"🕒 Time taken to insert all records into Auth table: {elapsed_time:.2f} seconds")
