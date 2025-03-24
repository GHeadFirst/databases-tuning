# 🚀 Database Tuning: Efficient Data Loading

## 📌 Project Overview

This project explores different methods for efficiently loading large datasets into **PostgreSQL** and **MariaDB** databases. We compare multiple approaches, including **row-by-row inserts, bulk loading, and parallel batch processing** to analyze their performance impact.

To ensure **cross-platform compatibility** and a **consistent environment**, we use **Docker and Docker Compose** for database management and containerized execution.

---

## 🛠️ Setup Instructions

### 🔹 Prerequisites

Ensure you have the following installed on your system:

- **Docker & Docker Compose** ([Install Docker](https://docs.docker.com/get-docker/))
- **Python 3.9+** ([Download Python](https://www.python.org/downloads/))
- **Git** ([Install Git](https://git-scm.com/downloads))

### 🔹 Step 1: Clone the Repository

```bash
git clone https://github.com/your-repo-name/database-tuning.git
cd database-tuning
```

### 🔹 Step 2: Build and Start the Database Containers

```bash
docker-compose up -d --build
```

This will:

- Start **PostgreSQL** on `localhost:5432` and **MariaDB** on `localhost:3307`
- Set up **volumes** to persist database data
- Apply predefined environment variables for authentication

To stop the containers, run:

```bash
docker-compose down
```

To remove everything, including stored data:

```bash
docker-compose down -v
```

### 🔹 Step 3: Install Dependencies

Run the following command to install the required Python libraries:

```bash
pip install -r requirements.txt
```

### 🔹 Step 4: Run Data Loading Scripts

Run any of the following scripts to test different approaches:

```bash
python straightforward_implementation.py   # Basic row-by-row inserts
python copy_expert.py                      # Bulk loading using COPY (PostgreSQL)
python parallel_batch_multi_processing.py   # Parallel batch processing
```

Before running another test, reset the database to avoid performance variations:

```bash
python clear-tables.py
```

---

## 🔄 Portability

We ensured **cross-platform compatibility** using:

✅ **Docker** – Ensures a **consistent** environment across Linux & Windows.\
✅ **Python ****`os.path`** – Dynamically handles file paths for different OS.\
✅ **Database Abstraction (****`db_connection.py`****)** – Manages connections for PostgreSQL & MariaDB.\
✅ **Adapted Queries** – Used `COPY` for PostgreSQL & `LOAD DATA INFILE` for MariaDB.

📌 **Limitations:**

- Some database operations, such as `COPY` in PostgreSQL, are not portable to MariaDB.
- Docker dependency means the setup requires **Docker installed** on any system running the code.

---

## 📜 References

- **PostgreSQL Documentation**: [https://www.postgresql.org/docs/](https://www.postgresql.org/docs/)
- **MariaDB Documentation**: [https://mariadb.com/kb/en/documentation/](https://mariadb.com/kb/en/documentation/)
- **Docker Documentation**: [https://docs.docker.com/](https://docs.docker.com/)
- **Psycopg2 Documentation**: [https://www.psycopg.org/docs/](https://www.psycopg.org/docs/)
- **MySQL Connector Documentation**: [https://dev.mysql.com/doc/connector-python/en/](https://dev.mysql.com/doc/connector-python/en/)
- **GitHub Repository**: [https://github.com/GHeadFirst/databases-tuning](https://github.com/GHeadFirst/databases-tuning)

