import threading
import time
import logging
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db_connection import get_postgres_connection
from utilities import create_table

# Logging
logging.basicConfig(
    filename='experiment_results.log',
    level=logging.INFO,
    format='%(asctime)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

TRANSFER_COUNT = 100

# Setup table
conn, cursor = get_postgres_connection()
account_schema = "aid INTEGER PRIMARY KEY, balance INTEGER"
create_table(conn, cursor, "account", account_schema)
cursor.execute("""
    INSERT INTO account (aid, balance)
    VALUES (0, 1000), (1, 0)
    ON CONFLICT DO NOTHING;
""")
conn.commit()
cursor.close()
conn.close()

def reset_balances():
    conn, cursor = get_postgres_connection()
    cursor.execute("UPDATE account SET balance = 1000 WHERE aid = 0;")
    cursor.execute("UPDATE account SET balance = 0 WHERE aid = 1;")
    conn.commit()
    cursor.close()
    conn.close()

def get_balance(aid):
    conn, cursor = get_postgres_connection()
    cursor.execute("SELECT balance FROM account WHERE aid = %s;", (aid,))
    result = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return result

class TransactionThread(threading.Thread):
    def __init__(self, isolation_level, solution):
        super().__init__()
        self.isolation_level = isolation_level
        self.solution = solution

    def run(self):
        for _ in range(TRANSFER_COUNT):
            while True:
                try:
                    conn, cursor = get_postgres_connection()
                    cursor.execute(f"BEGIN TRANSACTION ISOLATION LEVEL {self.isolation_level};")
                    
                    if self.solution == "a":
                        # Solution (a): atomic updates
                        cursor.execute("UPDATE account SET balance = balance - 1 WHERE aid = 0;")
                        cursor.execute("UPDATE account SET balance = balance + 1 WHERE aid = 1;")
                    elif self.solution == "b":
                        # Solution (b): SELECT + manual update
                        cursor.execute("SELECT balance FROM account WHERE aid = 0;")
                        balance_0 = cursor.fetchone()[0]
                        cursor.execute("SELECT balance FROM account WHERE aid = 1;")
                        balance_1 = cursor.fetchone()[0]
                        cursor.execute("UPDATE account SET balance = %s WHERE aid = 0;", (balance_0 - 1,))
                        cursor.execute("UPDATE account SET balance = %s WHERE aid = 1;", (balance_1 + 1,))
                    
                    conn.commit()
                    cursor.close()
                    conn.close()
                    break
                except Exception:
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    continue

def run_experiment(isolation_level, thread_count, solution):
    reset_balances()
    c1 = get_balance(0)

    threads = [TransactionThread(isolation_level, solution) for _ in range(thread_count)]
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    end = time.time()

    c2 = get_balance(0)
    correctness = (c1 - c2) / 100
    duration = end - start
    log_msg = f"Solution {solution.upper()} | Isolation: {isolation_level}, Threads: {thread_count}, Time: {duration:.2f}s, Correctness: {correctness:.2f}"
    print(log_msg)
    logging.info(log_msg)

def main():
    for solution in ['a', 'b']:
        for isolation in ['READ COMMITTED', 'SERIALIZABLE']:
            for threads in range(1, 6):
                run_experiment(isolation, threads, solution)

if __name__ == '__main__':
    main()
