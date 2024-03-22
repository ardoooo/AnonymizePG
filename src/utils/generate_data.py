import psycopg2
from faker import Faker
import time
from src.utils.db_connector import DatabaseConnector

connector = DatabaseConnector()

src_conn = connector.get_src_connection()
src_conn.autocommit = False

cur = src_conn.cursor()
fake = Faker()

for _ in range(15):
    name = fake.name()
    salary = fake.random_number(digits=5)
    address = fake.address()

    cur.execute("INSERT INTO workers (name, salary, address) VALUES (%s, %s, %s)",
                (name, salary, address))

    src_conn.commit()

src_conn.close()
