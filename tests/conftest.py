import pytest
import psycopg2
import subprocess
import time


@pytest.fixture(scope="session", autouse=True)
def prepare_env():
    subprocess.run(["docker-compose", "up", "-d"], check=True)
    time.sleep(10)
    try:
        yield
    finally:
        subprocess.run(["docker-compose", "down"], check=True)


def create_conn(host, dbname):
    return psycopg2.connect(
        host=host, port=5432, dbname=dbname, user="user", password="password"
    )


def clear_db(host: str):
    conn = create_conn(host, "postgres")
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS db1;")
    cursor.execute("CREATE DATABASE db1;")
    cursor.close()
    conn.close()


def run_sql_script(cursor, script_path):
    with open(script_path, "r") as file:
        sql_script = file.read()
    cursor.execute(sql_script)


def setup_db(host, init_script=None):
    clear_db(host)
    conn = create_conn(host, "db1")
    conn.autocommit = True
    cursor = conn.cursor()
    if init_script:
        run_sql_script(cursor, init_script)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


@pytest.fixture
def postgres_prod():
    host = "172.16.238.10"
    init_script = "db_src_init.sql"
    yield from setup_db(host, init_script)


@pytest.fixture
def postgres_test1():
    host = "172.16.238.11"
    yield from setup_db(host)


@pytest.fixture
def postgres_test2():
    host = "172.16.238.12"
    yield from setup_db(host)
