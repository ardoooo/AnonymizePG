import functools
import pytest
import psycopg2
import subprocess
import time
import os


def _find_file(start_dir, file_name, levels_up=2):
    current_dir = start_dir

    for _ in range(levels_up + 1):
        script_path = os.path.join(current_dir, file_name)
        if os.path.isfile(script_path):
            return script_path

        new_dir = os.path.dirname(current_dir)
        if new_dir == current_dir:
            break
        current_dir = new_dir

    return None


@pytest.fixture
def find_file(request):
    test_dir = os.path.dirname(str(request.fspath))
    yield functools.partial(_find_file, test_dir)


@pytest.fixture
def run_script(request):
    test_dir = os.path.dirname(request.fspath)

    def _run(script_name, config_name):
        script_path = _find_file(test_dir, script_name, 3)
        config_path = _find_file(test_dir, config_name)

        subprocess.run(
            ["python3", script_path, config_path],
            check=True,
            cwd=test_dir
        )

    return _run


@pytest.fixture(scope="session", autouse=True)
def prepare_env():
    subprocess.run(
        ["docker-compose", "-f", "env_preparation/docker-compose.yml", "up", "-d"],
        check=True,
    )
    time.sleep(10)
    try:
        yield
    finally:
        subprocess.run(
            [
                "docker-compose",
                "-f",
                "env_preparation/docker-compose.yml",
                "down",
                "-v",
            ],
            check=True,
        )
        pass


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


def run_sql_script(cursor, script_path, test_dir=None):
    with open(_find_file(test_dir, script_path), "r") as file:
        sql_script = file.read()
    cursor.execute(sql_script)


def setup_db(host, init_script=None, test_dir=None):
    clear_db(host)
    conn = create_conn(host, "db1")
    conn.autocommit = True
    cursor = conn.cursor()
    if init_script:
        run_sql_script(cursor, init_script, test_dir)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


@pytest.fixture
def postgres_prod(request):
    host = "172.16.238.10"
    init_script = "db_src_init.sql"
    test_dir = os.path.dirname(str(request.fspath))
    yield from setup_db(host, init_script, test_dir)


@pytest.fixture
def postgres_test1():
    host = "172.16.238.11"
    yield from setup_db(host)


@pytest.fixture
def postgres_test2():
    host = "172.16.238.12"
    yield from setup_db(host)
