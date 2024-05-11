import pytest
import subprocess


@pytest.mark.parametrize("a", [1])
def test_depers_and_replicate(postgres_prod, postgres_test1, postgres_test2, a):

    postgres_prod.execute("SELECT * FROM workers;")
    prod_data = postgres_prod.fetchall()
    assert prod_data is not None
    assert len(prod_data) == 18

    subprocess.run(
        ["python3", "../depers_and_replicate.py", "../example/copy_settings.json"],
        check=True,
    )

    postgres_test1.execute("SELECT * FROM _transfer_workers;")
    test_db1_data = postgres_test1.fetchall()
    assert test_db1_data == prod_data

    postgres_test2.execute("SELECT * FROM _transfer_workers;")
    test_db2_data = postgres_test2.fetchall()
    assert test_db2_data == prod_data


@pytest.mark.parametrize("a", [1])
def test_depers_only(postgres_prod, a):

    postgres_prod.execute("SELECT * FROM workers;")
    prod_data = postgres_prod.fetchall()
    assert prod_data is not None
    assert len(prod_data) == 18

    subprocess.run(
        ["python3", "../depers_only.py", "../example/copy_settings.json"], check=True
    )

    postgres_prod.execute("SELECT * FROM _transfer_workers;")
    processed_prod_data = postgres_prod.fetchall()
    assert processed_prod_data == prod_data
