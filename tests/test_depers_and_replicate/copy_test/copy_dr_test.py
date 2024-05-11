import pytest


def test_depers_and_replicate(
    postgres_prod, postgres_test1, postgres_test2, run_script
):

    postgres_prod.execute("SELECT * FROM workers;")
    prod_data = postgres_prod.fetchall()
    assert prod_data is not None
    assert len(prod_data) == 18

    run_script("depers_and_replicate.py", "settings.json")

    postgres_test1.execute("SELECT * FROM _transfer_workers;")
    test_db1_data = postgres_test1.fetchall()
    assert test_db1_data == prod_data

    postgres_test2.execute("SELECT * FROM _transfer_workers;")
    test_db2_data = postgres_test2.fetchall()
    assert test_db2_data == prod_data
