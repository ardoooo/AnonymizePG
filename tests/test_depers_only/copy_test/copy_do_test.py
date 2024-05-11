import pytest


def test_depers_and_replicate(postgres_prod, run_script):

    postgres_prod.execute("SELECT * FROM workers;")
    prod_data = postgres_prod.fetchall()
    assert prod_data is not None
    assert len(prod_data) == 8

    run_script("depers_only.py", "settings.json")

    postgres_prod.execute("SELECT * FROM _transfer_workers;")
    processed_prod_data = postgres_prod.fetchall()
    assert processed_prod_data == prod_data
