from sas_select.datasheet import find_xl_url, BASE_URL_STR, populate_db_from_df
from sas_select.db import get_db
import os
import pandas


def test_find_xl_url():
    url = find_xl_url()
    assert "full.xls" in url
    assert BASE_URL_STR in url


def test_populate_db_from_df(app, client):
    with app.app_context():
        with open(os.path.join(os.path.dirname(__file__), "sas-schedule-test-full.xlsx"), "rb") as f:
            df = pandas.read_excel(f)
            populate_db_from_df(df)
        sas_db = get_db()
        count_products = sas_db.execute('select count(*) from tbl_products').fetchone()
        assert count_products['count(*)'] == 33  # there should be 33 records imported from spreadsheet
        entitlements = sas_db.execute('select MaximumQty from tbl_products').fetchall()
        for e in entitlements:
            assert "a" in e['MaximumQty'] or "m" in e['MaximumQty']
    response = client.post("/", data={'CompanyCode': '1', 'BrandName': ''})
    assert response.data.count(b'<tr>') <= 21  # search function should limit results to 20 (+ 1 header row)


