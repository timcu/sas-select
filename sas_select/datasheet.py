# excel to sql database

from bs4 import BeautifulSoup
from urllib.request import urlopen
import pandas as pd
import datetime
from . import db

COLUMN_NAMES = ('Group ID',
                'SAS Code',
                'Company Code',
                'Brand Name',
                'Product Description',
                'Pack Size',
                'Maximum Qty\nMonthly (m)\nAnnual (a)',
                'Pack Price',
                'Pack Premium')


def init_db():
    base_url_str = "https://www.health.gov.au/internet/main/publishing.nsf/Content/"
    page_url_str = "health-stoma-schedule-index.htm"

    page = urlopen(base_url_str + page_url_str)

    soup = BeautifulSoup(page, features="html.parser")

    div_read = soup.find(id='read')

    lst_a = div_read.find_all("a")
    if len(lst_a) == 0:
        raise IOError("Could not find any hyperlinks on SAS Utilisition web page " + base_url_str + page_url_str)
    xl_url_str = None
    for a in lst_a:
        href = a.get('href')
        if 'xls' in href:
            xl_url_str = base_url_str + href
            break

    if xl_url_str is None:
        raise IOError("Could not find url for xls for any year between 2012 and " + str(datetime.date.today().year))

    headers = pd.read_excel(xl_url_str, nrows=0).columns
    if len(COLUMN_NAMES) != len(headers):
        raise ValueError("Wrong number of columns", len(COLUMN_NAMES), len(headers))

    for expected, read in zip(COLUMN_NAMES, headers):
        if expected != read:
            raise ValueError("Discrepancy in column names", expected, read)

    df = pd.read_excel(xl_url_str, skiprows=0)

    sas_db = db.get_db()

    cursor = sas_db.cursor()

    cursor.execute("drop table if exists tbl_products")

    cursor.execute("""create table tbl_products (
        ID integer primary key,
        GroupID varchar,
        SASCode varchar,
        CompanyCode varchar,
        BrandName varchar,
        ProductDescription varchar,
        PackSize numeric,
        MaximumQty varchar,
        PackPrice numeric,
        PackPremium numeric
    )""")
    sas_db.commit()

    sql = """insert into tbl_products (ID,GroupID, SASCode, CompanyCode, BrandName, ProductDescription, PackSize, MaximumQty, PackPrice, PackPremium) 
    values (?,?,?,?,?,?,?,?,?,?)"""

    for index, row in df.iterrows():
        cursor.execute(sql,(index, row['Group ID'], row['SAS Code'], row['Company Code'], row['Brand Name'], row['Product Description'], row['Pack Size'], row['Maximum Qty\nMonthly (m)\nAnnual (a)'], row['Pack Price'], row['Pack Premium']))

    sas_db.commit()

    sql = "select * from tbl_products limit 20"
    records = cursor.execute(sql).fetchall()

    for record in records:
        print(dict(record))
