import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import xlrd
import re

def scrape(url):
    r = requests.get(url)
    raw_html = r.content
    soup = BeautifulSoup(raw_html, 'html.parser')
    soup3 = soup.findAll(href=re.compile("-full.xlsx"))
    for link in soup3:
      msg = link.get('href')
      SS = "https://www.health.gov.au/internet/main/publishing.nsf/Content/", msg
      msg1 = ("".join(SS))
    return(msg1)
stoma_f = scrape("https://www.health.gov.au/internet/main/publishing.nsf/Content/health-stoma-schedule-index.htm")
df = pd.read_excel(stoma_f)  #### testing function in pandas

db = sqlite3.connect(
    database='db_products.sqlite',
    detect_types=sqlite3.PARSE_DECLTYPES
)

db.row_factory = sqlite3.Row

cursor = db.cursor()

cursor.execute("drop table if exists tbl_products")

cursor.execute("""create table tbl_products (
    ID integer primary key,
    GroupID varchar,
    SASCode varchar,
    CompanyCode varchar,
    BrandName varchar,
    ProductDescription varchar,
    MaximumQty varchar,
    PackSize numeric,
    PackPrice numeric,
    PackPremium numeric
)""")
db.commit()

sql = """insert into tbl_products (ID,GroupID, SASCode, CompanyCode, BrandName, ProductDescription, MaximumQty, PackSize, PackPrice, PackPremium) 
values (?,?,?,?,?,?,?,?,?,?)"""

for index, row in df.iterrows():
    cursor.execute(sql,(index, row['Group ID'], row['SAS Code'], row['Company Code'], row['Brand Name'], row['Product Description'], row['Pack Size'], row['Maximum Qty\nMonthly (m)\nAnnual (a)'], row['Pack Price'], row['Pack Premium']))

db.commit()

sql = "select * from tbl_products limit 20"
records = cursor.execute(sql).fetchall()

for record in records:
    print(dict(record))
