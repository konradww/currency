# -*- coding: utf-8 -*-
import pyodbc
import json
import logging
import io
import requests
# convert STR to FLOAT
import ast
import datetime
import pandas as pd
from bs4 import BeautifulSoup

now = datetime.datetime.now()
data = ("%s-%s-%s" % (now.year, now.month, now.day))

logging.basicConfig(level=logging.DEBUG, format=' %(asctime)s - %(levelname)s - %(message)s')
# i added data to file JSON
# data = {'server' : 'localhost',
#         'database' : 'TEST',
#         'username' : 'sa',
#         'password' : '********'
#         }
# with io.open('data.json', 'w', encoding='utf8') as outfile:
#     outfile.write(json.dumps(data,
#                              sort_keys=False,
#                              ensure_ascii=False,
#                              indent=4))

with open('data.json') as data_file:
    data_loaded = json.load(data_file)

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};'
                      'SERVER=' + data_loaded['server']
                      + ';PORT=1443;DATABASE='
                      + data_loaded['database']
                      + ';UID=' + data_loaded['username']
                      + ';PWD=' + data_loaded['password'])
cursor = cnxn.cursor()

page = requests.get(data_loaded['website'])
soup = BeautifulSoup(page.content, 'html.parser')

def currency():
    dict = {}
    if soup.find_all('td')[35].get_text() == '1 EUR':
        dict[soup.find_all('td')[35].get_text()] = (soup.find_all('td')[36].get_text()).replace(",", ".")
    if soup.find_all('td')[37].get_text() == '1 USD':
        dict[soup.find_all('td')[37].get_text()] = (soup.find_all('td')[38].get_text()).replace(",", ".")
    if soup.find_all('td')[39].get_text() == '1 CHF':
        dict[soup.find_all('td')[39].get_text()] = (soup.find_all('td')[40].get_text()).replace(",", ".")
    if soup.find_all('td')[41].get_text() == '1 GBP':
        dict[soup.find_all('td')[41].get_text()] = (soup.find_all('td')[42].get_text()).replace(",", ".")
    return dict

# logging.info(currency())

# key - currency
# v - value
for k, v in currency().items():
    logging.debug(k, ast.literal_eval(v))
    question = ("BEGIN TRAN "
                "IF NOT EXISTS "
                "(SELECT id FROM dbo.currency with (updlock, rowlock, holdlock) where currency= '%s' and date='%s') "
                "INSERT INTO dbo.currency(currency, value, date) VALUES ('%s',%f,'%s') "
                "COMMIT") % (k, data, k, ast.literal_eval(v), data)

    cursor.execute(question)
    cursor.commit()
# CREATE TABLE
# dbo.currency (id int IDENTITY(1,1)
# PRIMARY KEY,currency VARCHAR(20) NOT NULL, value INT NOT NULL, date VARCHAR(20) NOT NULL)
# ALTER TABLE dbo.currency ALTER COLUMN value FLOAT
# delete from dbo.currency
# select * from dbo.currency

# cursor.execute("SELECT * FROM dbo.currency ")
# for row in cursor:
#     print('%r' % (row))

question = "SELECT currency, date, avg(value) avg FROM dbo.currency GROUP BY ROLLUP (currency,date)"
df = pd.read_sql(question, cnxn)
print(df)
