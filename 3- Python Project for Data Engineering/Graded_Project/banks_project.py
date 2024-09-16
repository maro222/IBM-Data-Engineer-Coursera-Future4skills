import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.max_colwidth', 1000)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ':' + message + '\n')


def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {'Name': col[1].find_all('a')[1].contents[0],
                      'MC_USD_Billion': col[2].contents[0]}
            df_data = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df_data], ignore_index=True)
    return df



def transform(df, csv_path):
    dataframe = pd.read_csv(csv_path)
    dict = dataframe.set_index('Currency').to_dict()['Rate']
    GDP_list = df["MC_USD_Billion"].tolist()
    GDP_list = df["MC_USD_Billion"].astype(str).str.replace('\n', '').astype(float)
    df["MC_USD_Billion"] = GDP_list

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * dict['GBP'],2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * dict['EUR'],2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * dict['INR'],2)
    #print(df['MC_EUR_Billion'][4])
    print(df)
    return df


def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
   df.to_sql(table_name, sql_connection, if_exists = 'replace', index =False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)







output_csv_path = 'graded_project/Largest_banks_data.csv'
shared_csv_path = 'graded_project/exchange_rate.csv'
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attr = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
log_file = 'graded_project/code_log.txt'
log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attr)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, shared_csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_csv_path)
log_progress('Data saved to CSV file')

sql_conn = sqlite3.connect('graded_project/Banks.db')
log_progress('SQL Connection initiated')

load_to_db(df, sql_conn, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# run_query
query1 = 'SELECT * FROM Largest_banks'
query2 = 'SELECT AVG(MC_GBP_Billion) FROM Largest_banks'
query3 = 'SELECT Name from Largest_banks LIMIT 5'
run_query(query1, sql_conn)
run_query(query2, sql_conn)
run_query(query3, sql_conn)

log_progress('Process Complete')

sql_conn.close()
log_progress('Server Connection closed')































# import urllib.request
# url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
# file_name = url.rsplit('/')[-1]
# urllib.request.urlretrieve(url, file_name)