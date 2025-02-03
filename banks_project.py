import pandas as pd
from bs4 import BeautifulSoup
import requests
import sqlite3
import numpy as np 
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%m-%d %H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('/home/project/ETL_Banks_Data/code_log.txt', 'a') as f:
        f.write(timestamp + ':' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')

def extract(url, table_attribs):
    response = requests.get(url).text
    soup = BeautifulSoup(response, 'html.parser')
    data_frame = pd.DataFrame(columns = table_attribs)
    table_bodies = soup.find_all('tbody')
    rows = table_bodies[0].find_all('tr')[1:] # skips header

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Rank': col[0].text.strip(),
                'Bank_Name': col[1].text.strip(),
                'Market_Cap': float(col[2].text.strip())
            }
            df1 = pd.DataFrame(data_dict, index = [0]) # index set to zero becuse it is a single row temp data frame and first row will always have index 0
            data_frame = pd.concat([data_frame, df1] , ignore_index = True) # here we ingnore the index so above set value is ignored
    
    return data_frame

URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank', 'Bank_Name', 'Market_Cap']
data_frame  = extract(URL, table_attribs)
print(data_frame)
log_progress('Data extraction complete. Initiating Transformation process')


def transform(data_frame):
    exchange_rate_df = pd.read_csv('exchange_rate.csv')
    exchange_rate_dict = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    data_frame['MC_EUR_Billion'] = [np.round(x * exchange_rate_dict['EUR'],2) for x in data_frame['Market_Cap']]
    data_frame['MC_GBP_Billion'] = [np.round(x * exchange_rate_dict['GBP'],2) for x in data_frame['Market_Cap']]
    data_frame['MC_INR_Billion'] = [np.round(x * exchange_rate_dict['INR'],2) for x in data_frame['Market_Cap']]

    return data_frame

data_frame = transform(data_frame)
print('\n')
print(data_frame)
log_progress('Data transformation complete. Initiating loading process')


output_path = '/home/project/ETL_Banks_Data/Largest_Banks_By_MC.csv'

def load_to_csv(data_frame, output_path):
    data_frame.to_csv(output_path)

load_to_csv(data_frame, output_path)
log_progress('Data Saved to CSV file')


db_name = 'Banks.db'
table_name = 'Largest_Banks'
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)

load_to_db(data_frame, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

def run_query(query_statement, sql_connection):
    print(query_statement)
    output = pd.read_sql(query_statement, sql_connection)
    print(output)

query_statement = f'Select * From Largest_Banks'
run_query(query_statement, sql_connection)

query_statement = f"select avg(MC_GBP_Billion) from Largest_Banks"
run_query(query_statement, sql_connection)

query_statement = f" select Bank_Name from Largest_Banks Limit 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()
log_progress('Server Connection Closed')

