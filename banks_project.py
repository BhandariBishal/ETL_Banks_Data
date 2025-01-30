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
    rows = table_bodies[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {
                'Rank': col[0].contents[0],
                'Bank_Name': col[1].a.contents[0],
                'Market_Cap': float(col[2].contents[0].strip())
            }
            df1 = pd.DataFrame(data_dict, index = [0]) # index set to zero becuse it is a single row temp data frame and first row will always have index 0
            data_frame = pd.concat([data_frame, df1] , ignore_index = True) # here we ingnore the index so above set value is ignored
    
    return data_frame

URL = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Rank', 'Bank_Name', 'Market_Cap']
data_frame  = extract(URL, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')


# def transform(df, csv_path):

# def load_to_csv(df, output_path):

# def load_to_db(df, sql_connection, table_name):

# def run_query(query_statement, sql_connection):
