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
    with open('/home/project/ETL_Banks_Data/etl_project_log.txt', 'a') as f:
        f.write(timestamp + ':' + message + '\n')

log_progress('Preliminaries complete. Initiating ETL process')


def extract(url, table_attribs):

def transform(df, csv_path):

def load_to_csv(df, output_path):

def load_to_db(df, sql_connection, table_name):

def run_query(query_statement, sql_connection):
