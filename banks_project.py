"""
SCENARIO: 
You have been hired as a data engineer by research organization. Your boss has asked you to create a code that can be used to compile the list of the top 10 largest banks in the world ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP, EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. The processed information table is to be saved locally in a CSV format and as a database table.
Your job is to create an automated system to generate this information so that the same can be executed in every financial quarter to prepare the report.
"""

# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
from tabulate import tabulate

# Initialization of variables
url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billions']
table_name = 'Largest_banks'
db_name = '/home/aben/Documents/IBM/Course3/W2/E2_Final/Banks.db'
output_path = '/home/aben/Documents/IBM/Course3/W2/E2_Final/Largest_banks_data.csv'
csv_path = '/home/aben/Documents/IBM/Course3/W2/E2_Final/exchange_rate.csv'
log_file = '/home/aben/Documents/IBM/Course3/W2/E2_Final/code_log.txt'

# Task 1
def log_progress(message):
    ''' 
    This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing
    '''
    timestamp_format = '%d-%B-%Y-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    with open(log_file, "a") as file:
        file.write(timestamp + ' : ' + message + '\n\n')

# Task 2
def extract(url, table_attribs):
    ''' 
    This function aims to extract the required information from the website and save it to a dataframe. The function returns the dataframe for further processing. 
    '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows: 
        col = row.find_all('td')
        #bank_name = col[1].find_all('a')[1]['title']
        #market_cap = float(col[2].contents[0][:-1])
        if len(col) != 0 :
            data_dict = {
                "Name": col[1].contents[2],
                "MC_USD_Billions": col[2].contents[0]
                }
             
            df1 = pd.DataFrame(data_dict, index=[0])
            # Remove leading and trailing whitespaces
            df1 = df1.replace({"^\s*|\s*$": ""}, regex=True)
            df = pd.concat([df, df1], ignore_index=True)
            # Typecast the values to float
            df['MC_USD_Billions'] = pd.to_numeric(df['MC_USD_Billions'], errors='coerce')
            pd.set_option('display.colheader_justify', 'center')

    return df

# Task 3
def transform(df, csv_path):
    ''' 
    This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies
    '''
    df2 = pd.read_csv(csv_path)

    exchange_rate = df2.set_index('Currency').to_dict()['Rate']

    #df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 0) for x in df['MC_USD_Billion']]
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'],2) for x in df['MC_USD_Billions']]
    
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'],2) for x in df['MC_USD_Billions']]
    
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'],2) for x in df['MC_USD_Billions']]

    return df

# Task 4
def load_to_csv(df, output_path):
    ''' 
    This function saves the final data frame as a CSV file in the provided path. Function returns nothing.
    '''
    df.to_csv(output_path)

# Task 5
def load_to_db(df, sql_connection, table_name):
    ''' 
    This function saves the final data frame to a database table with the provided name. Function returns nothing.
    '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Task 6
def run_query(query_statement, sql_connection):
    ''' 
    This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. 
    '''
    print('\n\n', query_statement, ':')
    query_output = pd.read_sql(query_statement, sql_connection)
    print(tabulate(query_output, headers='keys', tablefmt='heavy_outline'))


"""
Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.
"""

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
print('\n\n')
print(tabulate(df, headers='keys', tablefmt='github'))
print('\n\n')
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df, csv_path)
print(tabulate(df, headers='keys', tablefmt='github'))
print('\n\n')
print(df['MC_EUR_Billion'][4])
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table. Executing queries')

query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete')
log_progress('Server Connection closed')


