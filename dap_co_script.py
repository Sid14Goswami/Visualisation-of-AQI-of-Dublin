import json
import os
import urllib
import argparse
import numpy as np
import os
import shutil
from pathlib import Path, PureWindowsPath
from bs4 import BeautifulSoup
import requests
from pprintpp import pprint
import re
from geopy.distance import geodesic,great_circle
import datefinder
import pandas as pd
import datetime
from time import gmtime, strftime
from selenium.webdriver import Firefox
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
import time
import pymongo
from collections import defaultdict
from io import StringIO
import pandas as pd
import requests
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
import psycopg2
from pprint import pprint
import psycopg2.extras as extras
import csv
from io import StringIO
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import random
import string

def db_table_name(length):
    letters = string.ascii_lowercase
    result_str = ''.join(random.sample(letters, length))
    return result_str


def manupulate_df(csv_ref):

    df = pd.read_csv(StringIO(requests.get(csv_ref).text))
    df.dropna(axis=1, how='all',inplace=True)
    df.columns = df.iloc[3]
    new_df = df.iloc[4:,:]
    return new_df

def data_preprocessing(ip=None,port=None,extract_key=None):

    client = pymongo.MongoClient("localhost", 27017)
    # db = client.test
    db = client.no
    result = db.dap
    array = list(result.find())
    print("array",array)
    print("array",len(array))

    # ini_dict = array[1]

    # print(ini_dict.keys())

    processed_dfs = {}
    i = 0
    for dicts in array:

        for key,values in dicts.items():

            if extract_key in key and key != '_id':


                df1 = manupulate_df(values[0])
                df2 = manupulate_df(values[1])

                frames = [df1, df2]
                result = pd.concat(frames)
                i+=1

                result.columns = result.columns.fillna('type')

                extra_col = [ (str(rowss1),str(rowss2)) for rowss1,rowss2 in zip(result.iloc[0],result.iloc[1]) ]
                for new_col,old_col in zip(extra_col,result.columns):
                    new_col1,new_col2 = new_col
                    result.rename(columns={str(old_col):str(old_col)+'_'+str(new_col1)+'_'+str(new_col2)}, inplace=True)
                    # result[old_col].fillna(old_col+'_'+str(new_col1)+'_'+str(new_col2), inplace=True)

                new_df_final = result.iloc[2:,:]
                new_df_final = new_df_final.reset_index()
                new_df_final = new_df_final.loc[:, ~new_df_final.columns.str.contains('^Unnamed')]

                processed_dfs.update({extract_key:new_df_final})
                # new_df_final.to_csv(f'/home/palash/palash-projects/NCI_pros/DAP/test_csvs/{key}_new.csv')

    return processed_dfs


def scrape_data(main_url,extract_key):

    ## Scraping the data using Selenium

    ## Setting up local dependencies for Selenium Process 
    opts = Options()
    opts.set_headless()
    browser = Firefox('/usr/local/bin/')
    browser.get(main_url)
    search_form = browser.find_elements_by_name("q")

    ## Search with the appropriate query in the search bar to get the data 
    search_form[0].send_keys('Air Quality Monitoring Data Dublin City')
    search_form[0].submit()
    time.sleep(8)
    browser.get(browser.current_url)
    hh_page_source = browser.page_source
    soup = BeautifulSoup(hh_page_source, 'lxml')

    ## Finding the data in particular html class & getting the main url for data
    final_list = soup.body.find_all(class_="dataset-item package-theme-energy")
    main_url=[]
    for hhs in final_list:
        current_html_obj = hhs.find(class_="pull-left dataset-title-text")
        if "Air Quality Monitoring Data Dublin City" in current_html_obj.text:
            main_url.append('https://data.gov.ie/'+current_html_obj.find('a',href=True)['href'])
    ## Getting the individual link "https://data.gov.ie//dataset/air-quality-monitoring-data-dublin-city"
    indi_url = main_url[0]
    page = requests.get(indi_url)
    soup_new = BeautifulSoup(page.text, "lxml")
    data_href_list = soup_new.body.find_all(class_="resource-url-analytics btn btn-lg btn-primary btn-block btn-download",href=True)
    ini_dict = defaultdict(list)

    ## Getting the data for 2011 year, data is in semi structured form
    for hhs_link in data_href_list:
        if hhs_link['href'].endswith(".csv"):
            # key_value = os.path.basename(hhs_link['href']).split('.')[0]
            key_value = re.split('-2011|-2012',os.path.basename(hhs_link['href']))[0]
            ini_dict[key_value].append(hhs_link['href'])
    final_dict = {}
    final_dict['api_response'] = ini_dict
    print("ini_dict kkkk",ini_dict.keys())
    json_object = { k.replace('.', '-'): v for k, v in ini_dict.items() if extract_key in k }

    ## Inserting semi structured Data into Mongo DB
    client = pymongo.MongoClient("localhost", 27017)
    db = client.no
    # db = client.test
    result = db.dap.insert(json_object)
    # result = db.dap.insert(json_object)
    if result:
        print("Data Inserted into Mongo")

    browser.quit()


def postgres_connect(conn, df, table):
    """
    $ sudo -i -u postgres
    $ createdb dap_db
    $ psql
    $ postgres=# CREATE ROLE test_dap WITH LOGIN PASSWORD 'pass';
    $ postgres=# GRANT ALL PRIVILEGES ON DATABASE dapdatabase TO test_dap;
    Using cursor.executemany() to insert the dataframe
    # """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES(%%s,%%s,%%s)" % (table, cols)
    print(query)
    cursor = conn.cursor()
    try:
        cursor.executemany(query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_many() done")
    cursor.close()


def dump_data_to_postgres(conn, df, table):
    
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    name_Table  = table
    sqlCreateTable = "create table "+ name_Table + "()"
    cursor.execute(sqlCreateTable)
    conn.commit()
    df.to_csv('/home/palash/palash-projects/NCI_pros/DAP/viz_data/viz-co.csv')
    print("Data Dumped to SQL Successfully")
    cursor.close()

def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


if __name__ == "__main__":

    # Extracting data for 'dublin-city-council-co'
    extract_key = 'dublin-city-council-co'
    main_url = "https://data.gov.ie/"
    scrape_data(main_url,extract_key)

     ## Read the semi structured data from Mongo and perform necessary pre processing for the data
    processed_data = data_preprocessing(ip="localhost",port=27017,extract_key=extract_key)
    clean_processed_data = processed_data[extract_key]

    ## Storing the clean processed data into POSTgres SQl DB & Exporting as csv
    # conn = psycopg2.connect("dbname=dapdatabase user=DapUser host='localhost' password='pass'")
    conn = psycopg2.connect("dbname=DapUser user=postgres host='localhost' password='pass'")
    ini_table_name = db_table_name(4)
    dump_data_to_postgres(conn, clean_processed_data,ini_table_name)

    ## Reading cleaned data for Visualization

    engine = create_engine('postgresql://postgres:pass@localhost/DapUser')
    df = pd.read_excel('/home/palash/palash-projects/NCI_pros/DAP/viz_data/co.xlsx')
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    print(df.head())