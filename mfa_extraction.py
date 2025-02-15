import configparser
import csv
import time
import warnings
from configparser import ConfigParser
from datetime import date, datetime, timedelta

import mysql.connector as mysql
import pandas as pd
import pymysql
import requests
from bs4 import BeautifulSoup
from loguru import logger

warnings.simplefilter(action= 'ignore')
logger.add('SchemeDetails_mfa_extract.log', rotation='500 MB')

from cryptography.fernet import Fernet

config = configparser.ConfigParser()
config.read('config.ini')

key = config['mysqldb']['key']
cipher_suit = Fernet(key.encode('utf-8'))
# cipher_text = cipher_suit.encrypt(config['mysqldb']['password'].encode())
uncipher_pwd = (cipher_suit.decrypt((config['mysqldb']['password']).encode('utf-8'))).decode()

audit_query = "Insert into mf_audit (Run_ID	,MF_House, Start_Date, End_Date, Elapsed_Time, Record_Count, Status, Description) values (%s,%s,%s,%s,%s,%s,%s,%s)"

# Extracting The Mutual Fund House ID and name
source = requests.get(f'https://www.amfiindia.com/nav-history-download')
soup = BeautifulSoup(source.text, 'lxml')

mf_id = soup.find('select', {'id': 'NavDownMFName'})
options = mf_id.find_all("option") # type: ignore
# for each element in that list, pull out the "value" with text attribute
values = [op.get("value") for op in options]
values = values[2:]
# print(values)
texts = [op.text for op in options]
texts = texts[2:]
# print(texts)

# connect to database
conn = pymysql.connect(host='localhost',
                        user=config['mysqldb']['user'],
                        password=uncipher_pwd,
                        db=config['mysqldb']['database'],
                        # autocommit=True,
                        local_infile=1)
print(f"Connected to DB: {config['mysqldb']['database']}".format('localhost'))

# Defining The Data Extraction Window
frm_dt = (date.today() - timedelta(days=2200)).strftime('%d-%b-%Y')
to_dt = date.today().strftime('%d-%b-%Y')
start_time = time.time()

try:
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")

    for mfid in values:
        start = time.time()
        counter = 0
        status = ''
        dscrptn = ''
        try:
            logger.info(f"Extracting Data for : {mfid}")
            mf_type = '1'

            nav_url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=' + mfid \
                    + '&tp=' + mf_type + '&frmdt=' + frm_dt + '&todt=' + to_dt
            print(nav_url)
                
            try:
                print(nav_url)
                stockdata = pd.read_csv(nav_url)
            except Exception as e:
                print(f"No Data")
                status = 'No Data'
            else:
                mf_df = stockdata.iloc[0:, :]
                mf_df[['Scheme_Code', 'Scheme_Name', 'ISIN_Div_Payout_ISIN_Growth', 'ISIN_Div_Reinvestment', 'Net_Asset_Value', 'Repurchase_Price', 'Sale_Price',
                 'Date' ]] = mf_df['Scheme Code;Scheme Name;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Net Asset Value;Repurchase Price;Sale Price;Date'].str.split(';', expand=True)
                # print(mf_df)
                mf_df1 = mf_df.iloc[:,1:]
                  # print(mf_df1)
                    # print(type(mf_df1))

                    # create cursor
                data_cursor = conn.cursor()
                table_name = 'mf_scheme'

                cols = ",".join([str(i) for i in mf_df1.columns.tolist()])
                status = 'InProgress'
                logger.info(f" Writing Data For {mfid}")

                for i,row in mf_df1.iterrows():
                    sql = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," *(len(row)-1) + "%s)"
                    data_cursor.execute(sql, tuple(row))
                    counter = mf_df1.shape[0]

                conn.commit()
                logger.info(f" Write Successful ... {mf_df1.shape}")
                status = 'Successful'
                
        except Exception as E:
            print("INSIDE FOR LOOP, WHEN DATA EXTRACTION STARTING ... \n", E)
            dscrptn = E
            status = 'Failure'

        finally:
            end = time.time()
            elapsed_time = end - start
            audit_cursor = conn.cursor()
            values = (run_id, mfid, frm_dt, to_dt, elapsed_time, counter, status, dscrptn)
            audit_cursor.execute(audit_query, values)
            conn.commit()
        
    conn.close()
    logger.info("Connection closed .. !")
        
except Exception as ex:
    print("OUTSIDE OF FOR LOOP : \n", ex)
