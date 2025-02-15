import re
import warnings
from datetime import date, datetime, timedelta

import mysql.connector as sql
import numpy as np
import pandas as pd
from pymysql import Connection

warnings.simplefilter(action= 'ignore')
from mysql.connector import Error

from cryptography.fernet import Fernet
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

key = config['mysqldb']['key']
cipher_suit = Fernet(key.encode('utf-8'))
# cipher_text = cipher_suit.encrypt((config['mysqldb']['password']).encode())
uncipher_pwd = (cipher_suit.decrypt((config['mysqldb']['password']).encode('utf-8'))).decode()

# Step 1: Establish a Connection
conn = sql.Connect(
    host = 'localhost',
    port = config['mysqldb']['port'],
    user = config['mysqldb']['user'],
    password = uncipher_pwd,
    database = config['mysqldb']['database']
)
if conn.is_connected():
    print('Connected to MySQL database...\n')

# Step 2: Read Datas from Database and Fetch All Datas for inserting within DataFrame
read_cursor = conn.cursor()
query = "SELECT Scheme_Code, Scheme_Name, Net_Asset_Value, Date from mf_scheme "
read_cursor.execute(query)
db = read_cursor.fetchall()
print("Successfully fetch the data from database...\n")

df = pd.read_sql(query, con = conn) # type: ignore
print(df.head(1))
print(df.tail(1))
print()

# splitting dataframe by row index
df_1  = df.iloc[:500000, :]
df_2  = df.iloc[500000:1000000, :]
df_3  = df.iloc[1000000:1500000, :]
df_4  = df.iloc[1500000:2000000, :]
df_5  = df.iloc[2000000:2500000, :]
df_6  = df.iloc[2500000:3000000, :]
df_7  = df.iloc[3000000:3500000, :]
df_8  = df.iloc[3500000:4000000, :]
df_9  = df.iloc[4000000:4500000, :]
df_10 = df.iloc[4500000:5000000, :]
df_11 = df.iloc[5000000:5500000, :]
df_12 = df.iloc[5500000:6000000, :]
df_13 = df.iloc[6000000:6500000, :]
df_14 = df.iloc[6500000:7000000, :]
df_15 = df.iloc[7000000:7500000, :]
df_16 = df.iloc[7500000:8000000, :]
df_17 = df.iloc[8000000:8500000, :]
df_18 = df.iloc[8500000:9000000, :]
df_19 = df.iloc[9000000:, :]

chunks = [df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15, df_16, df_17, df_18, df_19]
print("Shape of new dataframes - {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}".format(df_1.shape, df_2.shape, df_3.shape, df_4.shape, df_5.shape, df_6.shape, df_7.shape, df_8.shape, df_9.shape, df_10.shape, df_11.shape, df_12.shape, df_13.shape, df_14.shape, df_15.shape, df_16.shape, df_17.shape, df_18.shape, df_19.shape ))
print()
concat_chunks = []

for chunk in chunks:

    chunk.insert(0, "Scheme_Catagory", np.NaN)
    chunk.insert(0, "Mutual_Fund_Name", np.NaN)
    chunk = chunk.reset_index()

    for i in range(0, len(chunk['Scheme_Code'])):
        if len(str(chunk['Scheme_Code'].iloc[i])) > 8:
            if chunk['Scheme_Code'].iloc[i].startswith('Open Ended Schemes'):
                chunk.loc[[i], ['Scheme_Catagory','Scheme_Code']] = chunk.loc[:i, ['Scheme_Catagory','Scheme_Code']].shift(-1,axis=1)
            else:
                if re.search("Mutual Fund", chunk['Scheme_Code'].iloc[i]):
                    chunk.loc[[i], ['Mutual_Fund_Name','Scheme_Code']] = chunk.loc[:i, ['Mutual_Fund_Name','Scheme_Code']].shift(-1,axis=1)
        else:
            continue
    
    for i in range(0, len(chunk)-1):
        if pd.isnull(chunk.loc[i, 'Mutual_Fund_Name']) == False and pd.isnull(chunk.loc[i+1, 'Mutual_Fund_Name']) == True:
            chunk.loc[i+1, 'Mutual_Fund_Name'] = chunk.loc[i, 'Mutual_Fund_Name'] # type: ignore
        elif pd.isnull(chunk.loc[i, 'Mutual_Fund_Name']) == True and pd.isnull(chunk.loc[i+1, 'Mutual_Fund_Name']) == False:
            chunk.loc[i, 'Mutual_Fund_Name'] = chunk.loc[i+1, 'Mutual_Fund_Name'] # type: ignore
        elif pd.isnull(chunk.loc[i, 'Mutual_Fund_Name']) == False and pd.isnull(chunk.loc[i+1, 'Mutual_Fund_Name'] == False):
            if chunk.loc[i, 'Mutual_Fund_Name'] != chunk.loc[i+1, 'Mutual_Fund_Name']:
                chunk.loc[i+2, 'Mutual_Fund_Name'] = chunk.loc[i+1, 'Mutual_Fund_Name'] # type: ignore
            else:
                continue
    
    for i in range(0, len(chunk)-1):
        if pd.isnull(chunk.loc[i, 'Scheme_Catagory']) == False and pd.isnull(chunk.loc[i+1, 'Scheme_Catagory']) == True:
            chunk.loc[i+1, 'Scheme_Catagory'] = chunk.loc[i, 'Scheme_Catagory'] # type: ignore
        elif pd.isnull(chunk.loc[i, 'Scheme_Catagory']) == True and pd.isnull(chunk.loc[i+1, 'Scheme_Catagory']) == False:
            chunk.loc[i, 'Scheme_Catagory'] = chunk.loc[i+1, 'Scheme_Catagory'] # type: ignore
        elif pd.isnull(chunk.loc[i, 'Scheme_Catagory']) == False and pd.isnull(chunk.loc[i+1, 'Scheme_Catagory'] == False):
            if chunk.loc[i, 'Scheme_Catagory'] != chunk.loc[i+1, 'Scheme_Catagory']:
                chunk.loc[i+2, 'Scheme_Catagory'] = chunk.loc[i+1, 'Scheme_Catagory'] # type: ignore
            else:
                continue

    chunk.dropna(axis=0, inplace=True)
    chunk.drop_duplicates(inplace=True)    

    chunk_d = chunk[chunk['Scheme_Name'].str.contains('Direct', na=False)]
    chunk_dg = chunk_d[chunk_d['Scheme_Name'].str.contains('Growth', na= False)]
    concat_chunks.append(chunk_dg)
    print(concat_chunks)

chunk_df = pd.concat(concat_chunks)
chunk_df = chunk_df.reset_index(drop=True)
chunk_df = chunk_df.drop('index', axis=1)
print(chunk_df.shape)
print(chunk_df.head(1))
print(chunk_df.tail(1))

# For writing data from DataFrame to Database
# Step 1: Establish a Connection
try:
    if conn.is_connected():
        print('Connected to MySQL database')

        # Step 2: Convert Dataframe to SQL Insert Statements
        dataframe = chunk_df
        table_name = 'mf_scheme_dg'
        write_cursor = conn.cursor()
        cols = ",".join([str(i) for i in chunk_df.columns.tolist()])

        for i,row in dataframe.iterrows():
            sql_query = f"INSERT INTO {table_name} (" + cols + ") VALUES (" + "%s," *(len(row)-1) + "%s)"
            write_cursor.execute(sql_query, tuple(row))

    conn.commit()
    print(f" Write Successful.{chunk_df.shape}")
except Error as e:
    print(f'Error connecting to MySQL database: {e}')

# Step 3: Close the Connection
conn.close()