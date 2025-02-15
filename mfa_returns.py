# Import Libraries
import configparser
import warnings

import mysql.connector as sql
import pandas as pd
from cryptography.fernet import Fernet

warnings.simplefilter(action="ignore")

config = configparser.ConfigParser()
config.read('config.ini')

key = config['mysqldb']['key']
cipher_suit = Fernet(key.encode('utf-8'))
# cipher_text = cipher_suit.encrypt(config['mysqldb']['password'].encode())
uncipher_pwd = (cipher_suit.decrypt((config['mysqldb']['password']).encode('utf-8'))).decode()

# password = config['mysqldb']['password']
# DataBase Connection and Fetching Data from Server with Security

conn = sql.Connect(host = 'localhost', port = config['mysqldb']['port'], user = config['mysqldb']['user'], \
                   password = uncipher_pwd, database = config['mysqldb']['database'])

cursor = conn.cursor()
query = f"SELECT * from mf_scheme_dg"
cursor.execute(query)
db = cursor.fetchall()
print("DataBase Connection Successful ... :)")
    
# Create a DataFrame
mf_db = pd.read_sql(query, con = conn) # type: ignore
mf = mf_db

# Settings DataFrame Display
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1500)

# Cleansing DataFrame
mf.dropna(axis=0, inplace=True)
mf.drop_duplicates(inplace=True)

print("Dataframe Shape : ", mf.shape)
print("Unique no of Scheme Name : ", mf['Scheme_Name'].nunique())
print("Unique no of Scheme Code : ", mf['Scheme_Code'].nunique())

mf['Net_Asset_Value'] = pd.to_numeric(mf['Net_Asset_Value'], errors='coerce').fillna(0, downcast='infer') # type: ignore
mf['Date'] = pd.to_datetime(mf['Date'], format='mixed')

all_fund_id = mf.loc[(mf['Date'] == (pd.Timestamp(max(mf['Date']))))].Scheme_Code.unique()

final_df = pd.DataFrame()

count = 0

for fund in all_fund_id:

    count += 1

    df_mf = mf[mf['Scheme_Code'].str.contains(fund)]
    print(f"Fetching Data for {count} : {df_mf.Scheme_Name.unique()}")

    start_date = pd.Timestamp(max(df_mf['Date']))
    df_mf['Date'] = pd.to_datetime(df_mf['Date'], errors='coerce')
    df_mf = df_mf[df_mf['Date'].notnull()] # type: ignore
    if df_mf.empty:
        continue
    start_date = df_mf['Date'].max()

    # Additional check: if start_date is still NaT, skip this fund.
    if pd.isnull(start_date):
        continue

    # Calculate end_date by subtracting 2200 days from start_date.
    end_date = start_date - pd.Timedelta(days=2200)

    # Debug prints (optional)
    print("Start Date:", start_date)
    print("End Date:", end_date)

    # if df_mf.empty or df_mf['Date'].isna().all():  # Check if DataFrame is empty or all dates are NaT
    #     continue
    # start_date = df_mf['Date'].dropna().max()  # Get the max valid date
    # if pd.isnull(start_date):  # Extra safety check
    #     continue
    # end_date = start_date - pd.DateOffset(days=2200)  # Use DateOffset instead of Timedelta for better handling

    # print(start_date, end_date)

    df_mf = df_mf.loc[(df_mf['Date'] <= start_date) & (df_mf['Date'] >= end_date), :]
    # print(max(df_mf['Date']), min(df_mf['Date']))

    df_mf.sort_values(['Date'], ascending=[0], inplace=True) # type: ignore

    df_mf['1M_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-21)
    df_mf['1M_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['1M_Old_NAV']) ** (12/1)) - 1) * 100
    df_mf['3M_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-63)
    df_mf['3M_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['3M_Old_NAV']) ** (12/3)) - 1) * 100
    df_mf['6M_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-126)
    df_mf['6M_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['6M_Old_NAV']) ** (12/6)) - 1) * 100
    df_mf['1Y_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-252)
    df_mf['1Y_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['1Y_Old_NAV']) ** (12/12)) - 1) * 100
    df_mf['3Y_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-756)
    df_mf['3Y_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['3Y_Old_NAV']) ** (12/36)) - 1) * 100
    df_mf['5Y_Old_NAV'] = df_mf['Net_Asset_Value'].shift(-1260)
    df_mf['5Y_Return_in(%)'] = (((df_mf['Net_Asset_Value'] / df_mf['5Y_Old_NAV']) ** (12/60)) - 1) * 100

    df = df_mf.iloc[:1]
    final_df = pd.concat([final_df, df], ignore_index=True) # type: ignore

print(final_df) # type: ignore
final_df.to_excel("Master_Return_File.xlsx")