import streamlit as st
import pandas as pd
import numpy as np
import yaml
import pyodbc
from market_growth_analysis.etl.stagging import *
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import sys
import pandas as pd
import matplotlib.pyplot as plt

# sys.path.append('../conf') 
# Load the YAML file
with open('conf/global.yml', 'r') as f:
    columns = yaml.safe_load(f)

# Load the YAML file
with open('conf/local.yml', 'r') as f:
    credentials = yaml.safe_load(f)


# Get credentials
driver = credentials['warehouse_db']['driver']
server = credentials['warehouse_db']['server']
database = credentials['warehouse_db']['database']
trusted = credentials['warehouse_db']['trusted_connection']
user = credentials['warehouse_db']['user']
password = credentials['warehouse_db']['password']

# Create connection db
conn = pyodbc.connect('Driver={SQL Server};'
                      f'Server={server};'
                      f'Database={database};'
                      'Trusted_Connection=yes;')
cursor = conn.cursor()

# Get data
join_tables_query = '''
select *
from DIM_COMPANY dim
join FACT_BALANCE_SHEET bs on bs.ticker = dim.ticker
join FACT_CASH_FLOW_STATEMENT cfs on cfs.PK = bs.PK
join FACT_INCOME_STATEMENT fis on fis.PK = bs.PK
join FACT_PRICES fp on fp.PK = bs.PK
join FACT_RATIOS fr on fr.PK = bs.PK
'''
# join_tables_query = '''
# select *
# from DIM_COMPANY dim
# '''


data = pd.read_sql(join_tables_query, conn)
# drop duplicated columns
data = data.T.drop_duplicates().T
data['Date'] = pd.to_datetime(data['Date'], format="%Y/%m/%d").dt.strftime("%Y/%m/%d")


# Prepare data
company_data = data[['Date', 'ticker', 'Growth +1']].sort_values(by=['ticker', 'Date']).reset_index(drop=True)
categories_data = data[['ticker', 'industry', 'sector']].drop_duplicates()
st.write(company_data.head())

# Pivot the data
pivoted_data = company_data.pivot(index='ticker',columns='Date', values='Growth +1')
st.write(pivoted_data.head())


# Plot the data
plt.figure(figsize=(12, 6))  # Set the size of the plot
for column in pivoted_data.columns:
    plt.plot(pivoted_data[column], label=column)

plt.xlabel('Time')  # Set the x-axis label
plt.ylabel('Year Growth')  # Set the y-axis label
plt.title('Year Growth Evolution')  # Set the title of the plot
plt.legend()  # Show the legend
plt.xticks(rotation=45)  # Rotate x-axis tick labels for better readability
plt.tight_layout()  # Adjust the layout
plt.show()  # Display the plot
