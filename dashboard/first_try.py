import streamlit as st
st. set_page_config(layout="wide")
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

@st.cache_data()  # Add allow_output_mutation=True if working with mutable objects like a database connection
def fetch_data_from_database():
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
    data = pd.read_sql(join_tables_query, conn)

    return data


# Get credentials
driver = credentials['warehouse_db']['driver']
server = credentials['warehouse_db']['server']
database = credentials['warehouse_db']['database']
trusted = credentials['warehouse_db']['trusted_connection']
user = credentials['warehouse_db']['user']
password = credentials['warehouse_db']['password']

col11, col12, col13, col14 = st.columns(4)

# ========================= GET DATA =========================
data = fetch_data_from_database()
data = data.T.drop_duplicates().T
data['Date'] = pd.to_datetime(data['Date'], format="%Y/%m/%d").dt.strftime("%Y/%m/%d")

data_filtered = data[['Date', 'country', 'sector', 'industry','ticker', 'Growth +1']]
# ========================= FILTERS =========================
# Country
with col11:
    country_options = tuple(data_filtered['country'].unique())
    country_selected = st.selectbox('Select country', country_options)
    data_country = data_filtered[data_filtered['country'] == country_selected]
# Sector
with col12:
    sector_options = tuple(data_country['sector'].unique())
    sector_selected = st.selectbox('Select Sector', sector_options)
    data_sector = data_country[data_country['sector'] == sector_selected]
# Industry
with col13:
    industry_options = tuple(data_sector['industry'].unique())
    industry_selected = st.selectbox('Select Industry', industry_options)
    data_industry = data_sector[data_sector['industry'] == industry_selected]
# Company
with col14:
    company_options = tuple(data_industry['ticker'].unique())
    company_selected = st.selectbox('Select Company', company_options)
    data_company = data_industry[data_industry['ticker'] == company_selected]


# ========================= PREPARE DATA =========================
# all_data = data_filtered[['Date','country', 'Growth +1']].groupby(by=['Date','country']).median().sort_values(by=['Date', 'country']).reset_index()
country_data = data_country[['Date','country', 'Growth +1']].groupby(by=['Date','country']).median().sort_values(by=['Date', 'country']).reset_index()
sector_data = data_sector[['Date','sector', 'Growth +1']].groupby(by=['Date','sector']).median().sort_values(by=['Date', 'sector']).reset_index()
industry_data = data_industry[['Date', 'industry', 'Growth +1']].groupby(by=['Date','industry']).median().sort_values(by=['Date', 'industry']).reset_index()
company_data = data_company[['Date', 'ticker', 'Growth +1']].sort_values(by=['Date', 'ticker']).reset_index(drop=True)

# categories
# categories_data = data_industry[['ticker', 'industry', 'sector']].drop_duplicates()


# Pivot country data
pivoted_country_data = country_data.pivot(index='Date',columns='country', values='Growth +1')
pivoted_country_data.index= pd.to_datetime(pivoted_country_data.index, format="%Y/%m/%d")
# Pivot sector data
pivoted_sector_data = sector_data.pivot(index='Date',columns='sector', values='Growth +1')
pivoted_sector_data.index= pd.to_datetime(pivoted_sector_data.index, format="%Y/%m/%d")
# Pivot industry data
pivoted_industry_data = industry_data.pivot(index='Date',columns='industry', values='Growth +1')
pivoted_industry_data.index= pd.to_datetime(pivoted_industry_data.index, format="%Y/%m/%d")
# Pivot ticker data
pivoted_company_data = company_data.pivot(index='Date',columns='ticker', values='Growth +1')
pivoted_company_data.index= pd.to_datetime(pivoted_company_data.index, format="%Y/%m/%d")

# ========================= PLOT DATA =========================
# Plot each ticker as a separate line

def plot_line_plot(pivoted_data):
    plt.figure(figsize=(10, 6))  # Adjust the figure size as needed
    for column in pivoted_data.columns:
        filtered_data = pivoted_data.dropna(subset=[column])
        plt.plot(filtered_data.index, filtered_data[column], marker='o', label=column)


    # Customize the plot
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.title('Line Plot for Tickers')
    plt.grid(True)
    plt.legend()

    # Rotate the x-axis labels
    plt.xticks(rotation=45)

    # Display the plot using Streamlit
    st.set_option('deprecation.showPyplotGlobalUse', False) # Avoid Warning
    st.pyplot()

    return

col21, col22, col23, col24 = st.columns([0.25, 0.25, 0.25, 0.25]) 
# Country
with col21:
    plot_line_plot(pivoted_country_data)
# Sector
with col22:
    plot_line_plot(pivoted_sector_data)
# Industry
with col23:
    plot_line_plot(pivoted_industry_data)
# Company
with col24:
    plot_line_plot(pivoted_company_data)


