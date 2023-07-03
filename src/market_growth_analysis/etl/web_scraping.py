from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import json
from datetime import datetime
import logging
from pathlib import Path 
import concurrent.futures
import tqdm
import yfinance as yf
import pandas as pd
import os
from os.path import join
from datetime import datetime
from pathlib import Path 
from yahoofinancials import YahooFinancials


############################### FINANCIAL SHEETS ###############################

def parser(URL):

    # Load the html and get the json with the info
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, "html.parser")
    pattern = re.compile(r"var originalData = \[(.*?)\]", re.MULTILINE | re.DOTALL)
    scriptParse = re.search(pattern, str(soup))
    block = scriptParse.groups()[0]
    res = json.loads("".join(["[", block, "]"]))

    # Get the info inside the json
    format_yyyymmdd = "%Y-%m-%d"
    financial_df = pd.DataFrame()

    for financial in res:
        temporal_df = pd.DataFrame()
        dict_parsed = dict()
        
        # Take the name of the financial metric
        link = BeautifulSoup(str(financial['field_name']), "html.parser")
        name = link.get_text()
        
        # Take the values of these financial metric
        for key, value in zip(financial.keys(), financial.items()):
            try: 
                date = datetime.strptime(key, format_yyyymmdd)
                dict_parsed[value[0]] = value[1]

            except:
                continue
                
        temporal_df = pd.DataFrame.from_dict(dict_parsed, orient='index', columns=[name])
        financial_df = financial_df.join(temporal_df, how='outer')

    return financial_df.reset_index(names='Date')



def scrape_company_data(company_key, company_url, industry, sector, company_full_name, company_country, financial_sheet):
    try:
        # Get the data
        URL = company_url
        data = parser(URL)
        data['ticker'] = company_key
        data['industry'] = industry
        data['sector'] = sector
        data['company_full_name'] = company_full_name
        data['country'] = company_country

        return data

    except Exception as e:
        error_message = f"Error occurred for {company_key}: {str(e)}"
        logging.error(error_message)

        return None



def get_sector_data(sector_url):
    page_sector = requests.get(sector_url)
    soup_sector = BeautifulSoup(page_sector.content, "html.parser")
    pattern_sector = re.compile(r"var data = \[(.*?)\]", re.MULTILINE | re.DOTALL)
    scriptParse_sector = re.search(pattern_sector, str(soup_sector))

    block_sector = scriptParse_sector.groups()[0]
    return json.loads("".join(["[", block_sector, "]"]))



def get_industry_data(industry_url):
    page_industry = requests.get(industry_url)
    soup_industry = BeautifulSoup(page_industry.content, "html.parser")
    pattern_industry = re.compile(r"var data = \[(.*?)\]", re.MULTILINE | re.DOTALL)
    scriptParse_industry = re.search(pattern_industry, str(soup_industry))

    block_industry = scriptParse_industry.groups()[0]
    return json.loads("".join(["[", block_industry, "]"]))



def process_sector(sector, sector_dict, financial_sheet, executor):
    financial_statement_url = f"{financial_sheet}?freq=A"
    URL_sector = sector_dict[sector]
    res_sector = get_sector_data(URL_sector)

    dict_industry = {}

    for industry in res_sector:
        link = BeautifulSoup(str(industry['link']), "html.parser")
        link = link.find("a")
        link = str(link['href'])
        dict_industry[industry['zacks_x_ind_desc']] = link

    concat = pd.DataFrame()  # New dataframe to store concatenated results

    for industry in tqdm.tqdm(dict_industry, desc=f"Industries from {sector}"):
        URL_industry = dict_industry[industry]
        res_industry = get_industry_data(URL_industry)

        dict_company = {}
        dict_company_full_name = {}
        dict_company_country = {}

        for company in res_industry:
            link = BeautifulSoup(str(company['link']), "html.parser")
            link = link.find("a")
            link = str(link['href'])
            link = link.replace("stock-price-history", financial_statement_url)
            dict_company[company['ticker']] = link
            dict_company_full_name[company['ticker']] = company['comp_name_2']
            dict_company_country[company['ticker']] = company['country_code']

        futures = []
        for key in dict_company.keys():
            future = executor.submit(scrape_company_data, key, dict_company[key], industry, sector,
                                    dict_company_full_name[key], dict_company_country[key], financial_sheet)
            futures.append(future)

        concurrent.futures.wait(futures)

        # Retrieve results and concatenate dataframes
        results = [future.result() for future in futures if future.result() is not None]
        if results:
            industry_df = pd.concat(results, ignore_index=True)
            concat = pd.concat([concat, industry_df], ignore_index=True)

    return concat

############################### PRICES ###############################



def get_price_info(ticker, financial_sheet):

    # Read the csv from stagging
    ticker_file = financial_sheet[financial_sheet['ticker']==ticker]

    # # Get the company data from yfinance
    # stock_info = yf.Ticker(ticker)
    # hist = stock_info.history(period="max", interval = "1mo")

    # Get the company data from yahoofinancials
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = '1900-01-01'
    yahoo_financials = YahooFinancials(ticker)
    data = yahoo_financials.get_historical_price_data(start_date, end_date, 'monthly')
    historical_prices = data[ticker]['prices']
    hist = pd.DataFrame(historical_prices)
    hist.columns = hist.columns.str.title()
    hist.drop(columns='Date', inplace=True)
    hist = hist.rename(columns={'Formatted_Date': 'Date'})
    hist['Date'] = pd.to_datetime(hist['Date'])
    hist['year'] = hist['Date'].dt.year
    hist.dropna(inplace=True)


    # Convert 'Date' column in 'hist' dataframe to string format
    hist['Date'] = hist['Date'].astype(str)
    # Extract the 'YYYY-MM' format from 'Date' column in 'ticker_file' dataframe
    ticker_file['YearMonth'] = ticker_file['Date'].str.slice(0, 7)
    # Filter 'hist' dataframe based on 'YYYY-MM' values from 'ticker_file'
    filtered_hist = hist[hist['Date'].str.slice(0, 7).isin(ticker_file['YearMonth'])]



    # Format to datetime
    filtered_hist['Date'] = pd.to_datetime(filtered_hist['Date'])
    hist['Date'] = pd.to_datetime(hist['Date'])
    # Find the earliest date in the filtered_hist dataframe
    earliest_date = filtered_hist['Date'].min()
    # Filter 'hist' dataframe for rows with a date greater than the earliest date
    filtered_hist_after_earliest = hist[hist['Date'].dt.year > earliest_date.year]
    # Find the earliest date in the filtered_hist dataframe
    earliest_date = filtered_hist['Date'].min()
    # Filter 'hist' dataframe for rows with a date greater than the earliest date
    filtered_hist_after_earliest = hist[hist['Date'].dt.year < earliest_date.year]
    # Filter the resulting dataframe for rows with the same month as the earliest date
    appended_hist = filtered_hist_after_earliest[filtered_hist_after_earliest['Date'].dt.month == earliest_date.month]
    # Concatenate the filtered_hist dataframe with the appended_hist dataframe
    prices_filtered = pd.concat([filtered_hist, appended_hist]).sort_values(by='year').reset_index(drop=True)



    prices_filtered['ticker'] = ticker
    prices_filtered['Growth -1'] = (prices_filtered['Close'] - prices_filtered['Close'].shift(periods=1)) /  prices_filtered['Close'].shift(periods=1)
    prices_filtered['Growth +1'] = (prices_filtered['Close'].shift(periods=-1) - prices_filtered['Close']) /  prices_filtered['Close']
    prices_filtered['Growth +5'] = (prices_filtered['Close'].shift(periods=-5) - prices_filtered['Close']) /  prices_filtered['Close']
    prices_filtered['Growth -10'] = (prices_filtered['Close'] - prices_filtered['Close'].shift(periods=5)) /  prices_filtered['Close'].shift(periods=5)
    prices_filtered['Growth -5'] = (prices_filtered['Close'] - prices_filtered['Close'].shift(periods=10)) /  prices_filtered['Close'].shift(periods=10)

    # filter_years = pd.to_datetime(ticker_file['Date']).dt.year.tolist()
    # histgrouped_filtered = prices_filtered[prices_filtered['year'].isin(filter_years)]
    prices_filtered.reset_index(names=['longevity'], inplace=True)
    
    return prices_filtered



def process_prices_company(company, financial_sheet):
    try:
        histgrouped_filtered = get_price_info(company, financial_sheet)
        return histgrouped_filtered

    except Exception as e:
        error_message = f"Error occurred for {company[:-4]}: {str(e)}"
        logging.error(error_message)
        return None