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

    return financial_df



def scrape_company_data(company_key, company_url, industry, sector, company_full_name, company_country):
    try:
        # Get the data
        URL = company_url
        data = parser(URL)
        data['ticker'] = company_key
        data['industry'] = industry
        data['sector'] = sector
        data['company_full_name'] = company_full_name
        data['country'] = company_country

        # Export the data
        filepath = Path(f'balance_sheet_data/{company_key}.csv')
        filepath.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(filepath)

    except Exception as e:
        error_message = f"Error occurred for {company_key}: {str(e)}"
        logging.error(error_message)



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



def process_sector(sector, sector_dict, financial_statement_url):
    URL_sector = sector_dict[sector]
    res_sector = get_sector_data(URL_sector)

    dict_industry = {}

    for industry in res_sector:
        link = BeautifulSoup(str(industry['link']), "html.parser")
        link = link.find("a")
        link = str(link['href'])
        dict_industry[industry['zacks_x_ind_desc']] = link

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
                                    dict_company_full_name[key], dict_company_country[key])
            futures.append(future)

        concurrent.futures.wait(futures)