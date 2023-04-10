from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import json
from datetime import datetime

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