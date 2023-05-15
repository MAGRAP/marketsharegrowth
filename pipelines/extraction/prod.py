from market_growth_analysis import process_sector, scrape_company_data
import concurrent
import tqdm
from bs4 import BeautifulSoup
import requests

URL = "https://www.macrotrends.net/stocks/research"
page = requests.get(URL)


soup = BeautifulSoup(page.content, "html.parser")

results = soup.find_all("a", href= lambda text: "/stocks/sector/" in text.lower())
sector_dict = dict()

for result in results:
    sector_dict[result.text] = ("".join(("https://www.macrotrends.net", result["href"]))).strip()

with concurrent.futures.ThreadPoolExecutor() as executor:
    print("Retrieving income statement")
    for sector in tqdm.tqdm(sector_dict, desc="Sectors"):
        process_sector(sector, sector_dict, "income-statement?freq=A")

    print("Retrieving cash flow statements")
    for sector in tqdm.tqdm(sector_dict, desc="Sectors"):
        process_sector(sector, sector_dict, "cash-flow-statement?freq=A")

    print("Retrieving balance sheet")
    for sector in tqdm.tqdm(sector_dict, desc="Sectors"):
        process_sector(sector, sector_dict, "balance-sheet?freq=A")