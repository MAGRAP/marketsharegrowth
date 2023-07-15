import yaml
import pyodbc
from market_growth_analysis.etl.stagging import *
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import os
print("current working directory")
print(os.getcwd())
# Load the YAML file
with open('conf/global.yml', 'r') as f:
    columns = yaml.safe_load(f)

# Load the YAML file
with open('conf/local.yml', 'r') as f:
    credentials = yaml.safe_load(f)

print(credentials)