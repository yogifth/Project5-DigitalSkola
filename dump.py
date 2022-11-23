import os
import pandas as pd
import numpy as np

import psycopg2 as pg
from sqlalchemy import create_engine

# read file csv
listfile = [ "bigdata_customer", "bigdata_product", "bigdata_transaction" ]
for file in listfile:
    path = os.getcwd()+"\\data\\"
    df = pd.read_csv(path + file + ".csv", )

    # connection
    engine = create_engine("postgresql://postgres:password@localhost:1234/postgres")

    # dump data
    try:
        df.to_sql(file, index=False, con=engine, if_exists="replace")
        print(f"Data {file} dump success")
    except:
        print(f"Data {file} dump failed")