import pandas as pd
import numpy as np
import psycopg2 as pg
from sqlalchemy import create_engine

from mrjob.job import MRJob
from mrjob.step import MRStep


cols = 'id_transaction,id_customer,date_transaction,product_transaction,amount_transaction'.split(',')

def read_db():
    # connection
    engine = create_engine(f'postgresql://postgres:password@localhost:1234/postgres')
    
    # read data
    df = pd.read_sql_table('bigdata_transaction', con=engine)
    data = [tuple(x) for x in df.values]
    for raw in data:
        return raw
    
class OrderMonthCount(MRJob):

    def mapper(self, _, line):
        # convert each line into a dictionary
        row = dict(zip(cols, read_db(line))) 
        # yield the mont date transaction 'yyyy-mm-dd'
        yield row['date_transaction'][5:7], 1
        
    def reducer(self, key, values):
        # data_transaction compute
        yield None, (key, sum(values))

    def sort(self, key, values):
        data = []
        for order_month, order_count in values:
            data.append((order_month, order_count))

        for order_month, order_count in data:
            yield order_month, order_count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.sort)
        ]


if __name__ == '__main__':
    OrderMonthCount.run()