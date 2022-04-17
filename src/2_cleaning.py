import pandas as pd
import numpy as np

PATH_CUSTOMERS = 'output/customers_data.csv'
PATH_ORDERS = 'output/orders_data.csv'
PATH_PRODUCTS = 'output/products_data.csv'

customers_data = pd.read_csv('output/customers_data.csv')
orders_data = pd.read_csv('output/orders_data.csv')
products_data = pd.read_csv('output/products_data.csv')
transactions_data = pd.read_csv("output/transactions_data.csv")

dataset = {'customers':customers_data, 'products': products_data, 'orders':orders_data, 
           'transactions':transactions_data}
orders_columns_to_parse = ["order_date", "ship_date"]


def drop_duplicated(dataset, 
                    keep=None,
                    subset = None,
                    inplace=False,
                    ignore_index=True):
    
    dataset = dataset.drop_duplicates(keep=keep,
                                      subset=subset,
                                      inplace=inplace, 
                                      ignore_index=ignore_index)
    
    return dataset 


def clean_date(dataset,
               columns=None):
    
    for col in columns:
        dataset[col] = pd.to_datetime(orders_data[col])
        
    return dataset

def run(save_file=True):
    for data in dataset:
        
        if data == "customers":
            subset = ["customer_id", "customer_name"]
            dataset[data] = drop_duplicated(dataset[data], subset=subset , keep='last')
        elif data == "products":
            subset = "product_id"
            dataset[data] = drop_duplicated(dataset[data], subset=subset , keep='last')
        elif data == "transactions":
            subset = ["order_id", "customer_id", "product_id"]
            dataset[data] = drop_duplicated(dataset[data], subset=subset , keep='last')
        else:
            dataset[data] = drop_duplicated(dataset[data], keep='last')
        
        if data == "products":
            dataset[data]["product_name"] = dataset[data]["product_name"].str.replace("\'", "")
        elif data == "customers":
            dataset[data]["customer_name"] = dataset[data]["customer_name"].str.replace("\'", "")
        
        if data == 'orders':
            clean_date(dataset[data], orders_columns_to_parse)    
            
        if save_file:
            dataset[data].to_csv('output/'+data+'_clean.csv', index=False)
            
    
if __name__ == '__main__':
    run()
    print("-------------------------cleaning process done-----------------------------")