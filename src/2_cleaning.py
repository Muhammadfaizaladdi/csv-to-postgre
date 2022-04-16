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



def drop_duplicated(dataset, 
                    keep=None, 
                    inplace=False,
                    ignore_index=True,
                    save_file=True,
                    out_file_name = None,
                    return_file=True):
    
    dataset = dataset.drop_duplicates(keep=keep, inplace=inplace, ignore_index=ignore_index)
    
    if save_file:
        dataset.to_csv("output/"+out_file_name+'_clean.csv', index=ignore_index)
        
    if return_file:
        return dataset    

def run():
    for data in dataset:
    drop_duplicated(dataset[data], keep='first', out_file_name=data)
    
if __name__ == '__main__':
    run()
    print("-------------------------cleaning process done-----------------------------")