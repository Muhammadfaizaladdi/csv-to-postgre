import pandas as pd
import numpy as np

PATH="data/SampleSuperstore.csv"
ENCODING='windows-1254'
out_data = {'orders_data' : ['order_id', 'ship_mode'], 
            'customers_data' : ['customer_id', 'region'], 
            'products_data' : ['product_id', 'product_name'],
            'transactions_data' : ['order_id', 'customer_id', 'product_id', 'sales', 'quantity', 'discount', 'profit']}

def read_data(path, 
              save_file=True,
              return_file=True,
              set_index=None,
              encoding=None):

    data = pd.read_csv(path, encoding=encoding)
    
    if return_file:
        return data

def snake_case_columns(dataset,
                       lower=True,
                       replace=True):
    
    columns = dataset.columns
    
    if lower:
        columns = columns.str.lower()
        
    if replace:
        columns = columns.str.replace(' ', '_').str.replace('-', '_')
        
    return columns

def slice_columns(dataset,
                  sliced=True,
                  columns=None,
                  start_column=None,
                  end_column=None,
                  save_file=True,
                  keep_index=False,
                  out_file_name=None,
                  return_file=True):
    
    if sliced:
        sliced_column = dataset.loc[:, start_column:end_column]
    else:
        sliced_column = dataset.loc[:, columns]
    
    if save_file:
        sliced_column.to_csv('output/'+out_file_name+'.csv', index=keep_index)
    if return_file:
        return sliced_column
    

    

def run():
    data = read_data(path=PATH, encoding=ENCODING)
    columns = snake_case_columns(data)
    data.columns = columns
    for file in out_data:
        if file == 'transaction_data':
            slice_columns(data, 
                          sliced=False, 
                          columns=out_data[file],
                          out_file_name=file,
                          return_file=False)
        else:
            slice_columns(data, 
                          sliced=True, 
                          start_column=out_data[file][0], 
                          end_column=out_data[file][1], 
                          out_file_name=file,
                          return_file=False)

if __name__ == '__main__':
    run()
    print("-------------------read data done-----------------")