import pandas as pd

def run_extraction():
    try:
        data =pd.read_csv('zipco_transaction.csv')
        print('Data extracted successfully')
    except Exception as e:
        print(f'An error occured:{e}')