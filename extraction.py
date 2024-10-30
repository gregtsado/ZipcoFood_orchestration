import pandas as pd

def run_extraction():
    try:
        data =pd.read_csv(r"\\wsl.localhost\Ubuntu\home\gregtsado\airflow\zipco_food_dag\zipco_transaction.csv")
        print('Data extracted successfully')
    except Exception as e:
        print(f'An error occured:{e}')