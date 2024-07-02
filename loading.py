import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

def run_loading():
    data = pd.read_csv('cleaneddata.csv')
    customers = pd.read_csv('customers.csv')
    products = pd.read_csv('products.csv')
    staff = pd.read_csv('staff.csv')
    transaction = pd.read_csv('transaction.csv')
    
    load_dotenv(override=True)

    connect_str = os.getenv('azure_connection_string')
    container_name = os.getenv('container_name')

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    files =[
        (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
        (products, 'cleaneddata/products.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staff, 'cleaneddata/staff.csv'),
        (transaction, 'cleaneddata/transaction.csv')
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)
        blob_client.upload_blob(output, overwrite=True)
        print(f'{blob_name} loaded into azure blob storage')