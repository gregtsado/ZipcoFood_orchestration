import pandas as pd

def run_transformation():
    data = pd.read_csv(r'\data\transaction.csv')
    
    #handling missing values
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)
        
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col:'Unknown'}, inplace=True)
        
    data['Date'] = pd.to_datetime(data['Date'])
    
    #creating dimension tables
    products= data[['ProductName']].drop_duplicates().reset_index(drop=True)
    products.index.name = 'product_id'
    products = products.reset_index()

    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers.index.name = 'customer_id'
    customers = customers.reset_index()

    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff.index.name ='staff_id'
    staff =staff.reset_index()

    transaction = data.merge(products, on='ProductName', how='left')\
                        .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail'], how='left')\
                        .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')
                        
    transaction.index.name = 'transaction_id'
    transaction = transaction.reset_index()\
                    [['Date','transaction_id','customer_id','staff_id','Quantity', 'UnitPrice', 'StoreLocation','PaymentType', 'PromotionApplied',
                    'Weather', 'Temperature','StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min','OrderType', 'DayOfWeek','TotalSales']]
                    
    # Saving tables to csv
    
    data.to_csv('clean_data.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    products.to_csv('products.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)
    
    print('Data cleaning and transformation completed')
    