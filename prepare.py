import pandas as pd
import numpy as np
import env
from sklearn.model_selection import train_test_split

###################### Connect to the Codeup SQL Server ######################

def get_db_url(database):
    from env import host, user, password
    url = f'mysql+pymysql://{user}:{password}@{host}/{database}'
    return url

###################### Acquire Telco Data ######################

def get_data_from_sql():
    query = """
    SELECT customer_id, monthly_charges, tenure, total_charges
    FROM customers
    WHERE contract_type_id = 3;
    """
    df = pd.read_sql(query, get_db_url('telco_churn'))
    return df

###################### Wrangle Telco Data ######################

def wrangle_telco():
    """
    Queries the telco_churn database
    Returns a clean df with four columns:
    customer_id(object), monthly_charges(float), tenure(int), total_charges(float)
    """
    # Acquire
    telco = get_data_from_sql()
    
    # Clean
    telco['total_charges'] = telco['total_charges'].str.strip()
    telco = telco.replace(r'^\s*$', np.nan, regex=True)
    telco = telco.dropna()
    telco['total_charges'] = telco['total_charges'].astype(float)
    
    # Split
    train_validate, test = train_test_split(telco, test_size=.2, random_state= 42)
    train, validate = train_test_split(train_validate, test_size=.3, random_state= 42)
    
    return train, validate, test

def scale_telco(train, validate, test):
    """
    Takes in the pre-split data and uses train to fit the scaler. The scaler is then applied to all dataframes and 
    the dataframes are returned in thier scaled form.
    """
    # 1. Create the object
    scaler = sklearn.preprocessing.MinMaxScaler()
    
    # 2. Fit the object (learn the min and max value)
    scaler.fit(train[['total_charges', 'tenure', 'monthly_charges']])
    
    # 3. Use the object (use the min, max to do the transformation)
    train[['total_charges_scaled', 'tenure_scaled', 'monthly_charges_scaled']] = scaler.transform(train[['total_charges', 'tenure', 'monthly_charges']])
    validate[['total_charges_scaled', 'tenure_scaled', 'monthly_charges_scaled']] = scaler.transform(validate[['total_charges', 'tenure', 'monthly_charges']])
    test[['total_charges_scaled', 'tenure_scaled', 'monthly_charges_scaled']] = scaler.transform(test[['total_charges', 'tenure', 'monthly_charges']])
    
    return train, validate, test

