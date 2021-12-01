from env import host, user, password
import os

########################################### mySQL Connection ###########################################

def get_connection(database_name):
    '''
    This function takes in a string representing a database name for the Codeup mySQL server 
    and returns a string that can be used to open a connection to the server.
    '''
    return f'mysql+pymysql://{user}:{password}@{host}/{database_name}'

########################################### Obtain Zillow Dataframe ###########################################

def get_zillow_data():
    '''
    This function reads the Zillow database from the Codeup mySQL server and  returns a dataframe.
    If a local file does not exist, this function writes data to a csv file as a backup. The local file 
    ensures that data can be accessed, in the event that you cannot talk to the mySQL database. 
    '''
    # The filename will have 2017 at the end to represent that the only data being looked at is 
    # properties from the year 2017
    if os.path.isfile('zillow2017.csv'):
        # If csv file exists read in data from csv file.
        df = pd.read_csv('zillow2017.csv', index_col=0)
        
    else:
        
        # Read fresh data from database into a DataFrame
        # property land use type id is limited to 'Single Family Residential' properties.
        df =  pd.read_sql(""" SELECT bedroomcnt, 
                                     bathroomcnt, 
                                     calculatedfinishedsquarefeet, 
                                     taxvaluedollarcnt, 
                                     yearbuilt, 
                                     taxamount, 
                                     fips
                              FROM properties_2017
                              WHERE propertylandusetypeid = 261;""", 
                            get_connection('zillow')
                        )
        
        # Cache data into a csv backup
        df.to_csv('zillow2017.csv')
        
    return df

########################################### Clean Zillow Dataframe ###########################################

def wrangle_zillow (zillow):
    # Drop all data with nulls. This about 1.06% of the data, so shouldn't affect outcome
    zillow = zillow.dropna()
    
    # Change the data types of these columns to int
    zillow["yearbuilt"] = zillow.yearbuilt.astype(int)
    zillow["fips"] = zillow.fips.astype(int)
    
    return zillow