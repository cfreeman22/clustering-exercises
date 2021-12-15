# Zillow Data acquisition from the Zillow database
# importing the packages 
import env 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
 
import os 

# Initiating a Connection to the MYSQL server with connection info from env.py

def get_db_url(db_name):
    from env import username, host, password
    return f'mysql+pymysql://{username}:{password}@{host}/{db_name}'



def get_zillow_data():
    '''
    This function reads csv stored in the computer or the Zillow data from the Codeup db into a dataframe.
    '''
    filename = "zillow_df2.csv"

    if os.path.isfile(filename):
        return pd.read_csv(filename, index_col=0)
    else:
        # read the SQL query into a dataframe
        sql_query = """
                SELECT prop.*, 
       pred.logerror, 
       pred.transactiondate, 
       air.airconditioningdesc, 
       arch.architecturalstyledesc, 
       build.buildingclassdesc, 
       heat.heatingorsystemdesc, 
       landuse.propertylandusedesc, 
       story.storydesc, 
       construct.typeconstructiondesc 

FROM   properties_2017 prop  
       INNER JOIN (SELECT parcelid,
       					  logerror,
                          Max(transactiondate) transactiondate 
                   FROM   predictions_2017 
                   GROUP  BY parcelid, logerror) pred
               USING (parcelid) 
       LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
       LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
       LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
       LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
       LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
       LEFT JOIN storytype story USING (storytypeid) 
       LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
WHERE  prop.latitude IS NOT NULL 
       AND prop.longitude IS NOT NULL AND transactiondate <= '2017-12-31' 
"""
        df = pd.read_sql(sql_query, get_db_url('zillow')) #SQL query , database name, Pandas df

        # Write that dataframe to disk for later. Called "caching" the data for later.
        # Return the dataframe to the calling code
        # renaming column names to one's I like better
        
        df.to_csv(filename) 

        # Return the dataframe to the calling code
        # renaming column names to one's I like better
         
        return df  