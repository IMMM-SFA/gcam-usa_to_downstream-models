import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from gdp_deflator import *
import os

def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)

def get_go_carbon_price(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    save_output=False,
    gcam_query_name = "CO2 prices",
    path_to_query_file: str = './elec_queries.xml',
    ):

    units = 'Carbon Price (2015 USD/tonCarbon)'
    param = 'carbon_price_2015USDperTonCarbon'
    xlabel = 'Year'
    region = 'USA'

    # if it is rcp8.5 then there are no carbon prices to collect, set to 0
    if 'rcp85' in gcam_scenario:
        
        year_list = list(range(2015, 2101, 5))

        data = {'x': year_list, 
                'value': np.zeros(len(year_list)), 
                'units':[units]*len(year_list), 
                'param':[param]*len(year_list), 
                'scenario':[gcam_scenario]*len(year_list), 
                'xLabel':[xlabel]*len(year_list), 
                'region':[region]*len(year_list)
            }
        carbon_price = pd.DataFrame.from_dict(data)
        carbon_price['vintage'] = 'Vint_' + carbon_price['x'].astype(str)

    else:   
                       
        # create connection to gcam db
        conn = gcamreader.LocalDBConn(path_to_gcam_database, gcam_file_name)
        
        # parse the queries file
        queries = gcamreader.parse_batch_query(path_to_query_file)
        
        # collect dataframe
        carbon_price = conn.runQuery(get_query_by_name(queries, gcam_query_name))

        # select correct market
        carbon_price = carbon_price[carbon_price.market == 'globalCO2']

        # convert 1990$ to 2015$ following gdp_deflator
        carbon_price['value'] = carbon_price['value'] * deflate_gdp(2015, 1990)
        
        # drop extra columns
        carbon_price = carbon_price.drop(['scenario', 'market', 'Units'], axis=1)
        
        # rename columns
        carbon_price.rename(columns={
                'Year': 'x',
                }, inplace=True)
        carbon_price['units'] = units
        carbon_price['param'] = param
        carbon_price['scenario'] = gcam_scenario
        carbon_price['xLabel'] = xlabel
        carbon_price['region'] = region
        carbon_price['vintage'] = 'Vint_' + carbon_price['x'].astype(str)

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        carbon_price.to_csv(Path(f'./extracted_data/{gcam_scenario}_go_carbon_price.csv'), index=False)
    else:
       pass

    return carbon_price


def _get_go_carbon_price(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario):
  
  get_go_carbon_price(
     path_to_gcam_database,
     gcam_file_name,
     gcam_scenario)


if __name__ == "__main__":
  _get_go_carbon_price()
