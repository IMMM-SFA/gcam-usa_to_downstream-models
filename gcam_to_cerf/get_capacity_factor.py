import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
import os


def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)


def get_capacity_factor(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    save_output=False,
    gcam_query_name = "elec investment capacity factor",
    path_to_query_file: str = './elec_queries.xml',
    ):

    # create a Path from str
    db_path = Path(path_to_gcam_database)

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(db_path, gcam_file_name)

    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)

    # collect capacity factor
    capacity_factor = conn.runQuery(get_query_by_name(queries, gcam_query_name))

    # rename columns
    capacity_factor.rename(columns={
        'Units': 'units',
        'Year': 'year',
        'value': 'capacity_factor',
        }, inplace=True)

    # calculate the mean capacity factor across investment segments
    capacity_factor = capacity_factor[['region','technology', 
                                       'capacity_factor','year']].groupby(['region', 'technology','year'], as_index=False).mean()

    # collect vintage from year
    capacity_factor['vintage'] = capacity_factor['year']

    # transform to cerf expected format
    capacity_factor = standardize_format(capacity_factor, param='elec_capacity_factor_usa_in', scenario=gcam_scenario,
                                units='Capacity Factor', valueColumn='capacity_factor')
    
    # validate against new capacity deployments by vintage
    capacity_factor = pd.merge(capacity_crosscheck, capacity_factor, how='left', on=['scenario','region' ,'subRegion', 'xLabel',
                                                                                     'x', 'vintage', 'class2'])

    # print any missing values
    if capacity_factor[capacity_factor.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in capacity_factor[capacity_factor.value.isna()].iterrows():
            print(f"WARNING: Capacity Factor for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        capacity_factor.to_csv(Path(f'./extracted_data/{gcam_scenario}_capacity_factor.csv'), index=False)
    else:
       pass

    return capacity_factor


def _get_capacity_factor(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario):
  
  get_capacity_factor(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario)


if __name__ == "__main__":
  _get_capacity_factor()