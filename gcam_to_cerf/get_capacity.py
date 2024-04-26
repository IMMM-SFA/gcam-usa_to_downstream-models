import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
import os

def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)

def get_capacity(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    save_output=False,
    gcam_query_name = "elec capacity by tech and vintage",
    path_to_query_file: str = './elec_queries.xml',
    ):

    # set up unit conversions
    EXAJOULES_TO_GIGAWATT_HOURS = 277778
    HOURS_PER_YEAR = 8760

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(path_to_gcam_database, gcam_file_name)
    
    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)
    
    # collect dataframe
    capacity = conn.runQuery(get_query_by_name(queries, gcam_query_name))

    # convert from EJ at 100% capacity factor per year to GW
    capacity['value'] = (capacity['value'] * EXAJOULES_TO_GIGAWATT_HOURS) / HOURS_PER_YEAR

    # collect vintage
    capacity['vintage'] = capacity.technology.str.split("=", expand=True)[1]

    # collect technology name
    capacity['technology'] = capacity.technology.str.split(',', expand=True)[0]

    # rename columns
    capacity.rename(columns={
        'Units': 'units',
        'Year': 'year',
        'value': 'capacity_GW',
                    }, inplace=True)

    # transform to expected cerf format
    capacity = standardize_format(capacity, 
                                param='elec_cap_usa_GW', 
                                scenario=gcam_scenario,
                                units='GW', 
                                valueColumn='capacity_GW')
    
    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        capacity.to_csv(Path(f'./extracted_data/{gcam_scenario}_capacity.csv'), index=False)
    else:
       pass
    
    # create a dataframe of new capacity by vintage to check other parameters with
    capacity_crosscheck = capacity.copy()
    capacity_crosscheck['vintage'] = capacity_crosscheck['vintage'].str.split("_", expand=True)[1].astype(int)
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.vintage == capacity_crosscheck.x]
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.x >= 2020]
    capacity_crosscheck = capacity_crosscheck.drop(['param', 'classLabel2', 'units', 'value'], axis=1)
    capacity_crosscheck['vintage'] = "Vint_" + capacity_crosscheck['vintage'].astype(str)
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.class2 != 'hydro']

    return capacity, capacity_crosscheck


def _get_capacity(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario):
  
  get_capacity(
     path_to_gcam_database,
     gcam_file_name,
     gcam_scenario)


if __name__ == "__main__":
  _get_capacity()
