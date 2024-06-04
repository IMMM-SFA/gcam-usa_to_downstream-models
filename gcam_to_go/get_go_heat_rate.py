import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
import os


def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)


def get_go_heat_rate(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    save_output=False,
    gcam_query_name = "elec coefs by tech",
    path_to_query_file: str = './elec_queries.xml',
    ):

    # set up unit conversions
    BRITISH_THERMAL_UNITS_PER_EXAJOULE = 9.48e14
    KWH_PER_EXAJOULE = 2.77778e11

    # create a Path from str
    db_path = Path(path_to_gcam_database)

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(db_path, gcam_file_name)

    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)

    # heat rate is based on technology and input fuel
    heat_rate = conn.runQuery(get_query_by_name(queries, gcam_query_name))

    # select required fuels
    fuels = ['nuclearFuelGenII', 'nuclearFuelGenIII', 'refined liquids industrial', 'regional biomass', 'regional coal', 'wholesale gas']
    heat_rate = heat_rate[heat_rate.input.isin(fuels)]

    # convert EJ in per EJ out to BTU in per kwh out
    heat_rate['value'] = (heat_rate['value'] * BRITISH_THERMAL_UNITS_PER_EXAJOULE) / KWH_PER_EXAJOULE

    # rename columns
    heat_rate.rename(columns={
        'Units': 'units',
        'Year': 'x',
        'input': 'fuel_type',
        'technology':'class2',
        'region':'subRegion'
    }, inplace=True)

    # add columns
    heat_rate['param'] = 'elec_heat_rate_BTUperkWh'
    heat_rate['units'] = 'Heat Rate (BTU per kWh)'
    heat_rate['region'] = 'USA'
    heat_rate['scenario']  = gcam_scenario
    heat_rate['vintage'] = heat_rate['x']
    heat_rate['xLabel'] = 'year'
    heat_rate['classLabel2'] = 'technology'

    # reduce columns
    heat_rate = heat_rate[['scenario', 'region', 'subRegion', 'param', 'classLabel2' ,
                        'class2', 'xLabel', 'x', 'vintage', 'units', 'value']].reset_index(drop=True)

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        heat_rate.to_csv(Path(f'./extracted_data/{gcam_scenario}_go_heat_rates.csv'), index=False)
    else:
       pass

    return heat_rate


def _get_go_heat_rate(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario
      ):
  
  get_go_heat_rate(
      path_to_gcam_database,
      gcam_file_name,
      gcam_scenario
      )


if __name__ == "__main__":
  _get_go_heat_rate()