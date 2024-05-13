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
    gcam_cf_query_name = "elec investment capacity factor",
    gcam_energy_query_name = "elec investment by investment segment (energy)",
    path_to_query_file: str = './elec_queries.xml',
    ):

    # create a Path from str
    db_path = Path(path_to_gcam_database)

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(db_path, gcam_file_name)

    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)

    # collect capacity factor
    capacity_factor = conn.runQuery(get_query_by_name(queries, gcam_cf_query_name))

    # collect energy generation by investment segment, state, technology, and year
    energy = conn.runQuery(get_query_by_name(queries, gcam_energy_query_name))
    energy = energy.drop(['Units', 'scenario'], axis=1)
    energy = energy.sort_values(by=['region', 'technology', 'Year', 'sector'])

    # calculate what fraction of generation for each technology, state, and year combination happens in each investment segment (e.g., baseload)
    total_energy = energy.groupby(['region', 'technology', 'Year'], as_index=False).sum().rename(columns={'value':'total'}).drop(['sector'], axis=1)
    energy = pd.merge(energy, total_energy, how='left', on=['region', 'technology', 'Year'])
    energy['fraction'] = energy['value'] / energy['total']
    energy = energy[['region', 'technology', 'Year', 'sector', 'fraction']]


    # map to as many available energy fractions as possible for years past 2020
    capacity_factor = capacity_factor[capacity_factor.Year >=2020]
    capacity_factor = capacity_factor.sort_values(by=['region', 'technology', 'Year', 'sector'])
    capacity_factor = pd.merge(capacity_factor, energy, how='left')

    # drop rows that don't have corresponding generation fractions
    capacity_factor = capacity_factor[capacity_factor.fraction.notna()]

    # calculate weighted average across investment segments
    capacity_factor['value'] = capacity_factor['value'] *  capacity_factor['fraction'] 
    capacity_factor = capacity_factor[['region','technology', 'Year' ,'value',]].groupby(['region','technology','Year'], as_index=False).sum()

    # rename columns
    capacity_factor.rename(columns={
        'Units': 'units',
        'Year': 'year',
        'value': 'capacity_factor',
        }, inplace=True)

    # collect vintage from year. Given that the query is for investment technologies, the vintage will be the same as the year
    capacity_factor['vintage'] = capacity_factor['year']

    # transform to expected format
    capacity_factor = standardize_format(capacity_factor, param='elec_capacity_factor_usa_in', scenario=gcam_scenario,
                                    units='Capacity Factor', valueColumn='capacity_factor')

    # combine with capacity crosscheck to find any missing capacity factors
    capacity_factor = pd.merge(capacity_crosscheck, capacity_factor, how='left', on=['scenario','region' ,'subRegion','xLabel', 'x', 'vintage', 'class2'])

    # calculate mean of weighted average capacity factor by state and technology type
    state_tech_cf = capacity_factor[['scenario', 'region', 'subRegion', 'class2', 'xLabel', 'value']].groupby(['scenario', 'region', 'subRegion', 'class2', 'xLabel'], as_index=False).mean()

    # calculate mean of weighted average capacity factor by technology type
    tech_cf = capacity_factor[['scenario', 'region', 'class2', 'xLabel', 'value']].groupby(['scenario', 'region', 'class2', 'xLabel'], as_index=False).mean()

    # separate out rows with missing capacity factors
    missing_cf = capacity_factor[capacity_factor.value.isna()]
    missing_cf = missing_cf.drop(['value'], axis=1)

    # drop missing values from primary capacity factor df
    capacity_factor = capacity_factor[capacity_factor.value.notna()]

    # fill in missing capacity factors with mean of weighted average capacity factor by state and technology type first
    missing_cf = pd.merge(missing_cf, state_tech_cf, how='left')

    # split out filled in values
    fill1 = missing_cf[missing_cf.value.notna()]

    # split out rows that are still missing values
    missing_cf = missing_cf[missing_cf.value.isna()]
    missing_cf = missing_cf.drop(['value'], axis=1)

    # recombine with main capacity factor dataframe
    capacity_factor = pd.concat([capacity_factor, fill1])

    # fill in remaining missing capacity factors with mean of weighted average capacity factor technology type
    fill2 = pd.merge(missing_cf, tech_cf, how='left')

    # recombine with main capacity factor dataframe
    capacity_factor = pd.concat([capacity_factor, fill2])

    # fill in parameter info
    capacity_factor['param'].fillna('elec_capacity_factor_usa_in', inplace=True)
    capacity_factor['classLabel2'].fillna('technology', inplace=True)
    capacity_factor['units'].fillna('Capacity Factor', inplace=True)

    # check for any remaining missing values
    if capacity_factor[capacity_factor.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in capacity_factor[capacity_factor.value.isna()].iterrows():
            m = print(f"WARNING: capacity factor value for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

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