import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
from gdp_deflator import *
from region_to_states import *
import os

# get a list of all available states
ALL_STATES = sorted([s for states in REGION_TO_STATES.values() for s in states])

def grid_region_to_state(df) -> pd.DataFrame:
    def region_to_list(r):
        if r in REGION_TO_STATES:
            return REGION_TO_STATES[r]
        else:
            return [r]
    df['region'] = df.region.apply(region_to_list)
    df = df.explode('region', ignore_index=True)
    return df

def country_to_state(df) -> pd.DataFrame:
    def region_to_list(r):
        if r in COUNTRY_TO_STATES:
            return COUNTRY_TO_STATES[r]
        else:
            return [r]
    df['region'] = df.region.apply(region_to_list)
    df = df.explode('region', ignore_index=True)
    return df


def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)


def get_fuel_price_escalation(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    lifetime:pd.DataFrame,
    tech_to_fuel:pd.DataFrame,
    save_output=False,
    gcam_query_name='prices of all markets',
    path_to_query_file: str = './elec_queries.xml',
    ):

    # set up unit conversions
    GIGAJOULES_TO_MEGA_BRITISH_THERMAL_UNITS = 0.947817120

    # create a Path from str
    db_path = Path(path_to_gcam_database)

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(db_path, gcam_file_name)

    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)

    # collect fuel price data
    fuel_prices = conn.runQuery(get_query_by_name(queries, gcam_query_name))

    # regional biomass is provided at state level
    bio = fuel_prices[(fuel_prices.market.str.contains('regional biomass')) & (~fuel_prices.market.str.contains('Oil'))].copy()
    bio['fuel_type'] = 'regional biomass'
    bio['region'] = bio['market'].astype(str).str[:2]
    bio = bio[bio.region.isin(ALL_STATES)]

    # nuclear is provided at US level
    nuclear = fuel_prices[(fuel_prices.market.str.contains('USAnuclearFuelGenII|USAnuclearFuelGenIII'))].copy()
    nuclear['fuel_type'] = nuclear.market.astype(str).str[3:]
    nuclear['region'] = 'USA'
    nuclear = country_to_state(nuclear)

    # create a list of grid regions
    i=0
    for grid_region in REGION_TO_STATES.keys():
        if i ==0:
            grid_region_list =  grid_region
        else:
            grid_region_list = grid_region_list + "|"+ grid_region
        i+=1
    fuel_list='refined liquids industrial|regional coal|wholesale gas'
        
    # all other fuels are provided at grid region level
    other = fuel_prices[fuel_prices.market.str.contains(grid_region_list)].copy()
    other = other[other.market.str.contains(fuel_list)].copy()
    other['fuel_type'] =  other['market'].str.split('grid', expand=True)[1]
    other['region'] = other['market'].str.split('grid', expand=True)[0] + 'grid'

    # expand to all states
    other = grid_region_to_state(other)

    # recombine fuel dataframes
    prices = pd.concat([bio, nuclear, other])

    # convert GJ to MBTU (million btu)
    prices['value'] = prices['value'] / GIGAJOULES_TO_MEGA_BRITISH_THERMAL_UNITS

    # convert 1975$ to 2015$ following gdp_deflator
    prices['value'] = prices['value'] * deflate_gdp(2015, 1975) 

    # create a list of unique regions
    region_list = lifetime.subRegion.unique()

    # create a list of unique years
    year_list = lifetime.x.unique()

    # create a list of unique fuels
    fuel_list = prices.fuel_type.unique()

    # get min and max technology lifetimes
    min_life = lifetime['value'].min()
    max_life = lifetime['value'].max()

    # create a list of unique lifetimes
    lifetime_list = []
    for y in range(min_life, max_life+1, 5):
        lifetime_list.append(y)

    # sort fuel prices and reduce to year >= 2020
    p = prices[['Year', 'value', 'fuel_type', 'region']].copy()
    p = p.sort_values(by=['region', 'fuel_type', 'Year'])
    p = p[p.Year >=2020]

    #Pivoting dataframe
    p = pd.pivot_table(p, values='value', index=['fuel_type', 'region'],
                        columns=['Year'], aggfunc=np.mean).reset_index().rename_axis(None, axis=1)

    # create a dictionary for each region, year, fuel type, and lifetime combination to store values in
    esc_dict = {d:0 for d in region_list}
    for d in esc_dict:
        esc_dict[d] = {y:0 for y in year_list}
        for y in esc_dict[d]:
            esc_dict[d][y] = {f:0 for f in fuel_list}
            for f in esc_dict[d][y]:
                esc_dict[d][y][f] = {l:0 for l in lifetime_list}

    # calculate escalation rate and store values in dictionary
    for index, row in p.iterrows():
        f = row['fuel_type']
        d = row['region']
        for y in year_list:
            for l in lifetime_list:
                start_year = y
                end_year = min(y + l, 2100)

                # if the price is decreasing, take the absolute difference and divide by lifetime
                if (row[end_year] < row[start_year] ):
                    esc_rate = abs(row[start_year] - row[end_year]) / l

                # otherwise use escalation formula
                else:
                    esc_rate = ((row[end_year] / row[start_year])**(1/l)) - 1
                    pass

                # store value
                esc_dict[d][y][f][l] = esc_rate

    # get fuel by technology type copy
    fuel_by_tech = tech_to_fuel.copy().rename(columns={'technology':'class2'})

    # combine fuel type information with lifetime information for new capacity rows
    escalation_rate = pd.merge(lifetime, fuel_by_tech, how='left')

    # for each row, collect the corresponding escalation rate and add to dataframe
    for index, row in escalation_rate.iterrows():
        d = row['subRegion']
        y = row['x']
        l = row['value']
        f = row['fuel_type']
        if pd.isnull(row['fuel_type']):
            escalation_rate.loc[index,'fuel_esc'] = 0
        else:
            escalation_rate.loc[index,'fuel_esc'] = esc_dict[d][y][f][l]

    escalation_rate['param'] = 'fuel_price_escalation_rate_fraction'
    escalation_rate['units'] = 'fraction'
    escalation_rate = escalation_rate.drop(['value', 'fuel_type'], axis=1)
    escalation_rate = escalation_rate.rename(columns={'fuel_esc':'value'})        

    # validate against new capacity deployments by vintage
    escalation_rate = pd.merge(capacity_crosscheck, escalation_rate, how='left', 
                               on=['scenario','region' ,'subRegion','xLabel', 'x', 'vintage', 'class2'])   
                             
    # print any missing values
    if escalation_rate[escalation_rate.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in escalation_rate[escalation_rate.value.isna()].iterrows():
            print(f"WARNING: Fuel Price Escalation Rate for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        escalation_rate.to_csv(Path(f'./extracted_data/{gcam_scenario}_fuel_price_esc_rate.csv'), index=False)
    else:
       pass

    return escalation_rate


def _get_fuel_price_escalation(      
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
        ):
  
  get_fuel_price_escalation(
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
  )


if __name__ == "__main__":
  _get_fuel_price_escalation()