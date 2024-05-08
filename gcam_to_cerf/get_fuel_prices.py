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


def get_fuel_prices(
    path_to_gcam_database:str,
    gcam_file_name:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
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

    # nuclear is provided at US aggregate level and must be split out to all states
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
        
    # all other fuels are provided at grid region level and must be split into appropriate states
    other = fuel_prices[fuel_prices.market.str.contains(grid_region_list)].copy()
    other = other[other.market.str.contains(fuel_list)].copy()
    other['fuel_type'] =  other['market'].str.split('grid', expand=True)[1]
    other['region'] = other['market'].str.split('grid', expand=True)[0] + 'grid'

    # expand to all states
    other = grid_region_to_state(other)

    # recombine fuel dataframes
    prices = pd.concat([bio, nuclear, other])

    # map to fuel types by technology from heat rate output to get all technologies
    prices = tech_to_fuel.merge(prices, how='left', on='fuel_type')

    # convert $/GJ to $/MBTU (million btu)
    prices['value'] = prices['value'] / GIGAJOULES_TO_MEGA_BRITISH_THERMAL_UNITS

    # convert 1975$ to 2015$ following gdp_deflator
    prices['value'] = prices['value'] * deflate_gdp(2015, 1975) 
    
    # rename columns
    prices.rename(columns={
            'sector': 'fuel_type',
            'value': 'fuel_price_2015USDperMBTU',
            'Year': 'year',
            }, inplace=True)

    # transform to expected format
    prices = standardize_format(prices, param='elec_fuel_price_2015USDperMBTU', 
                                scenario=gcam_scenario, units='Fuel Cost (2015 USD/MBTU)', 
                                valueColumn='fuel_price_2015USDperMBTU', vintageColumn='year')
                                
    # validate fuel prices against new capacity deployments by vintage
    prices = pd.merge(capacity_crosscheck, prices, how='left', on=['scenario','region' ,'subRegion','xLabel', 'x', 'vintage', 'class2'])                            

    #replace renewable fuel prices with zero
    prices['value'] = np.where(prices['class2'].str.contains('wind|PV|CSP|geothermal'),
                                0,
                                prices['value'])

    prices['param'].fillna('elec_fuel_price_2015USDperMBTU', inplace=True)
    prices['classLabel2'].fillna('technology', inplace=True)
    prices['units'].fillna('Fuel Cost (2015 USD/MBTU)', inplace=True)

    # print any missing values
    if prices[prices.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in prices[prices.value.isna()].iterrows():
            print(f"WARNING: Fuel Price for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        prices.to_csv(Path(f'./extracted_data/{gcam_scenario}_fuel_prices.csv'), index=False)
    else:
       pass

    # calculate fuel price escalation between years
    fuel_escalation_rate = prices[['subRegion', 'class2', 'vintage', 'x', 'value']].copy()

    # sort values
    fuel_escalation_rate = fuel_escalation_rate.sort_values(by=['subRegion', 'class2', 'vintage', 'x'])

    # collect the previous year of available fuel price for given technology and state combination
    fuel_escalation_rate['y'] = fuel_escalation_rate['x'] - fuel_escalation_rate.groupby(['subRegion', 'class2'])['x'].diff()

    # create a copy of fuel price dataframe and rename values
    df = prices[['subRegion', 'class2', 'vintage', 'x', 'value']].copy()
    df = df.rename(columns={'value':'value_t_5', 'x':'y'})
    df = df.sort_values(by=['subRegion', 'class2', 'vintage', 'y'])
    df = df[['subRegion', 'y', 'class2', 'value_t_5']]

    # combine dataframes
    fuel_escalation_rate = fuel_escalation_rate.merge(df, how='left', on=['subRegion','class2', 'y'])

    # calculate escalation rate
    fuel_escalation_rate['esc_val'] = (fuel_escalation_rate['value'] - fuel_escalation_rate['value_t_5'] ) / fuel_escalation_rate['value_t_5']
    
    # adjust growth rates for cases where negative growth returns a positive value
    fuel_escalation_rate['esc_val'] = np.where((fuel_escalation_rate['value'] < 0) & 
                                               (fuel_escalation_rate['value_t_5'] < 0) & 
                                               (fuel_escalation_rate['value'] < fuel_escalation_rate['value_t_5']), 
                                               fuel_escalation_rate['esc_val'] * -1,
                                               fuel_escalation_rate['esc_val'])
    
    fuel_escalation_rate = fuel_escalation_rate.drop(['value'], axis=1) 

    # transform to expected cerf format
    fuel_escalation_rate = standardize_format(fuel_escalation_rate, param='fuel_price_escalation_rate_fraction',
                                scenario=gcam_scenario,
                                units='fraction', 
                                valueColumn='esc_val', vintageColumn='x')
    
    # fill first year values (which have no previous year to calculate from) with zero
    fuel_escalation_rate['value'].fillna(0, inplace=True)

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        fuel_escalation_rate.to_csv(Path(f'./extracted_data/{gcam_scenario}_fuel_price_esc_rate.csv'), index=False)
    else:
       pass

    return prices, fuel_escalation_rate


def _get_fuel_prices(      
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
        ):
  
  get_fuel_prices(
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
  )


if __name__ == "__main__":
  _get_fuel_prices()