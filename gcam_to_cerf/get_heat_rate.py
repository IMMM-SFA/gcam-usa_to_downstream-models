import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
from region_to_states import *
import os

# get a list of all available states
ALL_STATES = sorted([s for states in REGION_TO_STATES.values() for s in states])

def get_heat_rate(
    gcam_input_data_dir:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    save_output=False,
    gcam_data_file_name = "L223.TechEff_Dispatch.csv",
    ):

    # set up unit conversions
    BRITISH_THERMAL_UNITS_PER_EXAJOULE = 9.48e14
    KWH_PER_EXAJOULE = 2.77778e11

    # read in exogenous heat rate file
    heat_rate = pd.read_csv(Path(f'{gcam_input_data_dir}/{gcam_data_file_name}'), skiprows=1)

    # reduce to needed fuels
    fuels = ['nuclearFuelGenII', 'nuclearFuelGenIII', 'refined liquids industrial', 'regional biomass', 'regional coal', 'wholesale gas']
    heat_rate = heat_rate[heat_rate['minicam.energy.input'].isin(fuels)]

    # convert from efficiency to I-O coefficient
    heat_rate['value'] = 1 / heat_rate['efficiency']

    # collect vintage
    heat_rate['vintage'] = heat_rate['year']

    # convert EJ in per EJ out to BTU in per kwh out
    heat_rate['value'] = (heat_rate['value'] * BRITISH_THERMAL_UNITS_PER_EXAJOULE) / KWH_PER_EXAJOULE

    # rename columns
    heat_rate.rename(columns={
        'Units': 'units',
        'Year': 'year',
        'minicam.energy.input': 'fuel_type',
        'value': 'heat_rate_BTUperkWh',
    }, inplace=True)

    # reduce columns
    heat_rate = heat_rate[['year', 'vintage', 'region', 'technology', 'fuel_type', 'heat_rate_BTUperkWh']].reset_index(drop=True)

    # remove non-USA subRegions
    heat_rate = heat_rate[heat_rate.region.isin(ALL_STATES)]

    # create a list of fuels by technology that can be used for collecting fuel prices 
    tech_to_fuel = heat_rate[['technology', 'fuel_type']].copy().drop_duplicates(ignore_index=True)

    # transform to expected format
    heat_rate = standardize_format(heat_rate, param='elec_heat_rate_BTUperkWh', scenario=gcam_scenario, 
                                units='Heat Rate (BTU per kWh)', valueColumn='heat_rate_BTUperkWh')

    # validate against new capacity deployments by vintage
    heat_rate = pd.merge(capacity_crosscheck, heat_rate, how='left', on=['scenario','region' ,'subRegion','xLabel', 'x', 'vintage', 'class2'])

    #replace renewable heat rates with zero
    heat_rate['value'] = np.where(heat_rate['class2'].str.contains('wind|PV|CSP|geothermal'),
                                0,
                                heat_rate['value'])
    heat_rate['param'].fillna('elec_heat_rate_BTUperkWh', inplace=True)
    heat_rate['classLabel2'].fillna('technology', inplace=True)
    heat_rate['units'].fillna('Heat Rate (BTU per kWh)', inplace=True)


    # print any missing values
    if heat_rate[heat_rate.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in heat_rate[heat_rate.value.isna()].iterrows():
            print(f"WARNING: Heat Rate for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        heat_rate.to_csv(Path(f'./extracted_data/{gcam_scenario}_heat_rates.csv'), index=False)
    else:
       pass

    return heat_rate, tech_to_fuel


def _get_heat_rate(
        gcam_input_data_dir,
        gcam_scenario
      ):
  
  get_heat_rate(
        gcam_input_data_dir,
        gcam_scenario
      )


if __name__ == "__main__":
  _get_heat_rate()