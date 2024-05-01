import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
from region_to_states import *
import os

def country_to_state(df) -> pd.DataFrame:
    def region_to_list(r):
        if r in COUNTRY_TO_STATES:
            return COUNTRY_TO_STATES[r]
        else:
            return [r]
    df['region'] = df.region.apply(region_to_list)
    df = df.explode('region', ignore_index=True)
    return df

def get_carbon_dioxide_content(
    gcam_input_data_dir:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    tech_to_fuel:pd.DataFrame,
    save_output=False,
    gcam_data_file_name = "L202.CarbonCoef.csv",
    ):

    ATOMIC_WEIGHT_OF_CO2_PER_ATOMIC_WEIGHT_OF_CARBON = 44 / 12
    TONS_PER_KG = 1e-3
    MEGAWATT_HOURS_PER_GIGAJOULE = 0.277777778
    MEGA_BRITISH_THERMAL_UNITS_PER_MEGAWATT_HOUR = 3.412e6

    # read in database file
    co2_content = pd.read_csv(Path(f'{gcam_input_data_dir}/{gcam_data_file_name}'), skiprows=2)

    # rename columns
    co2_content.rename(columns={
            'sector': 'fuel_type',
            'PrimaryFuelCO2Coef': 'fuel_co2_content_tonsperMBTU',
            'PrimaryFuelCO2Coef.name':'fuel_type',
            'Year': 'year',
            }, inplace=True)

    # convert from KG of carbon per GJ to tons of carbon per million british thermal units
    co2_content['fuel_co2_content_tonsperMBTU'] = (co2_content['fuel_co2_content_tonsperMBTU'] * \
                                                   ATOMIC_WEIGHT_OF_CO2_PER_ATOMIC_WEIGHT_OF_CARBON * TONS_PER_KG ) / \
                                                    (MEGAWATT_HOURS_PER_GIGAJOULE * MEGA_BRITISH_THERMAL_UNITS_PER_MEGAWATT_HOUR)

    # map to fuel types by technology from heat rate output to get all technologies
    co2_content = tech_to_fuel.merge(co2_content, how='left', on='fuel_type')

    # select USA only
    co2_content = co2_content[co2_content.region == 'USA']

    # expand to all states
    co2_content = country_to_state(co2_content)

    # rename columns
    co2_content.rename(columns={
            'technology': 'class2',
            'fuel_co2_content_tonsperMBTU': 'value',
            'region':'subRegion',
            }, inplace=True)

    # # validate against new capacity deployments by vintage
    co2_content = pd.merge(capacity_crosscheck, co2_content, how='left', on=['subRegion', 'class2'])   

    co2_content = co2_content.drop(['fuel_type'], axis=1)

    #replace renewable and nuclear CO2 content with zero
    co2_content['value'] = np.where(co2_content['class2'].str.contains('wind|PV|CSP|geothermal|Gen'),
                                    0,
                                    co2_content['value'])

    # add columns
    co2_content['units'] = 'Fuel CO2 Content (Tons per MBTU)'
    co2_content['param'] = 'elec_fuel_co2_content_tonsperMBTU'

    # print any missing values
    if co2_content[co2_content.value.isna()].empty:
        print('All required values available')
    else:
        for index, row in co2_content[co2_content.value.isna()].iterrows():
            print(f"WARNING: Fuel Price for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        co2_content.to_csv(Path(f'./extracted_data/{gcam_scenario}_co2_content.csv'), index=False)
    else:
       pass

    return co2_content


def _get_carbon_dioxide_content(
    gcam_input_data_dir,
    gcam_scenario
    ):
  
  get_carbon_dioxide_content(
    gcam_input_data_dir,
    gcam_scenario
    )


if __name__ == "__main__":
  _get_carbon_dioxide_content()