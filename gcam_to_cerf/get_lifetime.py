import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
import os


def get_lifetime(
    gcam_input_data_dir:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    save_output=False,
    gcam_data_file_name = "L223.TechLifetime_Dispatch.csv",
    ):
    
    # read in database file
    lifetime = pd.read_csv(Path(f'{gcam_input_data_dir}/{gcam_data_file_name}'), skiprows=1)
    
    # rename columns
    lifetime.rename(columns={
                'Year': 'year',
                }, inplace=True)

    # adjust to cerf format
    lifetime = standardize_format(lifetime, param='elec_lifetime_yr', scenario=gcam_scenario, 
                                    units='lifetime (yr)', valueColumn='lifetime', vintageColumn='year')

    # validate against new capacity deployments by vintage
    lifetime = pd.merge(capacity_crosscheck, lifetime, how='left', on=['subRegion', 'class2']) 

    # print any missing values
    if lifetime[lifetime.value.isna()].empty:
        print('All required values available')
    else:
        for index, row in lifetime[lifetime.value.isna()].iterrows():
            print(f"WARNING: Lifetime for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        lifetime.to_csv(Path(f'./extracted_data/{gcam_scenario}_lifetime.csv'), index=False)
    else:
       pass

    return lifetime


def _get_lifetime(
    gcam_input_data_dir,
    gcam_scenario
    ):
  
  get_lifetime(
    gcam_input_data_dir,
    gcam_scenario
    )


if __name__ == "__main__":
  _get_lifetime()