import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
import os


def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)


def get_carbon_capture_rate(
    gcam_input_data_dir:str,
    gcam_scenario:str,
    capacity_crosscheck:pd.DataFrame,
    save_output=False,
    gcam_data_file_name = "L223.TechCarbonCapture_Dispatch.csv",
    ):

    # read in database file
    carbon_capture = pd.read_csv(Path(f'{gcam_input_data_dir}/{gcam_data_file_name}'), skiprows=1)

    # rename columns
    carbon_capture.rename(columns={'remove.fraction': 'carbon_capture_rate_fraction'}, inplace=True)

    # select required columns
    carbon_capture = carbon_capture[['year', 'region', 'technology', 'carbon_capture_rate_fraction']].reset_index(drop=True)

    # transform to expected cerf format
    carbon_capture = standardize_format(carbon_capture, 
                                        param='elec_carbon_capture_rate_fraction', 
                                        units='Carbon Capture Rate (fraction)',
                                        scenario=gcam_scenario,
                                        valueColumn='carbon_capture_rate_fraction', vintageColumn='year')
    
    # validate against new capacity deployments by vintage
    carbon_capture = pd.merge(capacity_crosscheck, carbon_capture, how='left', on=['scenario','region' ,'subRegion', 'xLabel',
                                                                                     'x', 'vintage', 'class2'])

    # technologies without CCS should have a carbon capture rate of zero
    carbon_capture['value'] = np.where(~carbon_capture.class2.str.contains('CCS'),
                                0,
                                carbon_capture['value'])

    carbon_capture['param'].fillna('elec_carbon_capture_rate_fraction', inplace=True)
    carbon_capture['classLabel2'].fillna('technology', inplace=True)
    carbon_capture['units'].fillna('Carbon Capture Rate (fraction)', inplace=True)

    # print any missing values
    if carbon_capture[carbon_capture.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in carbon_capture[carbon_capture.value.isna()].iterrows():
            print(f"WARNING: Carbon Capture Rate for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        carbon_capture.to_csv(Path(f'./extracted_data/{gcam_scenario}_carbon_capture_rate.csv'), index=False)
    else:
       pass

    return carbon_capture


def _get_carbon_capture_rate(
    gcam_input_data_dir,
    gcam_scenario
    ):
  
  get_carbon_capture_rate(
    gcam_input_data_dir,
    gcam_scenario
    )


if __name__ == "__main__":
  _get_carbon_capture_rate()