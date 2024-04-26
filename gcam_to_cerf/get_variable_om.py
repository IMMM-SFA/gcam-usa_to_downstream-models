import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
from gdp_deflator import *
import os

def get_query_by_name(queries, name):
    return next((x for x in queries if x.title == name), None)


def get_variable_om(
        path_to_gcam_database:str,
        gcam_file_name:str,
        gcam_scenario:str,
        capacity_crosscheck:pd.DataFrame,
        save_output=False,
        gcam_query_name= 'elec costs by tech and vintage and input',
        path_to_query_file: str = './elec_queries.xml',
        ):

    # set up unit conversions
    GIGAJOULES_TO_MEGAWATT_HOURS = 3.59999712

    # create a Path from str
    db_path = Path(path_to_gcam_database)

    # create connection to gcam db
    conn = gcamreader.LocalDBConn(db_path, gcam_file_name)

    # parse the queries file
    queries = gcamreader.parse_batch_query(path_to_query_file)

    # collect variable O&M
    variable_om = conn.runQuery(get_query_by_name(queries, gcam_query_name))

    # filter for variable O&M only
    variable_om = variable_om[variable_om.input == 'OM-var']

    # convert from $/GJ to $/MWh
    variable_om['value'] = variable_om['value'] * GIGAJOULES_TO_MEGAWATT_HOURS

    # convert 1975$ to 2015$ following gdp_deflator
    variable_om['value'] = variable_om['value'] * deflate_gdp(2015, 1975)

    # add vintage column from technology string
    variable_om['vintage'] = variable_om.technology.str.split("=", expand=True)[1]

    #rename columns
    variable_om.rename(columns={
        'value': 'variable_om_2015USDperMWh',
        'Year': 'year',
        }, inplace=True)

    # remove year component from technology name
    variable_om['technology'] = variable_om.technology.str.split(',', expand=True)[0]

    # transform to expected format
    variable_om = standardize_format(variable_om, param='elec_variable_om_2015USDperMWh',
                                    scenario=gcam_scenario, units="OnM Cost (2015 USD/MWh)", valueColumn='variable_om_2015USDperMWh')
    
    # validate against new capacity deployments by vintage
    variable_om = pd.merge(capacity_crosscheck, variable_om, how='left', on=['scenario','region' ,'subRegion','xLabel', 'x', 'vintage', 'class2'])

    #replace renewable values with zero
    variable_om['value'] = np.where(variable_om['class2'].str.contains('wind|PV|CSP'),
                                0,
                                variable_om['value'])

    variable_om['param'].fillna('elec_variable_om_2015USDperMWh', inplace=True)
    variable_om['classLabel2'].fillna('technology', inplace=True)
    variable_om['units'].fillna('OnM Cost (2015 USD/MWh)', inplace=True)

    if variable_om[variable_om.value.isna()].empty:
        print('All required values available')
    else:
        for index,row in variable_om[variable_om.value.isna()].iterrows():
            print(f"WARNING: Variable O&M for {row['subRegion']}, {row['class2']}, {row['vintage']} is missing\n") 

    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        variable_om.to_csv(Path(f'./extracted_data/{gcam_scenario}_variable_om.csv'), index=False)
    else:
       pass

    # calculate variable O&M escalation between years
    vom_escalation_rate = variable_om[['subRegion', 'class2', 'vintage', 'x', 'value']].copy()

    # sort values by state, technology, vintage, and year
    vom_escalation_rate = vom_escalation_rate.sort_values(by=['subRegion', 'class2', 'vintage', 'x'])

    # collect previous year
    vom_escalation_rate['y'] = vom_escalation_rate['x'] - vom_escalation_rate.groupby(['subRegion', 'class2'])['x'].diff()

    # rename values
    df = variable_om[['subRegion', 'class2', 'vintage', 'x', 'value']].copy()
    df = df.rename(columns={'value':'value_t_5', 'x':'y'})[['subRegion', 'y', 'class2', 'value_t_5']]

    # merge dataframes
    vom_escalation_rate = vom_escalation_rate.merge(df, how='left', on=['subRegion','class2', 'y'])

    # calculate escalation rate
    vom_escalation_rate['esc_val'] = (vom_escalation_rate['value'] - vom_escalation_rate['value_t_5'] ) / vom_escalation_rate['value_t_5']

    # fill in zero for technologies with variable O&M of 0
    vom_escalation_rate['esc_val'] = np.where((vom_escalation_rate['value']==0) & (vom_escalation_rate['value_t_5']==0), 0, vom_escalation_rate['esc_val'])
    vom_escalation_rate = vom_escalation_rate.drop(['value'], axis=1) 

    # transform to expected format
    vom_escalation_rate = standardize_format(vom_escalation_rate, param='elec_variable_om_escl_rate_fraction',
                                scenario=gcam_scenario,
                                units='fraction', 
                                valueColumn='esc_val', vintageColumn='x')

    #fill first year values with 0
    vom_escalation_rate['value'].fillna(0, inplace=True)

    if save_output:
        vom_escalation_rate.to_csv(Path(f'./extracted_data/{gcam_scenario}_variable_om_esc_rate.csv'), index=False)
    else:
       pass

    return variable_om, vom_escalation_rate

def _get_variable_om(
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
        ):
  
  get_variable_om(
        path_to_gcam_database,
        gcam_file_name,
        gcam_scenario
  )


if __name__ == "__main__":
  _get_variable_om()