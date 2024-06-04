import gcamreader
import numpy as np
import pandas as pd
from pathlib import Path
from standardize_output_format import *
import os

tech_bin = {'CSP (dry_hybrid)': 'CSP',
            'CSP (recirculating)': 'CSP',
            'Gen_III (cooling pond)': 'Nuclear',
            'Gen_III (once through)': 'Nuclear',
            'Gen_III (recirculating)': 'Nuclear',
            'Gen_III (seawater)': 'Nuclear',
            'PV': 'Solar PV',
            'biomass (IGCC CCS) (dry cooling)': 'Biomass CCS',
            'biomass (IGCC CCS) (recirculating)': 'Biomass CCS',
            'biomass (IGCC CCS) (seawater)': 'Biomass CCS',
            'biomass (IGCC) (dry cooling)': 'Biomass',
            'biomass (IGCC) (recirculating)': 'Biomass',
            'biomass (IGCC) (seawater)': 'Biomass',
            'biomass (conv CCS) (dry cooling)': 'Biomass CCS',
            'biomass (conv CCS) (recirculating)': 'Biomass CCS',
            'biomass (conv CCS) (seawater)': 'Biomass CCS',
            'biomass (conv) (cooling pond)': 'Biomass',
            'biomass (conv) (dry cooling)': 'Biomass',
            'biomass (conv) (recirculating)': 'Biomass',
            'biomass (conv) (seawater)': 'Biomass',
            'coal (IGCC CCS) (dry cooling)': 'Coal CCS',
            'coal (IGCC CCS) (recirculating)': 'Coal CCS',
            'coal (IGCC CCS) (seawater)': 'Coal CCS',
            'coal (conv pul CCS) (dry cooling)': 'Coal CCS',
            'coal (conv pul CCS) (recirculating)': 'Coal CCS',
            'coal (conv pul CCS) (seawater)': 'Coal CCS',
            'gas (CC CCS) (dry cooling)': 'Gas CCS',
            'gas (CC CCS) (recirculating)': 'Gas CCS',
            'gas (CC CCS) (seawater)': 'Gas CCS',
            'gas (CC) (cooling pond)': 'Gas',
            'gas (CC) (dry cooling)': 'Gas',
            'gas (CC) (recirculating)': 'Gas',
            'gas (CC) (seawater)': 'Gas',
            'gas (CT) (cooling pond)': 'Gas',
            'gas (CT) (dry cooling)': 'Gas',
            'gas (CT) (recirculating)': 'Gas',
            'gas (CT) (seawater)': 'Gas',
            'wind': 'Wind',
            'wind_offshore': 'Wind Offshore'}

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
    
    # split out pre-existing capacity rows
    prev_capacity = capacity.copy()
    prev_capacity['vintage'] = prev_capacity['vintage'].str.split("_", expand=True)[1].astype(int)
    prev_capacity = prev_capacity[prev_capacity.vintage < 2020]
    prev_capacity['vintage'] = "Vint_" + prev_capacity['vintage'].astype(str)

    capacity['vintage'] = capacity['vintage'].str.split("_", expand=True)[1].astype(int)
    capacity = capacity[capacity.vintage >= 2020]
    capacity['vintage'] = "Vint_" + capacity['vintage'].astype(str)

    # add a main technology identifier column
    capacity['tech_bin'] = capacity.class2.map(tech_bin)

    # remove rows with once through and seawater cooling and calculate the total capacity of all other cooling types
    capacity_total = capacity[~capacity.class2.str.contains('once through|seawater')].copy()
    capacity_total = capacity_total.groupby(['scenario', 'region', 'subRegion', 'param', 'tech_bin', 'xLabel','classLabel2', 'x','vintage', 'units'], as_index=False).sum()
    capacity_total = capacity_total.drop(['class2'], axis=1)
    capacity_total = capacity_total.rename(columns={'value':'total'})

    # merge with main capacity dataframe
    capacity_final = pd.merge(capacity, capacity_total, how= 'left')
    capacity_final['fraction'] = capacity_final['value'] / capacity_final['total']

    # select rows with once through cooling or seawater only
    capacity_otc_sea = capacity[capacity.class2.str.contains('once through|seawater')].copy()
    capacity_otc_sea= capacity_otc_sea.drop(['class2'], axis=1)
    capacity_otc_sea = capacity_otc_sea.groupby(['scenario', 'region', 'subRegion', 'param', 'tech_bin', 'xLabel','classLabel2', 'x','vintage', 'units'],
                                            as_index=False).sum()
    capacity_otc_sea = capacity_otc_sea.rename(columns={'value':'otc_sea_total'})

    # merge with capacity dataframe
    capacity_final = pd.merge(capacity_final, capacity_otc_sea, how= 'left')
    capacity_final['otc_sea_total'].fillna(0, inplace=True)
    capacity_final['fraction'].fillna(0, inplace=True)

    # for states that have only seawater cooling, replace with recirculating
    capacity_final['class2'] = np.where((capacity_final.class2.str.contains('seawater')) & (capacity_final.total.isna()), 
                                        capacity_final['class2'].str.replace("(seawater)", "(recirculating)"),
                                        capacity_final['class2'])

    # for states that have only once through cooling, replace with recirculating
    capacity_final['class2'] = np.where((capacity_final.class2.str.contains('once through')) & (capacity_final.total.isna()), 
                                        capacity_final['class2'].str.replace("(once through)", "(recirculating)"),
                                        capacity_final['class2'])

    # calculate new capacity value for other technologies by reallocating once through cooling capacity by weighted fractions
    capacity_final['value'] = capacity_final['value'] + (capacity_final['otc_sea_total'] * capacity_final['fraction'])

    # drop once-through and seawater rows from main dataframe along with helper columns
    capacity_final = capacity_final[~capacity_final.class2.str.contains('once through|seawater')]
    capacity_final = capacity_final.drop(['tech_bin', 'total', 'fraction', 'otc_sea_total'], axis=1)

    # stack pre-existing capacity rows back on the main dataframe
    capacity_final = pd.concat([capacity_final, prev_capacity])
    
    if save_output:
        os.makedirs(Path('./extracted_data'), exist_ok=True)
        capacity_final.to_csv(Path(f'./extracted_data/{gcam_scenario}_capacity.csv'), index=False)
    else:
       pass
    
    # create a dataframe of new capacity by vintage to check other parameters with
    capacity_crosscheck = capacity_final.copy()
    capacity_crosscheck['vintage'] = capacity_crosscheck['vintage'].str.split("_", expand=True)[1].astype(int)
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.vintage == capacity_crosscheck.x]
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.x >= 2020]
    capacity_crosscheck = capacity_crosscheck.drop(['param', 'classLabel2', 'units', 'value'], axis=1)
    capacity_crosscheck['vintage'] = "Vint_" + capacity_crosscheck['vintage'].astype(str)
    capacity_crosscheck = capacity_crosscheck[capacity_crosscheck.class2 != 'hydro']

    return capacity_final, capacity_crosscheck


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
