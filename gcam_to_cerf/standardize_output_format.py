import pandas as pd

def standardize_format(
    df:pd.DataFrame,
    param:str,
    units:str,
    valueColumn:str,
    scenario:str,
    vintageColumn='vintage',
    xColumn='year',
    xLabel='Year',
    classLabel2='technology',
    class2Column='technology',
    subRegionColumn='region',
    region='USA') -> pd.DataFrame:
    
    clone = df.copy()

    # add vintage column
    if vintageColumn in clone.columns:
        clone['vintage'] = 'Vint_' + clone[vintageColumn].astype(int).astype(str)
    else:
        clone['vintage'] = 'Vint_2015'

    # rename columns
    clone = clone.rename(columns={
        class2Column: 'class2',
        subRegionColumn: 'subRegion',
        valueColumn: 'value',
        xColumn: 'x',
        })
    
    # add year
    if not 'x' in clone.columns:
        clone['x'] = 2015
    
    # convert year to integer
    clone['x'] = clone['x'].astype(int)

    # add scenario name
    clone['scenario'] = scenario

    # add region name
    clone['region'] = region

    # add xlabel
    clone['xLabel'] = xLabel

    # add classlabel2
    clone['classLabel2'] = classLabel2

    # add parameter column
    clone['param'] = param

    # add units column
    clone['units'] = units

    return clone[['scenario', 'region', 'subRegion', 'param', 'classLabel2', 
                  'class2', 'xLabel', 'x', 'vintage', 'units', 'value']].reset_index(drop=True)

def _standardize_format(df,
    param,
    units,
    valueColumn):
  standardize_format(df,
    param,
    units,
    valueColumn)


if __name__ == "__main__":
  _standardize_format()