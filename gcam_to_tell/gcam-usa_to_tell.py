import click
import gcamreader
import pandas as pd
from pathlib import Path


def get_gcam_electricity_load(
  path_to_gcam_database: str,
  path_to_output_file: str,
  path_to_query_file: str = './gcam-usa_to_tell_queries.xml',
):

  # create a Path from str
  db_path = Path(path_to_gcam_database)

  # create connection to gcam db
  conn = gcamreader.LocalDBConn(db_path.parent, db_path.stem)

  # parse the queries file
  queries = gcamreader.parse_batch_query(path_to_query_file)

  # run the load query; note that we're assuming it's the first query in the file
  load = conn.runQuery(queries[0])

  # TODO update the scenario field to use official IM3 scenario names

  # aggregate the load
  aggregated = load[
    (load.input == 'electricity domestic supply')
  ][['scenario', 'region', 'Year', 'value']].groupby(['scenario', 'region', 'Year']).sum().reset_index().rename(columns={
      'Year': 'x',
  })

  # add columns to make output the same as with gcamextractor
  aggregated['subRegion'] = aggregated['region']
  aggregated['param'] = 'energyFinalConsumBySecEJ'
  aggregated['units'] = 'Final Energy by Sector (EJ)'
  aggregated['xLabel'] = 'Year'
  aggregated['vintage'] = 'Vint_' + aggregated['x'].astype(int).astype(str)

  # reorder the columns
  aggregated = aggregated[[
    'scenario', 'region', 'subRegion', 'param', 'xLabel', 'x', 'vintage', 'units', 'value',
  ]]

  # write to csv
  aggregated.to_csv(path_to_output_file, index=False)


@click.command()
@click.option(
    '--path-to-gcam-database',
    default='./database_basexdb_BAU',
    type=click.Path(
        file_okay=False,
        dir_okay=True,
        writable=False,
        resolve_path=True,
    ),
    required=True,
    prompt='What is the path to the directory containing the GCAM-USA database files?',
    help="""Path to the directory containing GCAM-USA database files."""
)
@click.option(
    '--path-to-output-file',
    default='./gcam-usa_electricity_load.csv',
    type=click.Path(
        file_okay=True,
        dir_okay=False,
        writable=True,
        resolve_path=True,
    ),
    required=True,
    prompt='Where should the output CSV file be written?',
    help="""Path to which the output CSV file should be written."""
)
def _get_gcam_electricity_load(path_to_gcam_database, path_to_output_file):
  get_gcam_electricity_load(path_to_gcam_database, path_to_output_file)


if __name__ == "__main__":
  _get_gcam_electricity_load()

