"""
Example code to demonstrate MySQL -> Redshift replication using pandas

"""
import click
from datetime import datetime
from db_utils import extract_table_into_dataframe, write_to_redshift
from helpers import setup_logging
import json
import sys


logger = setup_logging('etl')

with open('config.json', 'r') as f:
	config = json.load(f)


@click.group()
def cli():
	pass

@cli.command()
@click.option('--run_date', 
			  type=str,
			  default=datetime.today().strftime('%Y-%m-%d'),
			  help='Please enter the date to run the ETL script for')
@click.option('--table_name', 
			  type=str,
			  default=None,
			  help='Please enter the table name to replicate')
@click.option('--debug', 
			  type=bool,
			  default=True,
			  help='Debug run for local testing only')
@click.option('--backfill', 
			  type=bool,
			  default=False,
			  help='Updates tables for ')
def run(run_date, debug, backfill):
    logger.info("""Starting ETL run for {run_date} """
    			"""with backfill={backfill}""".format(run_date=run_date, 
    												  backfill=backfill))

    replicate(debug=debug,
    		  run_date=run_date,
    		  backfill=backfill)

    logger.info("ETL run for {run_date} complete".format(run_date=run_date))



def replicate_table(debug, table_name, run_date, backfill):
	source_config = config['SOURCE']
	df = extract_table_into_dataframe(source_config, table_name)

	write_to_redshift(df)


if __name__ == '__main__':

    sys.exit(cli())
  
