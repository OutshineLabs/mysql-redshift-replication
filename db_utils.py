import boto3
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from helpers import setup_logging

logger = setup_logging('load')

# load dotenv in the base root
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))  #refers to application_top
dotenv_path = os.path.join(ROOT_DIR, '.env')

load_dotenv(dotenv_path)

DB_USER = os.getenv('WS_RS_USER')
DB_SECRET = os.getenv('WS_RS_SECRET')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')


S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'dev-bucket')


if not DB_USER or not DB_SECRET:
    raise Exception('Environment Variables missing.')
    exit(0)


def execute_query(source_config, sql):
    """ Executes SQL query against source database. Used for reading 
    data into ETL process.

    Parameters
    ----------
    sql : sql query to be executed against source db

    Returns
    -------
    Dataframe with results from SQL query

    """

    conn_str = (
        "{engine}://{user}:"
        "{password}@{host}"
        ":{port}/{database}".format(
            engine=engine
            , user=user
            , password=password
            , host=host
            , port=port
            , database=database
        )
    )

    engine = create_engine(conn_str)
    return pd.read_sql_query(sql, con=engine)


def extract_table_into_dataframe(source_config, table_name):
    """ Executes SQL query against source database. Used for reading 
    data into ETL process.

    Parameters
    ----------
    sql : sql query to be executed against source db

    Returns
    -------
    Dataframe with results from SQL query

    """

    conn_str = (
        "{engine}://{user}:"
        "{password}@{host}"
        ":{port}/{database}".format(
            engine='mysql'
            , user=config['USER']
            , password=config['PASSWORD']
            , host=config['HOST']
            , port=config['PORT']
            , database=config['DATABASE']
        )
    )

    engine = create_engine(conn_str)
    return pd.read_sql_table(table_name=table_name, con=engine)



def run_redshift_cmd(sql):
    """ Runs SQL commands against a Redshift destination.

    Parameters
    ----------
    sql : Dataframe

    Returns
    -------
    True if query executes, False on error
    
    """

    conn_str = (
        "{engine}://{user}:"
        "{password}@{host}"
        ":{port}/{database}".format(
            engine='postgresql'
            , user=config['USER']
            , password=config['PASSWORD']
            , host=config['HOST']
            , port=config['PORT']
            , database=config['DATABASE']
        )
    )

    engine = create_engine(conn_str)

    try:
        with engine.connect() as conn:
            conn.execute(sql)
        return True
    except Exception as e:
        logger.error("""Error occurred during query"""
                         """execution with code: {e}""".format(e=e))
        return False


def limit(sql, row_limit):
    return sql + " LIMIT " + str(row_limit)


def _write_df_to_tmp_file(df, table_name, run_date):

    tmp_file_name = "./output/{table_name}_{run_date}.csv.gz".format(
        table_name=table_name,
        run_date=run_date
    )

    df.to_csv(tmp_file_name,
              sep="|",
              index=set_df_index,
              compression='gzip')

    return tmp_file_name


def _generate_s3_key(file_prefix, run_date):
    return 'output/{file_prefix}_{run_date}'.format(
        file_prefix=file_prefix,
        run_date=run_date
    )


def sync_to_s3(df, table_name, run_date):
    """ Write pandas DataFrame to an S3 bucket.

    Intermediate step of writing the dataframe contents
    to a temporary file

    Parameters
    ----------
    sql : Dataframe
    s3_bucket : bucket name

    Returns
    -------
    key of successfully uploaded file
    """

    session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    s3 = session.client('s3')

    filename = _write_df_to_tmp_file(df, table_name, run_date)
    bucket_name = S3_BUCKET_NAME
    key = _generate_s3_key(table_name, run_date)

    try:
        s3.upload_file(filename, bucket_name, key)
    except Exception as e:
        logger.error("""S3 File Upload failed"""
                         """error code: {e}""".format(e=e))
        return None

    return key



def run_truncate_table(schema, table_name):

    query_str = "TRUNCATE {schema}.{table_name}".format(
        schema=schema,
        table_name=table_name
    )

    run_redshift_cmd(query_str)



def write_df_to_redshift(df, schema, table_name, run_date=None, overwrite=True):
    """ Write pandas DataFrame to a Redshift target.

    First copies dataframe contents to S3, and then loads to Redshift

    Parameters
    ----------
    sql : Dataframe
    s3_bucket : bucket name

    Returns
    -------
    True 
    """

    s3_key = sync_to_s3(df, table_name, run_date)

    if overwrite:
        run_truncate_table(schema, table_name)

    if s3_key:
        copy_query_template = """
            COPY {schema}.{table} from 's3://{s3_bucket}/{key}'
            credentials 'aws_access_key_id={aws_access_key_id};aws_secret_access_key={aws_secret_access_key}'
            gzip ignoreheader 1;
            commit;
        """.format(
            schema=schema,
            table=table_name,
            s3_bucket=S3_BUCKET_NAME,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            key=s3_key
        )

        run_redshift_cmd(copy_query_template)
