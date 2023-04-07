# Example of how to use RedshiftToS3Transfer
#
# Ingesting the sample Redshift sampletickitdb sample data

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator

# Replace these with your own values. You will need to create a Connection
# in the Apache Airflow Admin section using the Amazon Redshift connection type
# in the extras field update the values for your setup
#
# { 
#  "iam": true,
#  "cluster_identifier": "{your_redshift_cluster}",
#  "port": 5439, 
#  "region": "{aws_region}",
#  "db_user": "{redshift_username}",
#  "database": "{redshfit_db}"
# }

REDSHIFT_CONNECTION_ID = 'default_redshift_connection'
S3_BUCKET = 'mwaa-094459-redshift'
S3_EXPORT_FOLDER = 'export'
REDSHIFT_SCHEMA = 'mwaa'
REDSHIFT_TABLE = 'public.sales'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'redshift_to_s3_dag',
    default_args=default_args,
    description='Export CSV from Redshift to S3',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

transfer_redshift_to_s3 = RedshiftToS3Operator(
    task_id="transfer_redshift_to_s3",
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    s3_bucket=S3_BUCKET,
    s3_key=S3_EXPORT_FOLDER,
    schema=REDSHIFT_SCHEMA,
    table=REDSHIFT_TABLE,
    dag=dag
)

transfer_redshift_to_s3