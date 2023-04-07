# Example of how to use RedshiftSQLOperator
#
# Uploading and creating Redshift tables for the sample Redshift tickitdb sample data

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator

import boto3
import requests
import boto3
import zipfile
import io
from io import BytesIO

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
REDSHIFT_SCHEMA = 'mwaa'
DOWNLOAD_FILE = 'https://docs.aws.amazon.com/redshift/latest/gsg/samples/tickitdb.zip'
S3_FOLDER = 'sampletickit'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Python code to download file and upload it to a S3 bucket

def download_zip():
    s3c = boto3.client('s3')
    indata = requests.get(DOWNLOAD_FILE)
    n=0
    with zipfile.ZipFile(io.BytesIO(indata.content)) as z:       
        zList=z.namelist()
        print(zList)
        for i in zList: 
            print(i) 
            zfiledata = BytesIO(z.read(i))
            n += 1
            s3c.put_object(Bucket=S3_BUCKET, Key=S3_FOLDER+'/'+i, Body=zfiledata)

sample_data_tables_users_sql="""
create table IF NOT EXISTS public.users(
	userid integer not null distkey sortkey,
	username char(8),
	firstname varchar(30),
	lastname varchar(30),
	city varchar(30),
	state char(2),
	email varchar(100),
	phone char(14),
	likesports boolean,
	liketheatre boolean,
	likeconcerts boolean,
	likejazz boolean,
	likeclassical boolean,
	likeopera boolean,
	likerock boolean,
	likevegas boolean,
	likebroadway boolean,
	likemusicals boolean);
"""

sample_data_tables_venue_sql="""
create table IF NOT EXISTS public.venue(
	venueid smallint not null distkey sortkey,
	venuename varchar(100),
	venuecity varchar(30),
	venuestate char(2),
	venueseats integer);
"""
sample_data_tables_category_sql="""
create table IF NOT EXISTS public.category(
	catid smallint not null distkey sortkey,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50));
"""

sample_data_tables_date_sql="""
create table IF NOT EXISTS public.date(
	dateid smallint not null distkey sortkey,
	caldate date not null,
	day character(3) not null,
	week smallint not null,
	month character(5) not null,
	qtr character(5) not null,
	year smallint not null,
	holiday boolean default('N'));
    """
sample_data_tables_event_sql="""
create table IF NOT EXISTS public.event(
	eventid integer not null distkey,
	venueid smallint not null,
	catid smallint not null,
	dateid smallint not null sortkey,
	eventname varchar(200),
	starttime timestamp);
    """
sample_data_tables_listing_sql="""
create table IF NOT EXISTS public.listing(
	listid integer not null distkey,
	sellerid integer not null,
	eventid integer not null,
	dateid smallint not null  sortkey,
	numtickets smallint not null,
	priceperticket decimal(8,2),
	totalprice decimal(8,2),
	listtime timestamp);
    """
sample_data_tables_sales_sql="""
create table IF NOT EXISTS public.sales(
	salesid integer not null,
	listid integer not null distkey,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null sortkey,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);
"""

dag = DAG(
    'setup_sample_data_dag',
    default_args=default_args,
    description='Setup Sample Data from Redshift documentation',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

files_to_s3 = PythonOperator(
        task_id='files_to_s3',
        python_callable=download_zip,
        dag=dag
    )

create_sample_users_tables = RedshiftSQLOperator(
    task_id = 'create_sample_users_tables',
    sql=sample_data_tables_users_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_category_tables = RedshiftSQLOperator(
    task_id = 'create_sample_category_tables',
    sql=sample_data_tables_category_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_venue_tables = RedshiftSQLOperator(
    task_id = 'create_sample_venue_tables',
    sql=sample_data_tables_venue_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_date_tables = RedshiftSQLOperator(
    task_id = 'create_sample_date_tables',
    sql=sample_data_tables_date_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_listing_tables = RedshiftSQLOperator(
    task_id = 'create_sample_listing_tables',
    sql=sample_data_tables_listing_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_event_tables = RedshiftSQLOperator(
    task_id = 'create_sample_event_tables',
    sql=sample_data_tables_event_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )
create_sample_sales_tables = RedshiftSQLOperator(
    task_id = 'create_sample_sales_tables',
    sql=sample_data_tables_sales_sql,
    redshift_conn_id=REDSHIFT_CONNECTION_ID,
    dag=dag
    )

files_to_s3 >> create_sample_users_tables
files_to_s3 >> create_sample_category_tables
files_to_s3 >> create_sample_venue_tables
files_to_s3 >> create_sample_date_tables
files_to_s3 >> create_sample_listing_tables
files_to_s3 >> create_sample_sales_tables
files_to_s3 >> create_sample_event_tables