from datetime import datetime
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python_operator import PythonOperator
import boto3
import json

POLL_INTERVAL = 10
DAG_ID = 'example_redshift'
DB_LOGIN = 'awsuser'
DB_NAME = 'dev'
CLUSTER = "mwaa-redshift"

def get_results(**kwargs):
    print("Getting Results")
    ti = kwargs['task_instance']
    #runqueryoutput = ti.xcom_pull(key='return_value', task_ids='run_query')
    #print(runqueryoutput)
    #payload = json.loads(runqueryoutput)
    #query_id = payload['RedshiftQueryId']
    query_id = "b468809a-7484-4dca-b9bb-add406b0f770"
    client = boto3.client('redshift-data', region_name='eu-west-1')
    response = client.get_statement_result(Id=query_id)
    query_data = response['Records']
    print(query_data)
    return query_data

with DAG(
    dag_id=DAG_ID, 
    schedule_interval=None,
    start_date=days_ago(1), 
    tags=['load'],
    description='DAG to run datawarehouse.load',
    default_view='graph', #grid, graph, duration, gantt, landing_times
    catchup=False
    ) as dag:
        run_query = RedshiftDataOperator(
               task_id='run_query',
               cluster_identifier=CLUSTER,
               database=DB_NAME,
               db_user=DB_LOGIN,
               sql="""
                        with x as (
                select
                    sellerid,
                    count(1) as rows_count
                from listing
                where sellerid is not null
                group by sellerid
                    )
                    select
                        count(1) as id_count,
                        count(case when rows_count > 1 then 1 end) as duplicate_id_count,
                        sum(rows_count) as total_row_count,
                        sum(case when rows_count > 1 then rows_count else 0 end) as duplicate_row_count
                    from x;
                        """,
                poll_interval=POLL_INTERVAL,
                await_result=True,
                )
        get_query_results = PythonOperator(
              task_id='get_query',
              python_callable=get_results
              )
        run_query >> get_query_results