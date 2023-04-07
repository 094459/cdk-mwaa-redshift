# Example of how to use XCOM with RedshiftDataOperator
#
# Creating the troubleshooting view

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators.python_operator import PythonOperator
import boto3

REDSHIFT_CLUSTER = 'mwaa-redshift-cluster'
REDSHIFT_USER = 'awsuser'
REDSHIFT_SCHEMA = 'mwaa'
POLL_INTERVAL = 10

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'run_query_results_xcom',
    default_args=default_args,
    description='Run a query and see results in xcom',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

query_sql = """
            with x as (
                select
                    sellerid,
                    count(1) as rows_count
                from public.listing
                where sellerid is not null
                group by sellerid
                    )
                    select
                        count(1) as id_count,
                        count(case when rows_count > 1 then 1 end) as duplicate_id_count,
                        sum(rows_count) as total_row_count,
                        sum(case when rows_count > 1 then rows_count else 0 end) as duplicate_row_count
                    from x;
"""

def get_results(**kwargs):
    print("Getting Results")
    ti = kwargs['task_instance']
    runqueryoutput = ti.xcom_pull(key='return_value', task_ids='query_sql')
    client = boto3.client('redshift-data', region_name='eu-west-1')
    response = client.get_statement_result(Id=runqueryoutput)
    query_data = response['Records']
    print(query_data)
    return query_data

query_sql = RedshiftDataOperator(
    task_id="query_sql",
    cluster_identifier=REDSHIFT_CLUSTER,
    database=REDSHIFT_SCHEMA,
    db_user=REDSHIFT_USER,
    sql=query_sql,
    poll_interval=POLL_INTERVAL,
    wait_for_completion=True,
    dag=dag
)

get_query_results = PythonOperator(
    task_id='get_query_results',
    python_callable=get_results,
    dag=dag
    )

query_sql >> get_query_results