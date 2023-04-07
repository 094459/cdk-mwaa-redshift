# Example of how to use RedshiftDataOperator
#
# Creating the troubleshooting view

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

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
    'create_stl_view',
    default_args=default_args,
    description='Creating troubleshooting view in Redshift',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_view_sql = """
create view loadview as
(select distinct tbl, trim(name) as table_name, query, starttime,
trim(filename) as input, line_number, colname, err_code,
trim(err_reason) as reason
from stl_load_errors sl, stv_tbl_perm sp
where sl.tbl = sp.id);
"""

create_redshift_tblshting_view = RedshiftDataOperator(
    task_id="create_redshift_tblshting_view",
    cluster_identifier=REDSHIFT_CLUSTER,
    database=REDSHIFT_SCHEMA,
    db_user=REDSHIFT_USER,
    sql=create_view_sql,
    poll_interval=POLL_INTERVAL,
    wait_for_completion=True,
    dag=dag
)

create_redshift_tblshting_view