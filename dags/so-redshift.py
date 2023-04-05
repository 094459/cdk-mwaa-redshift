from airflow import DAG
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="load_dag", 
    schedule_interval=None,
    start_date=days_ago(1), 
    tags=['load'],
    description='DAG to run datawarehouse.load',
    default_view='graph', #grid, graph, duration, gantt, landing_times
    catchup=False
    ) as dag:
    person_duplicate_check = RedshiftSQLOperator(
        task_id = 'person_duplicate_check',
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
        on_failure_callback = None,
        redshift_conn_id="airflow2redshift"
        )