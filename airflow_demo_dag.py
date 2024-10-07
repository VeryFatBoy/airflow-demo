from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import date

default_arg = {
        'owner' : 'airflow',
        'start_date' : str(date.today())
}

dag = DAG(
        'simple-s2-dag',
        default_args = default_arg,
        schedule_interval = '0 0 * * *'
)

s2_task = MySqlOperator(
        task_id = 's2_task',
        mysql_conn_id = 'mysql_default',
        autocommit = True,
        sql = 'airflow_demo.sql',
        params = {'test_user_id': -99},
        dag = dag
)

s2_task
