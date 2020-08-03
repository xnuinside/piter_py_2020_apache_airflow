from datetime import datetime
from random import choice

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator


def toss_a_coin_fn():
    side = choice(['heads', 'tails'])
    if side == 'heads':
        return 'prepare_for_the_launch'
    return 'no_launch_for_today'


with DAG(dag_id='rocket_launcher',
         start_date=datetime(2020, 7, 21),
         schedule_interval=None,
         max_active_runs=1,
         catchup=False) as dag:
    toss_a_coin = BranchPythonOperator(
        task_id='toss_a_coin',
        python_callable=toss_a_coin_fn
    )

    prepare_for_the_launch = BashOperator(
        task_id='prepare_for_the_launch',
        bash_command='echo "Chief, we are ready for the start!"'
    )

    launch_the_rocket = BashOperator(
        task_id='launch_the_rocket',
        bash_command='echo "Chief, rocket is launched!"'
    )

    no_launch_for_today = BashOperator(
        task_id='no_launch_for_today',
        bash_command='echo "Chief, no launch :("'
    )

    wait_for_approval = FileSensor(
        task_id='wait_for_approval',
        filepath='/opt/airflow/files/approved',
        poke_interval=10
    )

    wait_for_approval >> toss_a_coin >> [no_launch_for_today, prepare_for_the_launch]
    prepare_for_the_launch >> launch_the_rocket
