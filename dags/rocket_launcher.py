import os
from datetime import datetime
from random import choice

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator


def toss_a_coin_fn():
    side = choice(['heads', 'tails'])
    if side == 'heads':
        return 'prepare_for_the_launch'
    return 'no_launch_for_today'


default_args = {
    'dag_id': 'rocket_launcher',
    'start_date': None,
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False
}

with DAG(**default_args) as dag:
    toss_a_coin = BranchPythonOperator(
        task_id='toss_a_coin',
        python_callable=toss_a_coin_fn
    )

    fetch_the_weather_details = SimpleHttpOperator(
        task_id='fetch_the_weather_details',
        endpoint='/data/2.5/onecall',
        method='GET',
        http_conn_id='api.openweathermap.org',
        data={
            'lat': 60,
            'lon': 30,
            'exclude': 'minutely,hourly,daily',
            'appid': os.environ['WEATHER_API_KEY']
        }
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
