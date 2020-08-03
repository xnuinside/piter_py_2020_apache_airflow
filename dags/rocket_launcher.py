import json
import os
from datetime import datetime
from random import choice

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import TaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from telebot import TeleBot


def toss_a_coin_fn():
    side = choice(['heads', 'tails'])
    if side == 'heads':
        return 'prepare_for_the_launch'
    return 'no_launch_for_today'


def send_message(message):
    bot = TeleBot(token=os.environ['TELEGRAM_BOT_TOKEN'], parse_mode='markdown')
    bot.send_message(chat_id='@piter_py_2020_aw', text=message)


def on_failure_callback(context):
    print(context)
    send_message(
        f'Oh no!'
        f'Task something went wrong!'
    )


default_args = {
    'dag_id': 'rocket_launcher',
    'start_date': datetime(2020, 7, 21),
    'schedule_interval': None,
    'max_active_runs': 1,
    'catchup': False,
    'on_failure_callback': on_failure_callback
}


def is_the_weather_acceptable_fn(**context):
    ti: TaskInstance = context['ti']
    weather_details_str = ti.xcom_pull('fetch_the_weather_details')
    print(weather_details_str)
    weather = json.loads(weather_details_str)
    if weather['current']['clouds'] < 50:
        return 'weather_is_acceptable'
    return 'toss_a_coin'


with DAG(**default_args) as dag:
    toss_a_coin = BranchPythonOperator(
        task_id='toss_a_coin',
        python_callable=toss_a_coin_fn
    )

    fetch_the_weather_details = SimpleHttpOperator(
        task_id='fetch_the_weather_details',
        endpoint='/data/2.5/onecall',
        http_conn_id='http_weather',
        method='GET',
        data={
            'lat': 60,
            'lon': 30,
            'exclude': 'minutely,hourly,daily',
            'appid': os.environ['WEATHER_API_KEY']
        },
        log_response=True,
        xcom_push=True
    )

    is_weather_acceptable = BranchPythonOperator(
        task_id='is_weather_acceptable',
        python_callable=is_the_weather_acceptable_fn,
        provide_context=True
    )

    prepare_for_the_launch = BashOperator(
        task_id='prepare_for_the_launch',
        bash_command='echo "Chief, we are ready for the start!"',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    weather_is_acceptable = DummyOperator(
        task_id='weather_is_acceptable'
    )

    feeling_lucky = DummyOperator(
        task_id='feeling_lucky'
    )

    launch_the_rocket = PostgresOperator(
        task_id='launch_the_rocket',
        postgres_conn_id='rocket_postgres',
        sql="""
            insert into launch_status (execution_ts, status)
            values ('{{ ts }}', %s)
        """,
        parameters=('success',)
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

    wait_for_approval >> \
        fetch_the_weather_details >> \
        is_weather_acceptable >> \
        weather_is_acceptable >> \
        prepare_for_the_launch >> \
        launch_the_rocket

    is_weather_acceptable >> \
        toss_a_coin >> \
        feeling_lucky >> \
        prepare_for_the_launch

    toss_a_coin >> no_launch_for_today
