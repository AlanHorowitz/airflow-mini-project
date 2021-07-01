import pandas as pd
from datetime import datetime, timedelta, date
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import yfinance as yf
import os

work_dir = os.getcwd()

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def yahoo_finance_data(symbol):
    start_date = date.today() - timedelta(days=1)
    df = yf.download(symbol, start=start_date, interval='1m')
    df.to_csv(symbol + '.csv', header=False)


with DAG('market_vol',
         default_args=default_args,
         description='A simple DAG',
         schedule_interval=timedelta(days=1)
         ) as dag:
    t0 = BashOperator(task_id='create_temp_dir',
                      bash_command='mkdir -p /tmp/data/2021-07-01')

    t1 = PythonOperator(task_id='download_AAPL',
                        python_callable=yahoo_finance_data,
                        op_args=['AAPL'])

    t2 = PythonOperator(task_id='download_TSLA',
                        python_callable=yahoo_finance_data,
                        op_args=['TSLA'])

    t3 = BashOperator(task_id='move_AAPL_to_data_location',
                      bash_command='mv AAPL.csv /tmp/data/2021-07-01/'
    )
