import pandas as pd
from datetime import datetime, timedelta, date
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import yfinance as yf
import os.path

work_dir = os.getcwd()
temp_dir = '/tmp/data'

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def create_report(report_date):
    pass


def yahoo_finance_data(symbol, report_date):
    out_filename = symbol + '.csv'
    start_date = date.fromisoformat(report_date)
    end_date = start_date - timedelta(days=1)
    df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    df.to_csv(os.path.join(work_dir, out_filename), header=False)


with DAG('market_vol',
         default_args=default_args,
         description='A simple DAG',
         schedule_interval="0 18 * * 1-5"
         ) as dag:

    t0 = BashOperator(task_id='create_temp_dir',
                      bash_command='mkdir -p {{params.temp_dir}}/{{ ds }}',
                      params={'temp_dir': f'{temp_dir}'})

    t1 = PythonOperator(task_id='download_AAPL',
                        python_callable=yahoo_finance_data,
                        op_kwargs={'symbol': 'AAPL', 'report_date': '{{ ds }}'})

    t2 = PythonOperator(task_id='download_TSLA',
                        python_callable=yahoo_finance_data,
                        op_args=['TSLA'])

    t3 = BashOperator(task_id='move_AAPL_to_data_location',
                      bash_command=f'mv {work_dir}/AAPL.csv {temp_dir}/2021-07-01/')

    t4 = BashOperator(task_id='move_TSLA_to_data_location',
                      bash_command=f'mv {work_dir}/TSLA.csv {temp_dir}/2021-07-01/')

    t5 = PythonOperator(task_id='daily_report',
                        python_callable=create_report)
