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
    'start_date': datetime(2021, 7, 6),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def create_report(report_date, in_out_dir):
    """ Custom report showing data for the minutes with the highest volume and volatility."""
    df_cols = ['Datetime','Open','High','Low','Close','Adj Close','Volume']
    tickers = ['AAPL', 'TSLA']
    output_dfs = []
    for ticker in tickers:
        in_filename = f'{ticker}-{report_date}.csv'
        out_filename = f'market-vol-report-{report_date}.csv'
        df = pd.read_csv(os.path.join(in_out_dir, in_filename), header=None, names=df_cols)

        df['Swing'] = abs(df['High'] - df['Low'])
        max_swing_df = df[df['Swing'] == max(df['Swing'])].copy()
        max_swing_df['Type'] = 'Max Swing'
        max_swing_df['Ticker'] = ticker

        max_volume_df = df[df['Volume'] == max(df['Volume'])].copy()
        max_volume_df['Type'] = 'Max Volume'
        max_volume_df['Ticker'] = ticker

        # Add dataframes for both metrics per ticker
        output_dfs.extend([max_swing_df, max_volume_df])

    # Concatenate report pieces and format
    report = pd.concat(output_dfs)
    report["Time"] = report['Datetime'].str.slice(start=11)
    report = report.drop(columns=['Datetime', 'Open', 'Close', 'Adj Close']).set_index(['Type', 'Ticker', 'Time'])
    report.to_csv(os.path.join(in_out_dir, out_filename), header=True)


def yahoo_finance_data(symbol, report_date, out_dir):
    out_filename = symbol + '.csv'
    start_date = date.fromisoformat(report_date)
    end_date = start_date + timedelta(days=1)
    df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    df.to_csv(os.path.join(out_dir, report_date, out_filename), header=False)


with DAG('market_vol',
         default_args=default_args,
         description='A simple DAG',
         schedule_interval="0 22 * * 1-5"
         ) as dag:

    t0 = BashOperator(task_id='create_temp_dir',
                      bash_command='mkdir -p {{params.temp_dir}}/{{ ds }}',
                      params={'temp_dir': temp_dir})

    t1 = PythonOperator(task_id='download_AAPL',
                        python_callable=yahoo_finance_data,
                        op_kwargs={'symbol': 'AAPL', 'report_date': '{{ ds }}', 'out_dir': temp_dir})

    t2 = PythonOperator(task_id='download_TSLA',
                        python_callable=yahoo_finance_data,
                        op_kwargs={'symbol': 'TSLA', 'report_date': '{{ ds }}', 'out_dir': temp_dir})

    t3 = BashOperator(task_id='move_AAPL',
                      bash_command='mv {{params.temp_dir}}/{{ ds }}/AAPL.csv {{params.work_dir}}/AAPL-{{ ds }}.csv',
                      params={'temp_dir': temp_dir, 'work_dir': work_dir})

    t4 = BashOperator(task_id='move_TSLA',
                      bash_command='mv {{params.temp_dir}}/{{ ds }}/TSLA.csv {{params.work_dir}}/TSLA-{{ ds }}.csv',
                      params={'temp_dir': temp_dir, 'work_dir': work_dir})

    t5 = PythonOperator(task_id='create_report',
                        python_callable=create_report,
                        op_kwargs={'report_date': '{{ ds }}', 'in_out_dir': work_dir})

    # The download and move tasks can run concurrently for a ticker symbol
    t0 >> [t1, t2]
    t1 >> t3
    t2 >> t4
    [t3, t4] >> t5
