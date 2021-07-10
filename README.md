## Airflow Mini Project 1

### Download and process AAPL and TSLA trade activity on a daily basis with Airflow.  Run download and data movement concurrently using LocalExecutor.

### Environment

- pipenv
- pipenv install apache-airflow
- pipenv install 'apache-airflow[postgres]'
- sudo apt install postgresql
- sudo -u postgres psql
    - From psql prompt: alter user postgres password 'postgres';
- In AIRFLOW_HOME/airflow.cfg, use
  - executor = LocalExecutor
  - sql_alchemy_conn = postgresql+psycopg2://postgres:postgres@localhost/postgres
- git clone https://github.com/AlanHorowitz/https://github.com/AlanHorowitz/airflow-mini-project-1.git
- copy dags to AIRFLOW_HOME
- From AIRFLOW_HOME
  - airflow users create \
    --username admin \
    --firstname FIRST_NAME \
    --lastname LAST_NAME \
    --role Admin \
    --email admin@example.org
  - airflow init db

### Demonstration

``` 
    airflow webserver
    airflow scheduler
```

- Unpause market_vol DAG from Airflow UI

### Results 

Dag console after 3 daily runs

![console](./images/VirtualBox_pySpark_10_07_2021_14_59_32.png)
- Example of daily analysis report (market-vol-report-2021-07-08.csv).  Captures data from the minutes with highest volume and maximum price swing for AAPL and TSLA. 
``` 
Type,Ticker,Time,High,Low,Volume,Swing
Max Swing,AAPL,09:30:00-04:00,142.25,141.36000061035156,7134480,0.8899993896484375
Max Volume,AAPL,09:30:00-04:00,142.25,141.36000061035156,7134480,0.8899993896484375
Max Swing,TSLA,09:30:00-04:00,629.6998901367188,620.4600219726562,1549225,9.2398681640625
Max Volume,TSLA,09:30:00-04:00,629.6998901367188,620.4600219726562,1549225,9.2398681640625

```
