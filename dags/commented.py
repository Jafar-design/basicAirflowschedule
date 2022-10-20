
# cur, conn = connection.get_connection()


# gtbank_summary = '''
# SELECT DATE(time_in)Trx_date, 'AIRTIME' CATEGORY, CASE WHEN real_network IS NULL THEN network ELSE real_network END Network, COUNT(DISTINCT phonenumber)Unique_count,COUNT(phonenumber) Trx_count, FORMAT(SUM(amount),2) Trx_amount
# FROM gtb_airtime_request_logs         
# WHERE time_in BETWEEN DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00') AND DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
# AND completed_txn_status  NOT IN (2, 500,300)
# AND operation_id NOT LIKE '%-D%'
# AND txn_id NOT LIKE '%_repush%'
# AND txn_id NOT LIKE '%_R1%' AND client_id = 'ZW5HJKOK-983-POQ'
# GROUP BY 'AIRTIME', DATE(time_in), CASE WHEN real_network IS NULL THEN network ELSE real_network END
# UNION 
# SELECT DATE(time_in)Trx_date, 'DATA' CATEGORY, network, COUNT(DISTINCT phonenumber)Unique_count,COUNT(phonenumber) Trx_count,FORMAT(SUM(amount),2) Trx_amount
# FROM gtb_bundle_request_logs         
# WHERE time_in BETWEEN DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00') AND DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
# AND completed_txn_status NOT  IN (2, 500,300)
# AND operation_id NOT LIKE '%-D%'
# AND txn_id NOT LIKE '%_repush%'
# AND txn_id NOT LIKE '%_R1%' AND client_id = 'ZW5HJKOK-983-POQ'
# GROUP BY 'DATA', DATE(time_in), network
# '''



# now=datetime.now()
# @dag(
#     schedule='* * * * *',
#     start_date=datetime(2022,10,20),
#     catchup=False,
#     tags=['myFirstdag']
# )
# def doSummary():
#     @task
#     def extract():
#         cur.execute(gtbank_summary)
#         raw = cur.fetchall()
#         return raw
#     @task
#     def transform(raw):
#         df = DataFrame(raw, columns=['Trx_date', 'CATEGORY', 'Network',	'Unique_count',	'Trx_count'	'Trx_amount'])
#         return df
#     @task
#     def load(df):
#         df_csw.to_csv(f'csvTesting.csv', index=False)
    
#     extract = extract()
#     transform = transform(extract)
#     load = load(transform)

# doSummary = doSummary()




# import os
# import pandas as pd
# from datetime import datetime
# from airflow.models import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# from airflow.models import Variable


# with DAG(
#     dag_id='first_airflow_dag',
#     schedule_interval='* * * * *',
#     start_date=datetime(year=2022, month=10, day=19),
#     catchup=False
# ) as dag:
    
#     # 1. Get current datetime
#     task_get_datetime = BashOperator(
#         task_id='get_datetime',
#         bash_command='date'
#     )