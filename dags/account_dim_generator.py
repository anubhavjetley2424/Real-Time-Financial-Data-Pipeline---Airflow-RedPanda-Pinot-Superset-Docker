import random
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

start_date = datetime(year=2025, month=8, day=25)
default_args = {
    'owner': 'anubhav',
    'depends_on_past': False,
    'backfill': False,
    'start_date' : start_date
}

num_rows = 50
output_file = './account_dim_large_data.csv'

account_ids = []
account_types = []
statuses = []
customer_ids = []
balances = []
opening_dates = []

def generate_random_data(row_num):
    account_id = f'A{row_num:05d}'
    account_type = random.choice(['SAVINGS', 'CHECKING'])
    status = random.choice(['ACTIVE', 'ACTIVE'])
    customer_id = f'C{random.randint(1, 1000):05d}'
    balance = round(random.uniform(100.00, 10000.00), 2)
    
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 365))
    opening_date_millis = int(random_date.timestamp() * 1000)
    
    return account_id, account_type, status, customer_id, balance, opening_date_millis

def generate_account_dim_data():
    row_num = 1
    while row_num <= num_rows:
        account_id, account_type, status, customer_id, balance, opening_date_millis = generate_random_data(row_num)
        account_ids.append(account_id)
        account_types.append(account_type)
        statuses.append(status)
        customer_ids.append(customer_id)
        balances.append(balance)
        opening_dates.append(opening_date_millis)
        row_num += 1

    df = pd.DataFrame({
        'account_id': account_ids,
        'account_type': account_types,
        'status': statuses,
        'customer_id': customer_ids,
        'balance': balances,
        'opening_date': opening_dates
    })

    df.to_csv(output_file, index=False)

with DAG(dag_id='account_dim_generator',
         default_args=default_args,
         description='Generate large account dimension data in a CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['schema']) as dag:
    start = EmptyOperator(
        task_id='start_task',
    )

    generate_account_dimension_data = PythonOperator(
        task_id='generate_account_dim_data',
        python_callable=generate_account_dim_data
    )

    end = EmptyOperator(
        task_id='end_task',
    )

    start >> generate_account_dimension_data >> end