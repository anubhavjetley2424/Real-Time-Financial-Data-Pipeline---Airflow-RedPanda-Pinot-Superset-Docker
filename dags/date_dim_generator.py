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

num_rows = 100
output_file = './date_dim_large_data.csv'

def generate_random_date_data(row_num):
    year = random.randint(2015, 2025)
    month = random.randint(1, 12)
    day = random.randint(1, 28)  # Limiting to 28 to avoid month-end issues
    quarter = ((month - 1) // 3) + 1
    
    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 3650))
    transaction_date_millis = int(random_date.timestamp()) * 1000
    
    return month, day, year, quarter, transaction_date_millis

def generate_date_dim_data():
    # Initialize lists to store data
    months = []
    days = []
    years = []
    quarters = []
    transaction_dates = []

    # Generate data using a while loop
    row_num = 1
    while row_num <= num_rows:
        month, day, year, quarter, transaction_date_millis = generate_random_date_data(row_num)
        months.append(month)
        days.append(day)
        years.append(year)
        quarters.append(quarter)
        transaction_dates.append(transaction_date_millis)
        row_num += 1

    # Create a DataFrame
    df = pd.DataFrame({
        "month": months,
        "day": days,
        "year": years,
        "quarter": quarters,
        "transaction_date": transaction_dates
    })

    # Save DataFrame to CSV
    df.to_csv(output_file, index=False)

with DAG(dag_id='date_data_generator',
         default_args=default_args,
         description='A DAG to generate large date dimension data',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['dimension']) as dag:
    start = EmptyOperator(
        task_id='start_task',
    )

    generate_date_dim_data = PythonOperator(
        task_id='generate_date_dim_data',
        python_callable=generate_date_dim_data
    )

    end = EmptyOperator(
        task_id='end_task',
    )

    start >> generate_date_dim_data >> end