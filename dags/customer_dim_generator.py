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
output_file = './customer_dim_large_data.csv'

def generate_random_data(row_num):
    customer_id = f"C{row_num:05d}"
    first_name = f"FirstName{row_num}"
    last_name = f"LastName{row_num}"
    email = f"customer{row_num}@example.com"
    phone_number = f"+1-800-{random.randint(1000000, 9999999)}"  # Fixed randint syntax

    now = datetime.now()
    random_date = now - timedelta(days=random.randint(0, 3650))  # Fixed randint syntax
    registration_date_millis = int(random_date.timestamp()) * 1000
    return customer_id, first_name, last_name, email, phone_number, registration_date_millis

def generate_customer_dim_data():
    # Initialize lists to store data
    customer_ids = []
    first_names = []
    last_names = []
    emails = []
    phone_numbers = []
    registration_dates = []

    # Generate data using a while loop
    row_num = 1
    while row_num <= num_rows:
        customer_id, first_name, last_name, email, phone_number, registration_date_millis = generate_random_data(row_num)
        customer_ids.append(customer_id)
        first_names.append(first_name)
        last_names.append(last_name)
        emails.append(email)
        phone_numbers.append(phone_number)
        registration_dates.append(registration_date_millis)
        row_num += 1

    # Create a DataFrame
    df = pd.DataFrame({
        "customer_id": customer_ids,
        "first_name": first_names,
        "last_name": last_names,
        "email": emails,
        "phone_number": phone_numbers,
        "registration_date": registration_dates
    })

    # Save DataFrame to CSV
    df.to_csv(output_file, index=False)

with DAG(dag_id='customer_dim_generator',
         default_args=default_args,
         description='A DAG to generate large customer dimension data',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['dimension']) as dag:
    start = EmptyOperator(
        task_id='start_task',
    )

    generate_customer_dim_data = PythonOperator(
        task_id='generate_customer_dim_data',
        python_callable=generate_customer_dim_data
    )

    end = EmptyOperator(
        task_id='end_task',
    )

    start >> generate_customer_dim_data >> end