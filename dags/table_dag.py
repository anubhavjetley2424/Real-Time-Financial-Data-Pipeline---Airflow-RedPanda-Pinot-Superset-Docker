from datetime import datetime, timedelta

import pandas as pd
import glob
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
import requests
from typing import Any

class PinotTableSubmitOperator(BaseOperator):
    @apply_defaults
    def __init__(self, folder_path, pinot_url, *args, **kwargs):
        super(PinotTableSubmitOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Any:
        try:
            table_files = glob.glob(self.folder_path + '/*.json')
            for table_file in table_files:
                with open(table_file, 'r') as file:
                    table_data = file.read()

                    headers = {'Content-Type': 'application/json'}
                    response = requests.post(self.pinot_url, heades=headers, data=table_data)
                    
                    if response.status_code == 200:
                        self.log.info(f'Table successfully submitted to Apache Pinot')
                    else:
                        self.log.error(f'Failed to submit Table : {response.status_code}')
                        raise Exception(f'Table submission failed with status code {response.status_code}')
                    

        except Exception as e:
            self.log.error(f'An error occurred: {str(e)}')





start_date = datetime(year=2025, month=8, day=25)

default_args = {
    'owner': 'anubhav',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date
}

with DAG(dag_id='table_dag',
         default_args=default_args,
         description='A DAG to submit all table in a folder to Apache Pinot',
         schedule_interval=timedelta(days=1),
         start_date=start_date,
         tags=['table']) as dag:
    
    start = EmptyOperator(
        task_id='start_task'
    )

    submit_tables = PinotTableSubmitOperator(
        task_id='submit_tables',
        folder_path='/opt/airflow/dags/tables',
        pinot_url='http://pinot-controller:9000/tables'
    )

    end = EmptyOperator(
        task_id='end_task'
    )

    start >> submit_tables >> end