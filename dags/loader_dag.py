from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

start_date = datetime(year=2024, month=9, day=15)

default_args = {
    'owner': 'codewiththu',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date
}

with DAG(
    dag_id='dimension_batch_ingestion',
    default_args=default_args,
    description='A DAG to ingest dimension data into Pinot',
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    ingest_account_dim = BashOperator(
        task_id='ingest_account_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/account_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22:%22csv%22,%22recordReader.prop.delimiter%22:%22,%22%7D"'
        )
    )

    ingest_customer_dim = BashOperator(
        task_id='ingest_customer_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/customer_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22:%22csv%22,%22recordReader.prop.delimiter%22:%22,%22%7D"'
        )
    )

    ingest_branch_dim = BashOperator(
        task_id='ingest_branch_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/branch_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22:%22csv%22,%22recordReader.prop.delimiter%22:%22,%22%7D"'
        )
    )

    ingest_date_dim = BashOperator(
        task_id='ingest_date_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/date_dim_large_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=date_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22:%22csv%22,%22recordReader.prop.delimiter%22:%22,%22%7D"'
        )
    )

    ingest_account_dim >> ingest_customer_dim >> ingest_branch_dim >> ingest_date_dim
