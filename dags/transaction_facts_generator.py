from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.empty import EmptyOperator
import random
import json
from kafka import KafkaProducer

class KafkaProduceOperator(BaseOperator):
    @apply_defaults
    def __init__(self, kafka_broker, kafka_topic, num_records=100, *args, **kwargs):
        super(KafkaProduceOperator, self).__init__(*args, **kwargs)
        self.kafka_broker = kafka_broker
        self.kafka_topic = kafka_topic
        self.num_records = num_records

    def generate_transaction_data(self, row_num):
        customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, self.num_records+1)]
        account_ids = [f"A{str(i).zfill(5)}" for i in range(1, self.num_records+1)]
        branch_ids = [f"B{str(i).zfill(4)}" for i in range(1, self.num_records+1)]
        transaction_types = ['Credit', 'Debit', 'Transfer', 'Withdrawal', 'Deposit']
        currencies = ['USD', 'GBP', 'EUR']
        transaction_id = f"T{str(row_num).zfill(6)}"
        transaction_date = int((datetime.now() - timedelta(days=random.randint(0, 365))).timestamp() * 1000)
        account_id = random.choice(account_ids)
        customer_id = random.choice(customer_ids)
        transaction_type = random.choice(transaction_types)
        currency = random.choice(currencies)
        branch_id = random.choice(branch_ids)
        transaction_amount = round(random.uniform(10.0, 10000.0), 2)
        exchange_rate = round(random.uniform(0.5, 1.5), 4)
        transaction = {
            'transaction_id': transaction_id,
            'transaction_date': transaction_date,
            'account_id': account_id,
            'customer_id': customer_id,
            'transaction_type': transaction_type,
            'currency': currency,
            'branch_id': branch_id,
            'transaction_amount': transaction_amount,
            'exchange_rate': exchange_rate
        }
        return transaction

    def execute(self, context):
        producer = KafkaProducer(
            bootstrap_servers=self.kafka_broker,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        for row_num in range(1, self.num_records + 1):
            transaction = self.generate_transaction_data(row_num)
            producer.send(self.kafka_topic, value=transaction)
            self.log.info(f'Sent transaction: {transaction}')
        producer.flush()
        self.log.info(f'{self.num_records} transaction records have been sent to kafka topic {self.kafka_topic}')

start_date = datetime(year=2025, month=8, day=25)
default_args = {
    'owner': 'anubhav',
    'depends_on_past': False,
    'backfill': False,
    'start_date': start_date
}

with DAG(
    dag_id='transaction_facts_generator',
    default_args=default_args,
    description='Transaction fact data generator into kafka',
    schedule_interval=timedelta(days=1),
    tags=['fact_data']
) as dag:
    start = EmptyOperator(task_id='start_task')
    generate_txn_data = KafkaProduceOperator(
        task_id='generate_txn_fact_data',
        kafka_broker='kafka_broker:9092',
        kafka_topic='transaction_facts',
        num_records=100,
    )
    end = EmptyOperator(task_id='end_task')
    start >> generate_txn_data >> end