import uuid
import requests
import json
import pprint
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sqlalchemy as sq
import uuid

Base = sq.orm.declarative_base()

class Bitcoin_table(Base):
    __tablename__ = "bitcoin_price"

    id = sq.Column(sq.Text, primary_key=True)
    date = sq.Column(sq.Text)
    rate_EUR = sq.Column(sq.Float)
    rate_USD = sq.Column(sq.Float)
    rate_GBP = sq.Column(sq.Float)

default_args = {
    'start_date': datetime(year=2022, month=10, day=19)
}

def get_data(url: str, ti) -> None:

    res = requests.get(url)
    json_list = json.loads(res.content)
    ti.xcom_push(key='extracted_bitcoin_data', value=json_list)

def transform_data(ti) -> None:
    data = ti.xcom_pull(key='extracted_bitcoin_data', task_ids=['extract_bitcoin_data'])[0]

    id = str(uuid.uuid4())
    date = data.get('time', None).get('updated', None)
    rate_EUR = data.get('bpi', None).get('EUR', None).get('rate_float', None)
    rate_USD = data.get('bpi', None).get('USD', None).get('rate_float', None)
    rate_GBP = data.get('bpi', None).get('GBP', None).get('rate_float', None)

    row = {'id': id, 'date': date, 'rate_EUR': rate_EUR, 'rate_USD': rate_USD, 'rate_GBP': rate_GBP}

    ti.xcom_push(key='transformed_bitcoin_data', value=row)

def load_data(ti) -> None:
    data = ti.xcom_pull(key='transformed_bitcoin_data', task_ids=['transform_bitcoin_data'])[0]
    engine = sq.create_engine("sqlite:///bitcoin1.db", echo=True, future=True)
    Base.metadata.create_all(engine, checkfirst = True)
    with sq.orm.Session(engine) as session:
        row_to_insert = Bitcoin_table(**data)
        session.add_all([row_to_insert])
        session.commit()

def print_content(ti) -> None:
    engine2 = sq.create_engine("sqlite:///bitcoin1.db", echo=True, future=True)
    with engine2.connect() as e:
        d = e.exec_driver_sql('SELECT * FROM bitcoin_price')
        for i in d:
            ti.xcom_push(key='selected_data'+str(list(i)[0]), value=list(i))

with DAG(
    dag_id='bitcoin_data_collector',
    default_args=default_args,
    schedule_interval='@daily',
    description='ETL pipeline for processing users big version'
) as dag:

    # Task 1 - Fetch bitcoin data
    task_extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=get_data,
        op_kwargs={'url': 'https://api.coindesk.com/v1/bpi/currentprice.json'}
    )
    # Task 2 - Transform bitcoin data
    task_transform_data = PythonOperator(
        task_id='transform_bitcoin_data',
        python_callable=transform_data
    )
    # Task 3 - Load bitcoin data into DB
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    # Task 4 - Select data from DB for as a check
    task_print_content = PythonOperator(
        task_id='print_data',
        python_callable=print_content
    )

    task_extract_data >> task_transform_data >> task_load_data >> task_print_content