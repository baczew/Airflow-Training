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

default_args = {'start_date': datetime(year=2022, month=10, day=19)} #default args used by DAGs

Base = sq.orm.declarative_base()


class BitcoinTable(Base):
    '''Class to be used to create an initialization schema of the destination table in SQLite. ORM approach used'''

    __tablename__ = "bitcoin_price"

    id = sq.Column(sq.Text, primary_key=True)
    date = sq.Column(sq.Text)
    rate_EUR = sq.Column(sq.Float)
    rate_USD = sq.Column(sq.Float)
    rate_GBP = sq.Column(sq.Float)


def get_data(url: str, push_key: str, ti) -> None:
    '''Function that allows to get data from the URL & push it'''
    res = requests.get(url)
    json_list = json.loads(res.content)
    ti.xcom_push(key=push_key, value=json_list)


def transform_data(pull_key: str, push_key: str, ti) -> None:
    '''Function that transforms the pulled data, specified for the bitcoin API structure'''
    data = ti.xcom_pull(key=pull_key, task_ids=['extract_bitcoin_data'])[0]

    id = str(uuid.uuid4())
    date = data.get('time', None).get('updated', None)
    rate_EUR = data.get('bpi', None).get('EUR', None).get('rate_float', None)
    rate_USD = data.get('bpi', None).get('USD', None).get('rate_float', None)
    rate_GBP = data.get('bpi', None).get('GBP', None).get('rate_float', None)

    row = {'id': id, 'date': date, 'rate_EUR': rate_EUR, 'rate_USD': rate_USD, 'rate_GBP': rate_GBP}

    ti.xcom_push(key=push_key, value=row)


def load_data(pull_key: str, ti) -> None:
    '''Function that loads the data into the DB'''

    data = ti.xcom_pull(key=pull_key, task_ids=['transform_bitcoin_data'])[0]
    engine = sq.create_engine("sqlite:///bitcoin1.db", echo=True, future=True)
    Base.metadata.create_all(engine, checkfirst = True)
    with sq.orm.Session(engine) as session:
        row_to_insert = BitcoinTable(**data)
        session.add_all([row_to_insert])
        session.commit()


def print_content(ti) -> None:
    '''Function that prints pushes all the records from the DB - just to supervise'''
    engine2 = sq.create_engine("sqlite:///bitcoin1.db", echo=True, future=True)
    with engine2.connect() as e:
        data = e.exec_driver_sql('SELECT * FROM bitcoin_price')
        for i in data:
            ti.xcom_push(key='selected_data'+str(list(i)[0]), value=list(i))


with DAG(
    dag_id='bitcoin_data_collector',
    default_args=default_args,
    schedule_interval='*/1 * * * *',
    description='ETL pipeline for processing users big version',
    max_active_runs = 1
) as dag:

    # Task 1 - Fetch bitcoin data
    task_extract_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=get_data,
        op_kwargs={'url': 'https://api.coindesk.com/v1/bpi/currentprice.json', 'push_key': 'extracted_data'}
    )
    # Task 2 - Transform bitcoin data
    task_transform_data = PythonOperator(
        task_id='transform_bitcoin_data',
        python_callable=transform_data,
        op_kwargs={'pull_key': 'extracted_data', 'push_key': 'transformed_data'}
    )
    # Task 3 - Load bitcoin data into DB
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'pull_key': 'transformed_data'}
    )
    # Task 4 - Select data from DB for as a check
    task_print_content = PythonOperator(
        task_id='print_data',
        python_callable=print_content
    )

    task_extract_data >> task_transform_data >> task_load_data >> task_print_content