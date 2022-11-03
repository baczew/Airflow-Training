import uuid
import requests
import json
import pprint
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import os
import sqlalchemy as sq
import uuid
import datetime

default_args = {'start_date': datetime.datetime(year=2022, month=11, day=2)}  # default args used by DAGs

meta_data = sq.MetaData()


def create_table(input_name, input_rate, input_metadata):
    '''Function to add a separate table for chosen currency to the tables metadata'''

    metadata = input_metadata

    new_table = sq.Table(
        input_name,
        metadata,
            sq.Column("id", sq.Text),
            sq.Column("parent_id", sq.Text),
            sq.Column("date", sq.Text),
            sq.Column(f"BTC/{input_rate}", sq.Float)
    )
    return new_table


def create_table_schemas_for_chosen_currencies(currency_list):
    '''Function to create schemas of required tables according to the currency list'''

    currency_table_dict = {currency: create_table(f"bitcoin_price_{currency}", f"{currency}", meta_data) for currency in
                           currency_list}

    return currency_table_dict


def get_data(url: str, push_key: str, ti) -> None:
    '''Function that allows to get data from the URL & push it'''

    res = requests.get(url)
    json_list = json.loads(res.content)
    ti.xcom_push(key=push_key, value=json_list)


def transform_bitcoin_data(pull_key: str, push_key: str, ti) -> None:
    '''Function that transforms the pulled data, specified for the bitcoin API structure'''

    data = ti.xcom_pull(key=pull_key, task_ids=['extract_bitcoin_data'])[0]

    id = str(uuid.uuid4())
    date = data.get('time', None).get('updated', None)
    rate_USD = data.get('bpi', None).get('USD', None).get('rate_float', None)

    row = {'id': id, 'date': date, 'BTC/USD': rate_USD}

    ti.xcom_push(key=push_key, value=row)


def transform_currency_data(pull_key: str, push_key: str, ti) -> None:
    '''Function that transforms the pulled data, specified for the currency API structure'''

    data = ti.xcom_pull(key=pull_key, task_ids=['extract_currency_data'])[0]

    id = str(uuid.uuid4())
    date = datetime.datetime.now(datetime.timezone.utc).strftime("%b %-d, %Y %H:%M:00 UTC")
    rates = data.get('data', None).get('rates', None)

    rates_list = list(rates)
    USD_position = rates_list.index('USD')
    rates_currencies_only = {k: float(rates[k]) for k in rates_list[:USD_position]}

    row = {'id': id, 'date': date, 'rates': rates_currencies_only}

    ti.xcom_push(key=push_key, value=row)


def merge_data(source1: list, source2: list, push_key: str, ti) -> None:
    '''Merge two XCOMS into one table - Bitcoin prices in different currencies'''

    pull_key, task_id = source1
    data_source_bitcoin = ti.xcom_pull(key=pull_key, task_ids=[task_id])[0]

    pull_key, task_id = source2
    data_source_currency = ti.xcom_pull(key=pull_key, task_ids=[task_id])[0]

    currency_list = ti.xcom_pull(key='upload_currency_list', task_ids=['pick_currencies_data'])[0]

    result = dict()
    result['id'] = data_source_currency['id']
    result['date'] = data_source_currency['date']
    for currency in currency_list:
        result[f"BTC/{currency}"] = data_source_bitcoin['BTC/USD'] / (1 / data_source_currency['rates'][f'{currency}'])

    ti.xcom_push(key=push_key, value=result)


def load_data(pull_key: str, ti) -> None:
    '''Function that loads the data into the DB'''

    full_pulled_data = ti.xcom_pull(key=pull_key, task_ids=['merge_data'])[0]
    currency_list = ti.xcom_pull(key='upload_currency_list', task_ids=['pick_currencies_data'])[0]

    engine = sq.create_engine("sqlite:///bitcoin2.db", echo=True, future=True)
    table_dict = create_table_schemas_for_chosen_currencies(currency_list)
    meta_data.create_all(engine, checkfirst=True)

    for currency in table_dict:

        data = dict()
        data['id'] = str(uuid.uuid4())
        data['parent_id'] = full_pulled_data['id']
        data['date'] = full_pulled_data['date']
        data[f'BTC/{currency}'] = full_pulled_data[f'BTC/{currency}']

        with sq.orm.Session(engine) as session:

            query = table_dict[currency].insert().values(**data)
            session.execute(query)
            session.commit()


def print_content(ti) -> None:
    '''Function that pushes all the records from the DB - just to supervise'''

    engine2 = sq.create_engine("sqlite:///bitcoin2.db", echo=True, future=True)
    currency_list = ti.xcom_pull(key='upload_currency_list', task_ids=['pick_currencies_data'])[0]

    with engine2.connect() as e:

        for currency in currency_list:
            data = e.exec_driver_sql(f"SELECT '{currency}', * FROM bitcoin_price_{currency}")
            for i in data:
                ti.xcom_push(key='selected_data' + str(list(i)[1]), value=list(i))


def pick_currencies(currency_list, push_key, pull_key, task_id, ti):
    '''Function to push currency list and to use it later in the flow'''

    data_source_currency = ti.xcom_pull(key=pull_key, task_ids=[task_id])[0]

    currency = set(currency_list) & set(list(data_source_currency['rates']))

    ti.xcom_push(key=push_key, value=list(currency))


with DAG(
        dag_id='bitcoin_data_collector',
        default_args=default_args,
        schedule_interval='*/5 * * * *',
        description='ETL pipeline for processing users big version',
        max_active_runs=1
) as dag:

    # Task 0
    task_pick_currencies = PythonOperator(
        task_id='pick_currencies_data',
        python_callable=pick_currencies,
        op_kwargs={'currency_list': ['ISK', 'PLN', 'HUF', 'JP2', 'HUF'],
                   'push_key': 'upload_currency_list',
                   'pull_key': 'transformed_currency_data',
                   'task_id': 'transform_currency_data'}
    )

    # Task 1.1 - Fetch bitcoin data
    task_extract_bitcoin_data = PythonOperator(
        task_id='extract_bitcoin_data',
        python_callable=get_data,
        op_kwargs={'url': 'https://api.coindesk.com/v1/bpi/currentprice.json', 'push_key': 'extracted_bitcoin_data'}
    )
    # Task 1.2 - Fetch currency data
    task_extract_currency_data = PythonOperator(
        task_id='extract_currency_data',
        python_callable=get_data,
        op_kwargs={'url': 'https://api.coinbase.com/v2/exchange-rates?currency=usd',
                   'push_key': 'extracted_currency_data'}
    )
    # Task 2.1 - Transform bitcoin data
    task_transform_bitcoin_data = PythonOperator(
        task_id='transform_bitcoin_data',
        python_callable=transform_bitcoin_data,
        op_kwargs={'pull_key': 'extracted_bitcoin_data', 'push_key': 'transformed_bitcoin_data'}
    )
    # Task 2.2 - Transform currency data
    task_transform_currency_data = PythonOperator(
        task_id='transform_currency_data',
        python_callable=transform_currency_data,
        op_kwargs={'pull_key': 'extracted_currency_data', 'push_key': 'transformed_currency_data'}
    )
    # Task 3 - Merge data
    task_merge_data = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        op_kwargs={'source1': ['transformed_bitcoin_data', 'transform_bitcoin_data'],
                   'source2': ['transformed_currency_data', 'transform_currency_data'],
                   'push_key': 'merged_data'}
    )
    # Task 4 - Load bitcoin data into DB
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        op_kwargs={'pull_key': 'merged_data'}
    )
    # Task 5 - Select data from DB for as a check
    task_print_content = PythonOperator(
        task_id='print_data',
        python_callable=print_content
    )

    task_extract_bitcoin_data >> task_transform_bitcoin_data >> task_merge_data
    task_extract_currency_data >> task_transform_currency_data >> task_pick_currencies >> task_merge_data
    task_merge_data >> task_load_data >> task_print_content