import uuid
import requests
import json
import pprint
#from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import os
import sqlalchemy as sq
import uuid
import datetime


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


def create_table_schemas_for_chosen_currencies(currency_list, meta_data):
    '''Function to create schemas of required tables according to the currency list'''

    currency_table_dict = {currency: create_table(f"bitcoin_price_{currency}", f"{currency}", meta_data) for currency in
                           currency_list}

    return currency_table_dict


@dag(
        dag_id='bitcoin_data_collector_2.0',
        start_date = datetime.datetime(2022, 9, 11),
        max_active_runs=1
)

def dag_function():

    @task
    def pick_currencies(input_currency_list: list, available_currency_list: dict) -> list:
        '''Function to push currency list and to use it later in the flow'''

        currency = set(input_currency_list) & set(list(available_currency_list['rates']))
        return list(currency)


    @task
    def get_data(url: str) -> dict:
        '''Function that allows to get data from the URL & push it'''

        res = requests.get(url)
        json_list = json.loads(res.content)
        return json_list


    @task
    def transform_bitcoin_data(input_data: dict) -> dict:
        '''Function that transforms the pulled data, specified for the bitcoin API structure'''

        data = input_data

        id = str(uuid.uuid4())
        date = data.get('time', None).get('updated', None)
        rate_USD = data.get('bpi', None).get('USD', None).get('rate_float', None)

        row = {'id': id, 'date': date, 'BTC/USD': rate_USD}
        return row


    @task
    def transform_currency_data(input_data: dict) -> dict:
        '''Function that transforms the pulled data, specified for the currency API structure'''

        data = input_data

        id = str(uuid.uuid4())
        date = datetime.datetime.now(datetime.timezone.utc).strftime("%b %-d, %Y %H:%M:00 UTC")
        rates = data.get('data', None).get('rates', None)

        rates_list = list(rates)
        USD_position = rates_list.index('USD')
        rates_currencies_only = {k: float(rates[k]) for k in rates_list[:USD_position]}

        row = {'id': id, 'date': date, 'rates': rates_currencies_only}

        return row


    @task
    def merge_data(source1, source2, currency_list) -> dict:
        '''Merge two XCOMS into one table - Bitcoin prices in different currencies'''


        data_source_bitcoin = source1
        data_source_currency = source2
        currency_list = currency_list

        result = dict()
        result['id'] = data_source_currency['id']
        result['date'] = data_source_currency['date']
        for currency in currency_list:
            result[f"BTC/{currency}"] = data_source_bitcoin['BTC/USD'] / (
                        1 / data_source_currency['rates'][f'{currency}'])

        return result


    @task
    def load_data(data, currency_list_, meta_data) -> None:
        '''Function that loads the data into the DB'''

        full_pulled_data = data
        currency_list = currency_list_

        engine = sq.create_engine("sqlite:///bitcoin3.db", echo=True, future=True)
        table_dict = create_table_schemas_for_chosen_currencies(currency_list, meta_data)
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

        return 1


    @task
    def print_content(currency_list_, status) -> list:
        '''Function that pushes all the records from the DB - just to supervise'''

        engine2 = sq.create_engine("sqlite:///bitcoin3.db", echo=True, future=True)
        currency_list = currency_list_

        with engine2.connect() as e:

            for currency in currency_list:
                data = e.exec_driver_sql(f"SELECT '{currency}', * FROM bitcoin_price_{currency}")
                return [str(i) for i in data]


    plain_bitcoin_data = get_data('https://api.coindesk.com/v1/bpi/currentprice.json')
    plain_currency_data = get_data('https://api.coinbase.com/v2/exchange-rates?currency=usd')
    transformed_bitcoin_data = transform_bitcoin_data(plain_bitcoin_data)
    transformed_currency_data = transform_currency_data(plain_currency_data)
    clean_currencies = pick_currencies(['ISK', 'PLN', 'HUF', 'JP2', 'HUF'], transformed_currency_data)
    merged_data = merge_data(transformed_bitcoin_data, transformed_currency_data, clean_currencies)
    meta_data_obj = sq.MetaData()
    status = load_data(merged_data, clean_currencies, meta_data_obj)
    print(print_content(clean_currencies, status))



dag_function()



