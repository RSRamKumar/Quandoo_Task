

import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago



from quandoo_project.quandoo_webscraper_app import QuandooRestaurantsWebScraper


import pandas as pd
from datetime import datetime, timedelta
import pendulum


default_args = {
    'owner': 'Ram Kumar',
    'start_date': days_ago(0),
    'retries':2,

}

quandoo_restaurant_scraping_dag = DAG(
    'quandoo_restaurant_scraping_dag',
    default_args=default_args,
    description='Scrapping Restaurants Data in Quandoo',
    schedule_interval= timedelta(minutes=30)# @daily, @monthly
    catchup=False
)

task_1 = PythonOperator(
    task_id='quandoo_webscraing',
    python_callable= QuandooRestaurantsWebScraper,
	op_kwargs = {'city_name': 'berlin'}
    dag=quandoo_restaurant_scraping_dag
)

task_1 