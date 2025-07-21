'''
# Optimizing Employee Performance An End-to-End HR Analytics Data Pipeline

## Introduction
Name  : Yonathan Anggraiwan

This program was designed to implement an end-to-end data pipeline from PostgreSQL to ElasticSearch, utilizing an employee performance and productivity dataset sourced from [Kaggle](https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data). 

The project also includes data validation using the Great Expectations framework, and the results are presented through an interactive dashboard built with Kibana.
'''


import datetime as dt
from datetime import timedelta
import psycopg2 as db
from elasticsearch import Elasticsearch, helpers, exceptions
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def fetch_from_postgresql():
    '''
    This function retrieves raw employee data from a PostgreSQL database and stores it as an unprocessed CSV file.

    Source:
     - Host     : postgres
     - Port     : 5432
     - DB Name  : m_3
     - User     : airflow
     - Table    : table_m3

    Output:
     - File '/tmp/raw_data.csv' containing raw employee data from PostgreSQL.
    '''

def data_cleaning():
    '''
    This function performs basic data cleaning by:
     - Removing duplicate records
     - Standardizing column names to lowercase and trimming excessive whitespace
     - Dropping rows with null values

    Additionally:
     - Adds an age segmentation column (`age_cap`) with bins and labels:
         - Young (20–30), Adult (30–45), Middle-Aged (45–55), Senior (55–60)

    Input:
     - Raw CSV from '/tmp/raw_data.csv'

    Output:
     - Clean CSV saved to '/tmp/data_clean.csv' and '/opt/airflow/dags/data_clean.csv'
    '''

def wait_for_es():
    '''
    This function pings the Elasticsearch instance to verify it's ready to accept data.

    Mechanism:
     - Sends HTTP GET to Elasticsearch root endpoint
     - Timeout: 5 seconds
     - Raises an exception if the status code is not 200
    '''
    es = Elasticsearch(['http://elasticsearch:9200'], timeout=5)
    try:
        if not es.ping():
            raise exceptions.ConnectionError("Elasticsearch is not responding")
    except exceptions.ConnectionError as e:
        raise e
    except requests.exceptions.Timeout as e:
        raise exceptions.ConnectionError("Elasticsearch connection timed out") from e
    except requests.exceptions.RequestException as e:
        raise exceptions.ConnectionError("Elasticsearch connection error") from e
    except Exception as e:
        raise exceptions.ConnectionError("An unexpected error occurred while connecting to Elasticsearch") from e
    print("Elasticsearch is ready to accept data.")
    return True

def post_to_elasticsearch():
    '''
    This function reads the cleaned CSV and indexes the data into Elasticsearch
    one row at a time (non-bulk method).

    Target index: 'm3_clean_data'
    '''

def push_to_elasticsearch():
    '''
    This function pushes the cleaned CSV data into Elasticsearch using bulk indexing.
    It uses the 'Employee_ID' column as the document ID.

    Features:
     - Checks if the index exists, creates it if not
     - Uses Elasticsearch helpers' bulk method for efficiency

    Target index: 'm3_data'
    '''


with DAG('optimizing_employee_performance',
         default_args= {
            'owner': 'Yonathan',
            'start_date': dt.datetime(2024, 11, 1, 9, 10) - timedelta(hours=7),
            'retries': 1,
            'retry_delay': timedelta(minutes=10)},
         schedule_interval='10,20,30 9 * * 6',
         catchup=False
         ) as dag:

    fetchFromPostgresql = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_from_postgresql
    )

    datacleaning = PythonOperator(
        task_id='clean_data',
        python_callable=data_cleaning
    )

    wait_for_es_task = PythonOperator(
        task_id='wait_for_elasticsearch',
        python_callable=wait_for_es,
        retries=5,
        retry_delay=timedelta(seconds=15)
    )

    post_to_es = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_to_elasticsearch
    )

    fetchFromPostgresql >> datacleaning >> wait_for_es_task >> post_to_es