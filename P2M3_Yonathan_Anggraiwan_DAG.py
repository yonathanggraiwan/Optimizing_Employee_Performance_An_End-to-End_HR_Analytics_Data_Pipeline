'''
# Milestone 3

## Introduction
Nama  : Yonathan Anggraiwan

Batch : 028

Program ini dibuat untuk melakukan end-to-end data pipeline dari PostgreSQL ke ElasticSearch. Adapun dataset yang dipakai adalah dataset data karyawan yang didapatkan dari situs [Kaggle](https://www.kaggle.com/datasets/mexwell/employee-performance-and-productivity-data). 

Dalam project kali ini, juga dilakukan validasi data menggunakan teknik Great Expecations, dan visualisasi data yang ditampilkan dalam dashboard interaktif dari Kibana.

'''


import datetime as dt
from datetime import timedelta
import psycopg2 as db
from elasticsearch import Elasticsearch, helpers, exceptions
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Task 1 – Fetch data dari PostgreSQL dan simpan sebagai CSV
def fetch_from_postgresql():
    '''
    Fungsi ini mengambil data dari database PostgreSQL dan menyimpannya sebagai file CSV yang belum diolah atau masih mentah.

    Source:
     - Host     : postgres
     - Port     : 5432
     - DB Name  : m_3
     - User     : airflow
     - Table    : table_m3

    Output:
     - File '/tmp/raw_data.csv' berisi data mentah dari PostgreSQL.
    '''
    conn_string = "host='postgres' port=5432 dbname='m_3' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT * FROM table_m3", conn)
    df.to_csv('/tmp/raw_data.csv', index=False)
    conn.close()


# Task 2 – Pembersihan data sederhana
def data_cleaning():
    '''
    Fungsi ini melakukan pembersihan data, yaitu:
     - Menghapus duplikasi data
     - Standarisasi nama kolom ke lowercase dan menghilangkan spasi berlebih dalam data
     - Menghapus baris yang mengandung nilai kosong (null)

    Input:
     - CSV mentah dari '/tmp/raw_data.csv'

    Output:
     - CSV bersih di '/tmp/P2M3_Yonathan_Anggraiwan_data_clean.csv'
    '''
    df = pd.read_csv('/tmp/raw_data.csv')
    df = df.drop_duplicates()
    df = df.dropna()
    bins = [20, 30, 45, 55, 60]
    labels = ['Young', 'Adult', 'Middle-Aged', 'Senior']
    df['age_cap'] = pd.cut(df['Age'], bins=bins, labels=labels, right=True)

    df.columns = df.columns.str.lower().str.strip()
    df.to_csv('/tmp/P2M3_Yonathan_Anggraiwan_data_clean.csv', index=False)
    df.to_csv('/opt/airflow/dags/P2M3_Yonathan_Anggraiwan_data_clean.csv', index=False)

# Task 3 – Tunggu sampai Elasticsearch siap
def wait_for_es():
    '''
    Fungsi ini akan melakukan ping ke instance Elasticsearch
    untuk memastikan bahwa server sudah siap menerima data.

    Mekanisme:
     - Melakukan HTTP GET ke endpoint root Elasticsearch
     - Timeout: 5 detik
     - Raise Exception jika status bukan 200
    '''
    try:
        response = requests.get("http://elasticsearch:9200", timeout=5)
        if response.status_code != 200:
            raise Exception("Elasticsearch belum siap")
    except Exception as e:
        raise RuntimeError("Gagal mengkoneksikan ke Elasticsearch: " + str(e))

# Task 4 – Push data ke Elasticsearch (iteratif)
def post_to_elasticsearch():
    '''
    Fungsi ini membaca file CSV yang sudah dibersihkan,
    kemudian mengirim data baris demi baris ke Elasticsearch
    menggunakan metode indexing individual (bukan bulk).

    Index tujuan: 'm3_clean_data'
    '''
    df = pd.read_csv('/tmp/P2M3_Yonathan_Anggraiwan_data_clean.csv')
    es = Elasticsearch("http://elasticsearch:9200")
    index_name = "m3_clean_data"

    for _, row in df.iterrows():
        doc = row.to_dict()
        es.index(index=index_name, body=doc)


# Alternative method – Push secara bulk dengan ID unik
def push_to_elasticsearch():
    '''
    Fungsi ini mendorong data CSV bersih ke Elasticsearch secara bulk
    dengan menggunakan kolom 'Employee_ID' sebagai ID dokumen.

    Fitur:
     - Cek apakah index sudah ada, jika tidak maka akan dibuat secara otomatis
     - Menggunakan metode bulk dari Elasticsearch helpers untuk efisiensi

    Index tujuan: 'm3_data'
    '''
    es = Elasticsearch("http://elasticsearch:9200")
    index_name = "m3_data"

    df = pd.read_csv('/tmp/P2M3_Yonathan_Anggraiwan_data_clean.csv')

    actions = [
        {
            "_index": index_name,
            "_id": row['Employee_ID'],
            "_source": row.to_dict()
        }
        for _, row in df.iterrows()
    ]
    
    try:
        es.indices.create(index=index_name)
    except exceptions.RequestError as e:
        if e.error == 'resource_already_exists_exception':
            pass
        else:
            raise e

    # Bulk insert
    helpers.bulk(es, actions)

with DAG('Milestone_3',
         default_args= {
            'owner': 'Yonathan',
            'start_date': dt.datetime(2024, 11, 1, 9, 10) - timedelta(hours=7),
            'retries': 1,
            'retry_delay': timedelta(minutes=10)},
         schedule_interval='10,20,30 9 * * 6',
         catchup=False
         ) as dag:

    fetchFromPostgresql = PythonOperator(
        task_id='tarik_data',
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