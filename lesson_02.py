import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


# Top 10 domain zones
def get_top_domain_zones():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain']) # All data
    top_data_df['domain_zone'] = top_data_df['domain'].apply(lambda x: x.split('.')[-1]) # Split by comma and take last part (domain zone)

    # Group by zone, count qty of each, sort descending and take top 10
    top_10_domain_zones = top_data_df.groupby('domain_zone', as_index=False) \
        .agg({'rank': 'count'}) \
        .rename(columns={'rank': 'quantity'}) \
        .sort_values('quantity', ascending=False) \
        .reset_index(drop=True) \
        .head(10)
    
    top_10_domain_zones['Num'] = top_10_domain_zones['quantity'].rank(ascending=False).astype(int)
    
    top_10_domain_zones = top_10_domain_zones[['Num', 'domain_zone', 'quantity']]

    # Write to csv file
    with open('top_10_domain_zones.csv', 'w') as f:
        f.write(top_10_domain_zones.to_csv(index=False, header=False))


# Domain with longest name
def get_max_len_domain():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_df['domain_length'] = top_data_df['domain'].apply(lambda x: len(x)) # Get length of each domain

    # Sort by max length and in alphabet order. Take the first one
    max_len_domain = top_data_df.sort_values(['domain_length', 'domain'], ascending=[False, True]).iloc[0].domain

    # Make file with saved longest domain, first in alphabetical order if several longest domains
    with open('max_len_domain.txt', 'w') as f:
        f.write(str(max_len_domain))
            
# airflow rank
def airflow_rank():
    airflow_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    airflow_df['domain']
    desirable_domain = 'airflow.com'

    if desirable_domain in airflow_df['domain'].values:
        airflow_rank = int(airflow_df.query('domain == "airflow.com"')['rank'])
    else:
        airflow_rank = "airflow.com not in the list at this moment"

    with open('airflow_rank.txt', 'w') as f:
        f.write(str(airflow_rank))


# Print results
def print_data():
    with open('top_10_domain_zones.csv', 'r') as f:
        top_10_domain_zones = f.read()
    with open('max_len_domain.txt', 'r') as f:
        max_len_domain = f.read()
    with open('airflow_rank.txt', 'r') as f:
        airflow_rank = f.read()

    print(f'Top 10 domain zones on date:\n{top_10_domain_zones}')
    
    print(f'Domain with longest name: {max_len_domain}\n')
    
    print(f'Airflow rank: {airflow_rank}\n')

default_args = {
    'owner': 'a-gureev-18',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 4, 8),
    'schedule_interval': '00 12 * * *'
}    
    
dag = DAG('a-gureev-18_lesson_2', default_args=default_args)

t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

t2_top_10 = PythonOperator(task_id='get_top_domain_zones',
                    python_callable=get_top_domain_zones,
                    dag=dag)

t2_max_len = PythonOperator(task_id='get_max_len_domain',
                    python_callable=get_max_len_domain,
                    dag=dag)

t2_airflow = PythonOperator(task_id='airflow_rank',
                    python_callable=airflow_rank,
                    dag=dag)

t3 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)

t1 >> [t2_top_10, t2_max_len, t2_airflow] >> t3

#t1.set_downstream(t2_top_10)
#t1.set_downstream(t2_max_len)
#t1.set_downstream(t2_airflow)

#t2_top_10.set_downstream(t3)
#t2_max_len.set_downstream(t3)
#t2_airflow.set_downstream(t3)