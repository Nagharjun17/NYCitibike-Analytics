from airflow import DAG
from datetime import timedelta, datetime
import requests
from airflow.operators.python import PythonOperator
import pandas as pd
import boto3
from airflow.operators.bash_operator import BashOperator
import json
import ast


s3_client = boto3.client('s3')

target_bucket_name = 'nycitibike-transform-zone-yml'

station_info1_url = 'https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json'
station_info2_url = 'https://api.citybik.es/citi-bike-nyc.json'

def extract_data(**kwargs):
    url1 = kwargs['url1']
    url2 = kwargs['url2']
    station_info1 = requests.get('https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json')
    station_info2 = requests.get('https://api.citybik.es/citi-bike-nyc.json')
    station_info1_df = pd.DataFrame(station_info1.json()['data']['stations'])
    station_info2_df = pd.DataFrame(station_info2.json())
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file1_str = 'citibike_data1_' + date_now_string
    file2_str = 'citibike_data2_' + date_now_string
    station_info1_df.to_csv(f"{file1_str}.csv", index=False)
    station_info2_df.to_csv(f"{file2_str}.csv", index=False)
    station_info1_file_path = f"/home/ubuntu/{file1_str}.csv"
    station_info2_file_path = f"/home/ubuntu/{file2_str}.csv"
    output_list = [station_info1_file_path, file1_str, station_info2_file_path, file2_str]
    return output_list

def transform_data(task_instance):
    data1 = task_instance.xcom_pull(task_ids="tsk_extract_citibike_data")[0]
    object_key1 = task_instance.xcom_pull(task_ids="tsk_extract_citibike_data")[1]
    data2 = task_instance.xcom_pull(task_ids="tsk_extract_citibike_data")[2]
    object_key2 = task_instance.xcom_pull(task_ids="tsk_extract_citibike_data")[3]
    data1_df = pd.read_csv(data1)
    data2_df = pd.read_csv(data2)

    data2_df['lat'] = data2_df['lat'] / 1e6
    data2_df['lng'] = data2_df['lng'] / 1e6

    data2_df.rename(columns={'number': 'station_id', 'bikes': 'bikes_data2', 'free': 'free_docks'}, inplace=True)

    merged_df = pd.merge(data1_df, data2_df, on='station_id', how='inner')
    
    merged_df = merged_df[merged_df['is_renting'] == 1]

    merged_df['last_reported'] = pd.to_datetime(merged_df['last_reported'], unit='s')

    merged_df['vehicle_types_available'] = merged_df['vehicle_types_available'].apply(ast.literal_eval)

    def extract_bike_counts(vehicle_types):
        normal_bike = sum(v['count'] for v in vehicle_types if v['vehicle_type_id'] == '1')
        e_bike = sum(v['count'] for v in vehicle_types if v['vehicle_type_id'] == '2')
        return normal_bike, e_bike

    vehicle_counts = merged_df['vehicle_types_available'].apply(lambda x: extract_bike_counts(x))
    merged_df['normal_bike'] = vehicle_counts.apply(lambda x: x[0])
    merged_df['e_bike'] = vehicle_counts.apply(lambda x: x[1])

    merged_df.drop('is_installed', axis=1, inplace=True)
    merged_df.drop('is_renting', axis=1, inplace=True)
    merged_df.drop('is_returning', axis=1, inplace=True)
    merged_df.drop('num_scooters_unavailable', axis=1, inplace=True)
    merged_df.drop('num_scooters_available', axis=1, inplace=True)
    merged_df.drop('idx', axis=1, inplace=True)
    merged_df.drop('bikes_data2', axis=1, inplace=True)
    merged_df.drop('vehicle_types_available', axis=1, inplace=True)

    merged_df.to_csv("/home/ubuntu/merged_data.csv", index=False)


default_args = {
    'owner': 'nycitibike',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email': ['nagharjun2000@gmail.com', 'nm4074@nyu.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

with DAG('nycitibike_analytics_dag',
        default_args=default_args,
        # schedule_interval = '@weekly',
        catchup=False) as dag:

        extract_citibike_data = PythonOperator(
        task_id= 'tsk_extract_citibike_data',
        python_callable=extract_data,
        op_kwargs={'url1': station_info1_url, 'url2': station_info2_url}
        )

        transform_citibike_data = PythonOperator(
        task_id= 'tsk_transform_citibike_data',
        python_callable=transform_data
        )

        load_to_s3 = BashOperator(
            task_id = 'tsk_load_to_s3',
            bash_command = 'aws s3 mv /home/ubuntu/merged_data.csv s3://nycitibike-transform-zone-yml',
        )

        extract_citibike_data >> transform_citibike_data >> load_to_s3