import requests
import os
import shutil
from http import HTTPStatus
from datetime import datetime
from etl_az.settings import Settings
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def web_data_extractor(url: str):
    # Set a data_path from local env
    data_path = Settings().LOCAL_DATA_PATH
    # Create local data directory if not exists
    os.makedirs(data_path, exist_ok=True)

    response = requests.get(url)
    if response.status_code == HTTPStatus.OK:
        url_name = os.path.splitext(os.path.basename(url))[0]
        ingestion_date = datetime.now().strftime('%Y-%m-%d')
        file_name = f'{url_name}_{ingestion_date}.csv'
        filepath = os.path.join(data_path, file_name)
        with open(filepath, 'wb') as file_doc:
            file_doc.write(response.content)
    else:
        print(f"An error occurred while trying to access the URL {url}")
    return file_name


def get_source_name(csv_name):  # ajustar para algo mais abstrato
    if csv_name == 'dee-977':
        source = 'areacolhida'
    elif csv_name == 'dee-1690':
        source = 'areadestinadacolheita'
    elif csv_name == 'dee-978':
        source = 'quantidadeproduzida'
    elif csv_name == 'dee-979':
        source = 'rendimentomedio'
    elif csv_name == 'dee-980':
        source = 'valorproducao'
    return source

# Convert raw data to conformed parquet and upload to Bronze Layer


def csv_to_parquet():
    data_path = Settings().LOCAL_DATA_PATH
    local_csv_files_list = [f for f in os.listdir(data_path) if f.endswith('.csv')]
    for csv_file in local_csv_files_list:
        csv_name = csv_file.split('_')[0]
        date_file = csv_file.split('_')[1].split('.')[0]
        source = get_source_name(csv_name)
        # Convert local csv file to parquet
        df = pd.read_csv(f'{data_path}{csv_file}', encoding='latin-1', sep=',', skiprows=1)
        table = pa.Table.from_pandas(df)
        pq.write_table(table, f'{data_path}{source}_{date_file}.parquet')
    print('Files converted to parquet')
    return True


def clear_data():
    data_path = Settings().LOCAL_DATA_PATH
    # delete path
    shutil.rmtree(data_path)
    # create new empty path
    os.makedirs(f'{data_path}transformed/')
