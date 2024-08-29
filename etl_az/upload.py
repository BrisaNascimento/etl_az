# %%
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from etl_az.settings import Settings
from etl_az.connect import connect_to_adls
from etl_az.utils import csv_to_parquet
from etl_az.utils import get_source_name
import pyarrow as pa
import pyarrow.parquet as pq
import os


class Uploader():
    def __init__(self) -> None:
        ...

    def upload_erva_mate_to_raw(self, container_name: str) -> str:
        # Upload files from local directory to specific zone/blob name
        data_path = Settings().LOCAL_DATA_PATH
        try:
            local_csv_files_list = [f for f in os.listdir(data_path) if f.endswith('.csv')]
            for csv_file in local_csv_files_list:
                csv_name = csv_file.split('_')[0]
                date_file = csv_file.split('_')[1].split('.')[0]
                source = get_source_name(csv_name)
                blob_name = f"raw/govrs/agricultura/ervamate/{source}/{date_file}/{csv_name}_{date_file}.csv"
                blob = connect_to_adls(container_name, blob_name)
                with open(f'{data_path}/{csv_name}_{date_file}.csv', 'rb') as data:
                    blob.upload_blob(data, overwrite=True)
        except Exception as e:
            print(f"Upload csv to raw failed.{e}")
        else:
            print('Upload csv to raw sucessful')
            return True

    def upload_erva_mate_to_conformed(self, container_name: str) -> str:
        # Upload files from local directory to specific zone/blob name
        data_path = Settings().LOCAL_DATA_PATH
        try:
            local_parquet_files_list = [f for f in os.listdir(data_path) if f.endswith('.parquet')]
            for parquet_file in local_parquet_files_list:
                parquet_name = parquet_file.split('_')[0]
                date_file = parquet_file.split('_')[1].split('.')[0]
                blob_name = f"conformed/govrs/agricultura/ervamate/{parquet_name}/{date_file}/{parquet_name}_{date_file}.parquet"
                blob = connect_to_adls(container_name, blob_name)
                with open(f'{data_path}/{parquet_name}_{date_file}.parquet', 'rb') as data:
                    blob.upload_blob(data, overwrite=True)
        except Exception as e:
            print(f"Upload parquet to conformed failed.{e}")
        else:
            print('Upload parquet to conformed sucessful')
            return True

    def upload_erva_mate_to_silver(self, container_name: str) -> str:
        # Upload files from local directory to specific zone/blob name
        data_path = Settings().LOCAL_DATA_PATH
        try:
            local_parquet_files_list = [f for f in os.listdir(f'{data_path}transformed/') if f.endswith('.parquet')]
            for parquet_file in local_parquet_files_list:
                parquet_name = parquet_file.split('_')[0]
                date_file = parquet_file.split('_')[1].split('.')[0]
                blob_name = f"agricultura/ervamate/{parquet_name}/{date_file}/{parquet_name}_{date_file}.parquet"
                blob = connect_to_adls(container_name, blob_name)
                with open(f'{data_path}transformed/{parquet_name}_{date_file}.parquet', 'rb') as data:
                    blob.upload_blob(data, overwrite=True)
        except Exception as e:
            print(f"Upload parquet to Silver failed.{e}")
        else:
            print('Upload parquet to Silver sucessful')
            return True

    def upload_erva_mate_to_gold(self, container_name: str) -> str:
        # Upload files from local directory to specific zone/blob name
        data_path = Settings().LOCAL_DATA_PATH
        try:
            local_parquet_files_list = [f for f in os.listdir(f'{data_path}transformed/') if f.endswith('.parquet')]
            for parquet_file in local_parquet_files_list:
                parquet_name = parquet_file.split('_')[0]
                date_file = parquet_file.split('_')[1].split('.')[0]
                blob_name = f"agricultura/ervamate/{parquet_name}/{parquet_name}.parquet"
                blob = connect_to_adls(container_name, blob_name)
                with open(f'{data_path}transformed/{parquet_name}_{date_file}.parquet', 'rb') as data:
                    blob.upload_blob(data, overwrite=True)
        except Exception as e:
            print(f"Upload parquet to Gold failed.{e}")
        else:
            print('Upload parquet to Gold sucessful')
            return True
