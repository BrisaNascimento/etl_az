from etl_az.connect import connect_to_adls
from etl_az.settings import Settings


def download_from_conformed_adls(ingestion_date: str, container_name: str):
    source = [
        'areacolhida',
        'areadestinadacolheita',
        'quantidadeproduzida',
        'rendimentomedio',
        'valorproducao'
    ]
    for s in source:
        # define data_path
        data_path = './data/'
        # identifies blob source
        blob_conformed = f'conformed/govrs/agricultura/ervamate/{s}/{ingestion_date}/{s}_{ingestion_date}.parquet'
        # conect to resource
        blob = connect_to_adls(container_name, blob_conformed)
        # define local path to download and download data
        download_path = f'{data_path}{s}_{ingestion_date}.parquet'
        with open(download_path, 'wb') as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
    print('Download from conformed sucessful')
    return True


def download_from_silver_adls(ingestion_date: str, container_name: str):
    source = [
        'areacolhida',
        'areadestinadacolheita',
        'municipios',
        'quantidadeproduzida',
        'rendimentomedio',
        'valorproducao',
    ]
    for s in source:
        # define data_path
        data_path = './data/'
        # identifies blob source
        blob_conformed = f'agricultura/ervamate/{s}/{ingestion_date}/{s}_{ingestion_date}.parquet'
        # conect to resource
        blob = connect_to_adls(container_name, blob_conformed)
        # define local path to download and download data
        download_path = f'{data_path}{s}_{ingestion_date}.parquet'
        with open(download_path, 'wb') as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
    print('Download from Silver sucessful')
    return True


def download_from_gold_adls(ingestion_date: str, container_name: str):
    source = [
        'producaoervamate',
        'valorproducaoenriq',
    ]
    for s in source:
        # define data_path
        data_path = './data/'
        # identifies blob source
        blob_conformed = f'agricultura/ervamate/{s}/{s}.parquet'
        # conect to resource
        blob = connect_to_adls(container_name, blob_conformed)
        # define local path to download and download data
        download_path = f'{data_path}{s}.parquet'
        with open(download_path, 'wb') as my_blob:
            blob_data = blob.download_blob()
            blob_data.readinto(my_blob)
    print('Download from Gold sucessful')
    return True