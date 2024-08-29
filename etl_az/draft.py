import requests
import os
from http import HTTPStatus
from datetime import datetime
from etl_az.settings import Settings
import pandas as pd


def collect_erva_mate_data():
    '''
    Link references:
    area colhida - https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv
    area destinada a colheita - https://dados.dee.planejamento.rs.gov.br/download/dee-1690.csv
    quantidade produzida - https://dados.dee.planejamento.rs.gov.br/download/dee-978.csv
    rendimento medio - https://dados.dee.planejamento.rs.gov.br/download/dee-979.csv
    valor da producao - https://dados.dee.planejamento.rs.gov.br/download/dee-980.csv
    '''
    # Create a list of URLs from source
    url_list = [
    'https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv',
    'https://dados.dee.planejamento.rs.gov.br/download/dee-1690.csv',
    'https://dados.dee.planejamento.rs.gov.br/download/dee-978.csv',
    'https://dados.dee.planejamento.rs.gov.br/download/dee-979.csv',
    'https://dados.dee.planejamento.rs.gov.br/download/dee-980.csv'
    ]
    # Set a data_path from local env
    data_path = Settings().LOCAL_DATA_PATH
    # Create local data directory if not exists
    os.makedirs(data_path, exist_ok=True)

    for url in url_list:
        response = requests.get(url)
        if response.status_code == HTTPStatus.OK:
            url_name = os.path.basename(url)
            ingestion_date = datetime.now().strftime('%Y-%m-%d')
            if url_name == 'dee-977.csv':
                file_name = f'areacolhida_{ingestion_date}.csv'
                filepath = os.path.join(data_path, file_name)
                with open(filepath, 'wb') as file_doc:
                    file_doc.write(response.content)
                    print(f'{file_name} was sucessful extracted')
            elif url_name == 'dee-1690.csv':
                file_name = f'areadestinadacolheita_{ingestion_date}.csv'
                filepath = os.path.join(data_path, file_name)
                with open(filepath, 'wb') as file_doc:
                    file_doc.write(response.content)
                    print(f'{file_name} was sucessful extracted')
            elif url_name == 'dee-978.csv':
                file_name = f'quantidadeproduzida_{ingestion_date}.csv'
                filepath = os.path.join(data_path, file_name)
                with open(filepath, 'wb') as file_doc:
                    file_doc.write(response.content)
                    print(f'{file_name} was sucessful extracted')
            elif url_name == 'dee-979.csv':
                file_name = f'rendimentomedio_{ingestion_date}.csv'
                filepath = os.path.join(data_path, file_name)
                with open(filepath, 'wb') as file_doc:
                    file_doc.write(response.content)
                    print(f'{file_name} was sucessful extracted')
            elif url_name == 'dee-980.csv':
                file_name = f'valorproducao_{ingestion_date}.csv'
                filepath = os.path.join(data_path, file_name)
                with open(filepath, 'wb') as file_doc:
                    file_doc.write(response.content)
                    print(f'{file_name} was sucessful extracted')
        else:
            print(f"An error occurred while trying to access the URL {url}")
# %%
collect_erva_mate_data()
# %%

df = pd.read_csv('./data/area_colhida_2024-08-07.csv', sep=',', encoding='latin-1', header=0)
df

#%%
from etl_az.utils import web_data_extractor

web_data_extractor('https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv')
# %%
url_name = os.path.splitext(os.path.basename('https://dados.dee.planejamento.rs.gov.br/download/dee-977.csv'))[0]
print(url_name)
# %%
'''# ajustando e tentando generalizar
    def upload_erva_mate(self, container_name: str) -> str:
        # Upload files from local directory to specific zone/blob name
        data_path = Settings().LOCAL_DATA_PATH
        try:
            # obtem a lista de todos os arquivos locais
            local_files_list = [f for f in os.listdir(data_path) if f.endswith('.csv')]
            local_files_list_parquet = []
            # if f.endswith('.csv') desenvolver condicao para arquivos csv e para arquivos parquet
            for name_file in local_files_list:
                raw_name_file = name_file.split('_')[0]
                date_file = name_file.split('_')[1].split('.')[0]
                # obtem nome tratado
                source = get_source_name(raw_name_file)
                # faz upload dos arquivos em csv na raw
                blob_name = f"raw/govrs/agricultura/ervamate/{source}/{date_file}/{source}_{date_file}.csv"
                blob = connect_to_adls(container_name, blob_name)
                with open(f'{data_path}/{source}_{date_file}.csv', 'rb') as data:
                    blob.upload_blob(data)
                # converte o csv local em parquet ajustando o nome
                df = pd.read_csv(f'{data_path}{raw_name_file}', encoding='latin-1', sep=',', skiprows=1)
                table = pa.Table.from_pandas(df)
                parquet_name_file = f'{data_path}{source}_{date_file}.parquet'
                pq.write_table(table, parquet_name_file)
                # atualiza lista de parquet
                local_files_list_parquet.append(parquet_name_file)
                # faz o upload do parquet transformado
                for parquet_file in local_files_list_parquet:
                    blob_name = f"conformed/govrs/agricultura/ervamate/{parquet_name}/{date_file}/{parquet_name}_{date_file}.parquet"
                    blob = connect_to_adls(container_name, blob_name)
                    with open(f'{data_path}/{parquet_name}_{date_file}.csv', 'rb') as data:
                        blob.upload_blob(data)

        except Exception as e:
            print(f"Upload parquet failed.{e}")
        else:
            print('Upload parquet sucessful')
            return True'''

        # if url_name == 'dee-977.csv':
        #     file_name = f'areacolhida_{ingestion_date}.csv'
        #     filepath = os.path.join(raw_data_path, file_name)
        #     with open(filepath, 'wb') as file_doc:
        #         file_doc.write(response.content)
        #         print(f'{file_name} was sucessful extracted')
        # elif url_name == 'dee-1690.csv':
        #     file_name = f'areadestinadacolheita_{ingestion_date}.csv'
        #     filepath = os.path.join(raw_data_path, file_name)
        #     with open(filepath, 'wb') as file_doc:
        #         file_doc.write(response.content)
        #         print(f'{file_name} was sucessful extracted')
        # elif url_name == 'dee-978.csv':
        #     file_name = f'quantidadeproduzida_{ingestion_date}.csv'
        #     filepath = os.path.join(raw_data_path, file_name)
        #     with open(filepath, 'wb') as file_doc:
        #         file_doc.write(response.content)
        #         print(f'{file_name} was sucessful extracted')
        # elif url_name == 'dee-979.csv':
        #     file_name = f'rendimentomedio_{ingestion_date}.csv'
        #     filepath = os.path.join(raw_data_path, file_name)
        #     with open(filepath, 'wb') as file_doc:
        #         file_doc.write(response.content)
        #         print(f'{file_name} was sucessful extracted')
        # elif url_name == 'dee-980.csv':
        #     file_name = f'valorproducao_{ingestion_date}.csv'
        #     filepath = os.path.join(raw_data_path, file_name)
        #     with open(filepath, 'wb') as file_doc:
        #         file_doc.write(response.content)
        #         print(f'{file_name} was sucessful extracted')
