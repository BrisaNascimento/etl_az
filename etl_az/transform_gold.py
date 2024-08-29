from datetime import datetime
import pandas as pd
import numpy as np
import pyarrow as pa
import os
import pyarrow.parquet as pq
from etl_az.settings import Settings

# Transform data - cleaning and filtering - Gold
# To opt-in to the future behavior
pd.set_option('future.no_silent_downcasting', True)

# Set ingestion date
ingestion_date = datetime.now().strftime('%Y-%m-%d')
# Set data_path
data_path = Settings().LOCAL_DATA_PATH

df_ac = pd.read_parquet(f'{data_path}areacolhida_{ingestion_date}.parquet')
df_ad = pd.read_parquet(f'{data_path}areadestinadacolheita_{ingestion_date}.parquet')
df_qp = pd.read_parquet(f'{data_path}quantidadeproduzida_{ingestion_date}.parquet')
df_rm = pd.read_parquet(f'{data_path}rendimentomedio_{ingestion_date}.parquet')
df_vp = pd.read_parquet(f'{data_path}valorproducao_{ingestion_date}.parquet')
df_mu = pd.read_parquet(f'{data_path}municipios_{ingestion_date}.parquet')

# CREATE TABLE VALOR PRODUCAO ENRICHED
# create columns from monetary reference on df 'valorproducao' for conversion in local currency


def add_um_column(df, column_name: str, new_column_name: str):

    # Define conditions to new column content
    conditions = [
        df[column_name].between(1974, 1985, inclusive='both'),
        df[column_name].between(1986, 1988, inclusive='both'),
        df[column_name] == 1989,
        df[column_name].between(1990, 1992, inclusive='both'),
        df[column_name] == 1993,
        df[column_name] >= 1994
    ]
    # Define correspondent monetary unit
    um = ['Cruzeiro (Cr$)', 'Cruzado (Cz$)', 'Cruzado Novo (NCz$)', 'Cruzeiro (Cr$)', 'Cruzeiro Real (CR$)', 'Real (R$)']
    # Create new column
    df[new_column_name] = np.select(conditions, um, default='Undefined')
    return df


add_um_column(df_vp, 'ano', 'unidade_monetaria')


# Create a column to conversion rate to transform past local currency to actual local currency (Real)

def add_convertion_column(df, column_name: str, new_column_name: str):

    # Define conditions to new column content
    conditions = [
        df[column_name].between(1974, 1985, inclusive='both'),
        df[column_name].between(1986, 1988, inclusive='both'),
        df[column_name] == 1989,
        df[column_name].between(1990, 1992, inclusive='both'),
        df[column_name] == 1993,
        df[column_name] >= 1994
    ]
    # Define convertion rate
    cr = [0.000000000000363636363636364, 0.000000000363636363636364, 0.000000363636363636364, 0.000000363636363636364, 0.000363636363636364, 1]
    # Create new column
    df[new_column_name] = np.select(conditions, cr, default=0)
    return df


add_convertion_column(df_vp, 'ano', 'taxa_conversao')

# create a column production value in thousands of actual local currency (Real)
df_vp['valor_producao_mil_reais'] = df_vp['valor_producao_mil'] * df_vp['taxa_conversao']

# create a column production value in actual local currency (Real)
df_vp['valor_producao_reais'] = df_vp['valor_producao_mil_reais'] * 1000

# CREATE TRUSTED TABLE FOR GOLD

df_abt = df_ad.copy()

df_abt = df_abt.merge(df_ac, on=['id_ibge_municipio', 'ano'], how='left') \
            .merge(df_qp, on=['id_ibge_municipio', 'ano'], how='left') \
            .merge(df_rm, on=['id_ibge_municipio', 'ano'], how='left') \
            .merge(df_vp, on=['id_ibge_municipio', 'ano'], how='left') \
            .merge(df_mu, on=['id_ibge_municipio'], how='left')


# drop unnecessary columns in this view

df_abt.drop(['valor_producao_mil', 'unidade_monetaria', 'taxa_conversao', 'valor_producao_mil_reais'], axis=1, inplace=True)


# Reorganize columns
sequence = [
            'id_ibge_municipio', 'municipio', 'latitude',
            'longitude', 'ano', 'area_destinada', 'area_colhida',
            'quantidade_produzida', 'rendimento_medio', 'valor_producao_reais',
]
df_abt = df_abt[sequence]


# Save all dataframes to transformed data path

# valorproducao (enriched)
table = pa.Table.from_pandas(df_vp)
pq.write_table(table, f'{data_path}/transformed/valorproducaoenriq_{ingestion_date}.parquet')
# ABT (producaoervamate)
table = pa.Table.from_pandas(df_abt)
pq.write_table(table, f'{data_path}/transformed/producaoervamate_{ingestion_date}.parquet')