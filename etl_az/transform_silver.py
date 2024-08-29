import pandas as pd
import os
import numpy as np
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from etl_az.settings import Settings

# Transform data - cleaning and filtering - Silver
# To opt-in to the future behavior
pd.set_option('future.no_silent_downcasting', True)

# Set ingestion date
ingestion_date = datetime.now().strftime('%Y-%m-%d')
# Set data_path
data_path = Settings().LOCAL_DATA_PATH

# Transform parquet into dataframe

df_areacolhida = pd.read_parquet(f'{data_path}areacolhida_{ingestion_date}.parquet')
df_areadestinadacolheita = pd.read_parquet(f'{data_path}areadestinadacolheita_{ingestion_date}.parquet')
df_quantidadeproduzida = pd.read_parquet(f'{data_path}quantidadeproduzida_{ingestion_date}.parquet')
df_rendimentomedio = pd.read_parquet(f'{data_path}/rendimentomedio_{ingestion_date}.parquet')
df_valorproducao = pd.read_parquet(f'{data_path}valorproducao_{ingestion_date}.parquet')

# Renaming columns


def rename_columns(df):
    columns_list = list(df.columns)
    reg_columns = columns_list[0:4]
    year_columns = columns_list[4:]
    new_reg_columns = ['municipio', 'ibge', 'latitude', 'longitude']
    new_year_columns = []
    for year in year_columns:
        new_year = year.split('(')[0].split(' ')[-2]
        new_year_columns.append(new_year)
    new_year_columns
    # union two lists
    df.columns = new_reg_columns + new_year_columns
    return df


rename_columns(df_areacolhida)
rename_columns(df_areadestinadacolheita)
rename_columns(df_quantidadeproduzida)
rename_columns(df_rendimentomedio)
rename_columns(df_valorproducao)

# Replacing fields with '-' per null

df_areacolhida.replace('-', np.nan, inplace=True)
df_areadestinadacolheita.replace('-', np.nan, inplace=True)
df_quantidadeproduzida.replace('-', np.nan, inplace=True)
df_rendimentomedio.replace('-', np.nan, inplace=True)
df_valorproducao.replace('-', np.nan, inplace=True)

# Replacing fields with '...' per null - there is an error future warning

df_areacolhida.replace('...', np.nan, inplace=True)
df_areadestinadacolheita.replace('...', np.nan, inplace=True)
df_quantidadeproduzida.replace('...', np.nan, inplace=True)
df_rendimentomedio.replace('...', np.nan, inplace=True)
df_valorproducao.replace('...', np.nan, inplace=True)

# Retyping numerical columns to float .2


def retype_num_columns(df):
    num_columns = list(df.columns)[4:]
    for col in num_columns:
        df[col] = df[col].astype(float).round(2)
    return df


retype_num_columns(df_areacolhida)
retype_num_columns(df_areadestinadacolheita)
retype_num_columns(df_quantidadeproduzida)
retype_num_columns(df_rendimentomedio)
retype_num_columns(df_valorproducao)


# Retyping categorical columns to str


def retype_cat_columns(df):
    cat_columns = list(df.columns)[0:4]
    for col in cat_columns:
        df[col] = df[col].astype(str)
    return df


retype_cat_columns(df_areacolhida)
retype_cat_columns(df_areadestinadacolheita)
retype_cat_columns(df_quantidadeproduzida)
retype_cat_columns(df_rendimentomedio)
retype_cat_columns(df_valorproducao)

# transposing year columns


def transpose_year_columns(df):
    id_vars = list(df.columns)[0:4]
    df_t = df.melt(
        id_vars=id_vars,
        var_name='ano',
        value_name='valor'
    )
    return df_t


df_em_areacolhida = transpose_year_columns(df_areacolhida)
df_em_areadestinadacolheita = transpose_year_columns(df_areadestinadacolheita)
df_em_quantidadeproduzida = transpose_year_columns(df_quantidadeproduzida)
df_em_rendimentomedio = transpose_year_columns(df_rendimentomedio)
df_em_valorproducao = transpose_year_columns(df_valorproducao)


# reset index

df_em_areacolhida.reset_index(drop=True)
df_em_areadestinadacolheita.reset_index(drop=True)
df_em_quantidadeproduzida.reset_index(drop=True)
df_em_rendimentomedio.reset_index(drop=True)
df_em_valorproducao.reset_index(drop=True)


# check unique values from municipios, ibge, latitude, longitude if necessary
# unicos_teste = list(df_em_areadestinadacolheita['ibge'].unique())
# len(unicos_teste)  # 497

# checking latitude, longitude

# df_filtered = df_em_areadestinadacolheita[df_em_areadestinadacolheita['valor'].isnull()]
# df_filtered

# create new table DIM

df_col_1 = df_em_areacolhida[['municipio', 'ibge', 'latitude', 'longitude']]
df_col_2 = df_em_areadestinadacolheita[['municipio', 'ibge', 'latitude', 'longitude']]
df_col_3 = df_em_quantidadeproduzida[['municipio', 'ibge', 'latitude', 'longitude']]
df_col_4 = df_em_rendimentomedio[['municipio', 'ibge', 'latitude', 'longitude']]
df_col_5 = df_em_valorproducao[['municipio', 'ibge', 'latitude', 'longitude']]

df_dim_municipios = pd.concat(
    [df_col_1, df_col_2, df_col_3, df_col_4, df_col_5],
    ignore_index=True
)

# drop duplicated rows from dim, the reference is two first columns municipio and ibge
df_dim_municipios.drop_duplicates(subset=['municipio', 'ibge'], keep='first', inplace=True)


# check column frequency

# frequencia_duplicados = df_dim_municipios['municipio'].value_counts().reset_index()
# frequencia_duplicados
# frequencia_duplicados.info()
# frequencia_duplicados[frequencia_duplicados['count'] >= 2]
# df_dim_municipios[df_dim_municipios['municipio'] == 'Alpestre']

# Reorganize DIM columns
sequence = ['ibge', 'municipio', 'latitude', 'longitude']
df_dim_municipios = df_dim_municipios[sequence]

#  Rename ibge column to 'id_ibge_municipio'
df_dim_municipios = df_dim_municipios.rename(columns={'ibge': 'id_ibge_municipio'})

# Drop columns from dim in fact dataframes


def drop_fact(df):
    df.drop(columns=['municipio', 'latitude', 'longitude'], inplace=True)
    return df


drop_fact(df_em_areacolhida)
drop_fact(df_em_areadestinadacolheita)
drop_fact(df_em_quantidadeproduzida)
drop_fact(df_em_rendimentomedio)
drop_fact(df_em_valorproducao)

# Rename 'ibge' column on fact Dataframes to 'id_ibge_municipio'

df_em_areacolhida = df_em_areacolhida.rename(columns={'ibge': 'id_ibge_municipio'})
df_em_areadestinadacolheita = df_em_areadestinadacolheita.rename(columns={'ibge': 'id_ibge_municipio'})
df_em_quantidadeproduzida = df_em_quantidadeproduzida.rename(columns={'ibge': 'id_ibge_municipio'})
df_em_rendimentomedio = df_em_rendimentomedio.rename(columns={'ibge': 'id_ibge_municipio'})
df_em_valorproducao = df_em_valorproducao.rename(columns={'ibge': 'id_ibge_municipio'})

# Rename 'valor' column on fact Dataframes

df_em_areacolhida = df_em_areacolhida.rename(columns={'valor': 'area_colhida'})
df_em_areadestinadacolheita = df_em_areadestinadacolheita.rename(columns={'valor': 'area_destinada'})
df_em_quantidadeproduzida = df_em_quantidadeproduzida.rename(columns={'valor': 'quantidade_produzida'})
df_em_rendimentomedio = df_em_rendimentomedio.rename(columns={'valor': 'rendimento_medio'})
df_em_valorproducao = df_em_valorproducao.rename(columns={'valor': 'valor_producao_mil'})

# Retyping column 'Ano' to int


def retype_year_column(df):
    df['ano'] = df['ano'].astype(int)
    return df


retype_year_column(df_em_areacolhida)
retype_year_column(df_em_areadestinadacolheita)
retype_year_column(df_em_quantidadeproduzida)
retype_year_column(df_em_rendimentomedio)
retype_year_column(df_em_valorproducao)


# Save all dataframes in data path (tosilver) parquet format

# municipios
table = pa.Table.from_pandas(df_dim_municipios)
pq.write_table(table, f'{data_path}transformed/municipios_{ingestion_date}.parquet')
# areacolhida
table = pa.Table.from_pandas(df_em_areacolhida)
pq.write_table(table, f'{data_path}transformed/areacolhida_{ingestion_date}.parquet')
# areadestinadacolheita
table = pa.Table.from_pandas(df_em_areadestinadacolheita)
pq.write_table(table, f'{data_path}transformed/areadestinadacolheita_{ingestion_date}.parquet')
# quantidadeproduzida
table = pa.Table.from_pandas(df_em_quantidadeproduzida)
pq.write_table(table, f'{data_path}transformed/quantidadeproduzida_{ingestion_date}.parquet')
# rendimentomedio
table = pa.Table.from_pandas(df_em_rendimentomedio)
pq.write_table(table, f'{data_path}transformed/rendimentomedio_{ingestion_date}.parquet')
# valorproducao
table = pa.Table.from_pandas(df_em_valorproducao)
pq.write_table(table, f'{data_path}transformed/valorproducao_{ingestion_date}.parquet')
