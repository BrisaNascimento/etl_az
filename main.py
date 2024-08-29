from datetime import datetime
from etl_az.extract import Extraction
from etl_az.upload import Uploader
from etl_az.utils import csv_to_parquet, clear_data
from etl_az.download import (
    download_from_conformed_adls,
    download_from_silver_adls,
    download_from_gold_adls
)
import numpy as np

# Define date for downloads
date_fd = datetime.now().strftime('%Y-%m-%d')

# Extract data from source to local data path
Extraction().extract_erva_mate()

# Upload raw data to raw zone ADLS
Uploader().upload_erva_mate_to_raw('bronze')

# Tranform csv data to parquet
csv_to_parquet()

# Upload parquet data from source to conformed zone ADLS
Uploader().upload_erva_mate_to_conformed('bronze')

# Delete local diretory
clear_data()

# Capture data from Data Lake (Data Path)
download_from_conformed_adls(date_fd, 'bronze')

# Prepare for Silver
# read files from data path Clean and filter data into transformed data path


def transform_data_silver(script_path):
    with open(script_path, "r") as file:
        code = file.read()
        exec(code)


if __name__ == "__main__":
    script_path = "./etl_az/transform_silver.py"
    transform_data_silver(script_path)

# Upload from data path to Silver Layer
Uploader().upload_erva_mate_to_silver('silver')


# Delete local diretory
clear_data()


# Capture data from Data Lake (Silver)

download_from_silver_adls(date_fd, 'silver')

# Prepare for Gold
# Read files from data path, enrich and save data into transformed data path


def transform_data_gold(script_path):
    with open(script_path, "r") as file:
        code = file.read()
        exec(code)


if __name__ == "__main__":
    script_path = "./etl_az/transform_gold.py"
    transform_data_gold(script_path)

# Upload to Gold Layer

Uploader().upload_erva_mate_to_gold('gold')


# Delete local diretory
clear_data()

# Capture data from Data Lake (Gold)
# only for generate a database for PowerBI report without DL conection

download_from_gold_adls(date_fd, 'gold')
