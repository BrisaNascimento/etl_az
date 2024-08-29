from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import ContainerClient
from etl_az.settings import Settings


def connect_to_adls(container_name: str, blob_name: str):
    # Create client from a connection string
    connection_string = Settings().CONNECTION_STRING_DL
    blob = BlobClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name,
        blob_name=blob_name
    )
    return blob


def list_blobs_adls(container_name: str):
    connection_string = Settings().CONNECTION_STRING_DL
    container = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name)
    blob_list = container.list_blobs()
    for blob in blob_list:
        print(blob.name + '\n')


def list_all_blobs_adls(container_name: str):
    connection_string = Settings().CONNECTION_STRING_DL
    container = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=container_name)
    blob_list = []
    for blob in container.list_blobs():
        blob_list.append(blob)
    print(blob_list)
