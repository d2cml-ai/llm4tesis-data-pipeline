from azure.storage.blob import BlobServiceClient, BlobClient
from Secrets import BLOB_SERVICE_CONNECTION_STRING
from Constants import TEMP_DIR_PATH

def get_blob_service() -> BlobServiceClient:
        blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(BLOB_SERVICE_CONNECTION_STRING)
        return blob_service_client

def get_blob_client(blob_name: str, container_name: str, blob_service_client: BlobServiceClient) -> BlobClient:
        blob_client: BlobClient = blob_service_client.get_blob_client(
                container_name,
                blob_name
        )
        return blob_client