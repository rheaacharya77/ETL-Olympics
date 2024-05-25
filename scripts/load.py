import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")

# Save transformed DataFrame to Blob Storage
def save_df_to_blob_storage(df):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client('olympics')
    blob_client = container_client.get_blob_client('transformed_data.csv')
    blob_client.upload_blob(df.to_csv(index=False), overwrite=True)


