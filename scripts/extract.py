import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")

def upload_to_blob(file_name):
    blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    blob_client = blob_service_client.get_blob_client(container="olympics", blob=file_name)
    with open(os.path.join("../data", file_name), "rb") as data:
        blob_client.upload_blob(data)

file_names = ["Athletes.csv", "Coaches.csv", "EntriesGender.csv", "Medals.csv"]

for file_name in file_names:
    upload_to_blob(file_name)
