
import os
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import pandas as pd
import io

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

# Validation
   downloaded_blob = blob_client.download_blob().content_as_text()
   uploaded_data = pd.read_csv(io.StringIO(downloaded_blob))
  
   if len(uploaded_data) != len(df):
       print("Data upload validation failed: row count mismatch.")
   else:
       print("Data upload validation successful.")