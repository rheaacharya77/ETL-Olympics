import os
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Access the environment variables
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")

if AZURE_CONNECTION_STRING is None:
    raise ValueError("AZURE_CONNECTION_STRING is not set in the environment variables.")

# Load data from Azure Blob Storage
def load_csv_from_blob(file_name, container_client):
   blob_client = container_client.get_blob_client(blob=file_name)
   csv_data = blob_client.download_blob().readall()
   return pd.read_csv(io.BytesIO(csv_data), encoding='latin-1')

def extract_transform():
   blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
   container_client = blob_service_client.get_container_client("olympics")

   # Load datasets
   athletes = load_csv_from_blob("Athletes.csv", container_client)
   medals = load_csv_from_blob("Medals.csv",container_client)
   entries_gender = load_csv_from_blob("EntriesGender.csv",container_client)
   coaches = load_csv_from_blob("Coaches.csv", container_client)
  
   # Merge datasets
   merged_data = pd.merge(athletes, medals, how='left', left_on='Country', right_on='Team_Country') 
   merged_data = pd.merge(merged_data, entries_gender, how='left', on='Discipline')
   merged_data = pd.merge(merged_data, coaches, how='left', on=['Country', 'Discipline'])
  
   # Validate and transform data
   merged_data.dropna(inplace=True)
   
   merged_data['Country'] = merged_data['Country'].str.upper()
   merged_data['PersonName'] = merged_data['PersonName'].str.title()
   merged_data['Name'] = merged_data['Name'].str.title()

   # Select relevant columns
   final_dataset = merged_data[['PersonName', 'Country', 'Discipline', 'Rank','Gold', 'Silver', 'Bronze', 'Total_x','Total_y','Name']].copy()

   # Rename columns for clarity
   final_dataset.rename(columns={'PersonName':'Athlete','Total_x':'Total_Medal','Total_y':'Total_Athlete','Name':'Coach'}, inplace=True)
 
   # Remove duplicate rows
   final_dataset.drop_duplicates(inplace=True)
   
   final_dataset['Id'] = range(1, len(final_dataset) + 1)
   
   # Define the desired column order
   desired_order = ['Id', 'Athlete', 'Country', 'Discipline', 'Coach','Rank','Gold','Silver','Bronze','Total_Medal','Total_Athlete'] + [col for col in final_dataset.columns if col not in ['Id', 'Athlete', 'Country', 'Discipline', 'Coach','Rank','Gold','Silver','Bronze','Total_Medal','Total_Athlete']]
    
   # Reorder the DataFrame columns
   final_dataset = final_dataset[desired_order]
  
   return final_dataset
