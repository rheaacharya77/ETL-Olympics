import pandas as pd

def transform(datasets):
   athletes = datasets['Athletes.csv']
   medals = datasets['Medals.csv']
   entries_gender = datasets['EntriesGender.csv']
   coaches = datasets['Coaches.csv']
  
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
   print(final_dataset)
   return final_dataset
