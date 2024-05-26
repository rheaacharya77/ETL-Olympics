 
import os
import pandas as pd

def extract():
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
    file_names = ["Athletes.csv", "Coaches.csv", "EntriesGender.csv", "Medals.csv"]
    
    dfs = {}
    for file_name in file_names:
        file_path = os.path.join(data_dir, file_name)
        if os.path.exists(file_path):
            # encoding parameter
            df = pd.read_csv(file_path, encoding='latin1')  
            dfs[file_name] = df
        else:
            print(f"File not found: {file_path}")
    return dfs

