from scripts.extract import extract
from scripts.transform import transform
from scripts.load import save_df_to_blob_storage

def main():
    extracted_files = extract()
    final_dataset = transform(extracted_files)
    save_df_to_blob_storage(final_dataset)

if __name__ == "__main__":
    main()
