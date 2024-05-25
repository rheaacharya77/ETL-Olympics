from scripts.transform import extract_transform
from scripts.load import save_df_to_blob_storage

def main():
    final_dataset = extract_transform()
    save_df_to_blob_storage(final_dataset)

if __name__ == "__main__":
    main()
