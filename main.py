from scripts.transform import extract_transform
from scripts.load import load_to_sql

def main():
    final_dataset = extract_transform()
    # load_to_sql(final_dataset)

if __name__ == "__main__":
    main()
