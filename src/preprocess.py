import pandas as pd


def to_csv():
    df = pd.read_excel("raw_input_files/raw_index_report.xlsx", sheet_name=None)

    for sheet_name, data in df.items():
        csv_file_name = f"processed_output/{sheet_name}.csv"
        data.to_csv(csv_file_name, index=False)
        print(f"Saved {csv_file_name}")


to_csv()
