import pandas as pd


def load_pandas_and_format():
    raw_pandas_df = pd.read_excel("raw_input_files/raw_index_report.xlsx", sheet_name="Index Report", header=1)
    return raw_pandas_df

def index_aggregation_by_age(df):
    filtered_df = df[df['Attribute Name'].str.contains('Individuals of Age -', na=False)]
    
    # Extract age from the 'Attribute Name' column
    filtered_df = filtered_df.copy()
    filtered_df['Age'] = filtered_df['Attribute Name'].str.extract(r'Individuals of Age - (\d+)')[0].astype(int)
    
    # Define age bins
    bins = [18, 25, 35, 45, 55, 65, 100]
    labels = ['18-24', '25-34', '35-44', '45-54', '55-64', '65+']
    filtered_df['Age_Bin'] = pd.cut(filtered_df['Age'], bins=bins, labels=labels, right=False)
    
    # Group by age bin and calculate the index
    result = filtered_df.groupby('Age_Bin', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()
    
    # Calculate the index
    result['Index'] = (result['Persona Attribute Proportion'] / result['Base Adjusted Population Attribute Proportion']) * 100
    
    return result






def main():
    df = load_pandas_and_format()
    print(index_aggregation_by_age(df))

if __name__ == "__main__":
    main()

