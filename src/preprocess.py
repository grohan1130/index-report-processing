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

def index_aggregation_by_household_size(df):
    filtered_df = df[df['Attribute Name'].str.contains('Household Size -', na=False)]
    
    # Extract household size category from the 'Attribute Name' column
    filtered_df = filtered_df.copy()
    filtered_df['Household_Size'] = filtered_df['Attribute Name'].str.replace('Household Size - ', '')
    
    # Categorize into 1-4 and 5+
    def categorize_household_size(size_str):
        if 'One person' in size_str:
            return 'One person'
        elif 'Two persons' in size_str:
            return 'Two persons'
        elif 'Three persons' in size_str:
            return 'Three persons'
        elif 'Four persons' in size_str:
            return 'Four persons'
        else:  # Five, Six, Seven, Eight, Nine or more persons
            return 'Five+ persons'
    
    filtered_df['Household_Size_Category'] = filtered_df['Household_Size'].apply(categorize_household_size)
    
    # Group by household size category and calculate the index
    result = filtered_df.groupby('Household_Size_Category', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()
    
    # Calculate the index
    result['Index'] = (result['Persona Attribute Proportion'] / result['Base Adjusted Population Attribute Proportion']) * 100
    
    return result






def main():
    df = load_pandas_and_format()
    print(index_aggregation_by_age(df))
    print(index_aggregation_by_household_size(df))

if __name__ == "__main__":
    main()

