from numpy import result_type
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

<<<<<<< HEAD
def index_aggregation_by_household_income(df):
    filtered_df = df[df['Attribute Name'].str.contains('Income Tiers=', na=False)]
    
    # Extract income tier category from the 'Attribute Name' column
    filtered_df = filtered_df.copy()
    filtered_df['Income_Tier_Category'] = filtered_df['Attribute Name'].str.replace('Income Tiers=', '')
    
    # Group by income tier category and calculate the index
    result = filtered_df.groupby('Income_Tier_Category', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()
    
    # Calculate the index
    result['Index'] = (result['Persona Attribute Proportion'] / result['Base Adjusted Population Attribute Proportion']) * 100
    
=======
def index_aggregation_by_ethnicity(df):
    filtered_df = df[df['Attribute Name'].str.contains('Ethnicity Groups -', na=False)]

    #Extract ethnicity from the 'Attribute Name' column
    filtered_df = filtered_df.copy()

    # Extract the subgroup after the hyphen
    # e.g., "Ethnicity Groups - Eastern European" -> "Eastern European"
    filtered_df['Ethnicity_Subgroup'] = (
        filtered_df['Attribute Name']
        .str.extract(r'Ethnic\w*\s+Groups\s*-\s*(.+)$')[0]
        .str.strip()
    )

    subgroup_to_bin = {
        # African American
        'African American': 'African American',

        # White/European
        'Eastern European': 'White/European',
        'Jewish': 'White/European',
        'Western European': 'White/European',
        'Scandinavian': 'White/European',
        'Middle Eastern': 'White/European',
        'Mediterranean': 'White/European',

        # Asian
        'Polynesian': 'Asian',
        'Central and Southwest Asia': 'Asian',
        'Southeast Asia': 'Asian',
        'Far Eastern': 'Asian',

        # Hispanic
        'Hispanic': 'Hispanic',

        # Other
        'Uncoded': 'Other',
        'Other Groups': 'Other',
        'Native American': 'Other',
    }

    # Normalize case by matching keys case-sensitively after stripping
    filtered_df['Ethnicity_Bin'] = (
        filtered_df['Ethnicity_Subgroup']
        .map(lambda x: subgroup_to_bin.get(x, 'Other'))
    )

    bin_order = ['African American', 'White/European', 'Asian', 'Hispanic', 'Other']
    filtered_df['Ethnicity_Bin'] = pd.Categorical(filtered_df['Ethnicity_Bin'],
                                                  categories=bin_order, ordered=True)

    # Aggregate like age function
    result = (
        filtered_df
        .groupby('Ethnicity_Bin', observed=True)
        .agg({
            'Persona Attribute Proportion': 'sum',
            'Base Adjusted Population Attribute Proportion': 'sum'
        })
        .reset_index()
    )

    # Compute index
    result['Index'] = (
        result['Persona Attribute Proportion']
        / result['Base Adjusted Population Attribute Proportion']
    ) * 100

    return result

def index_aggregation_by_gender(df):
    #Keep rows that have "Gender -", but drop anything under "Children:"
    has_gender = df['Attribute Name'].str.contains(r'\bGender\s*-\s*', case=False, na=False)
    is_children = df['Attribute Name'].str.contains(r'\bChildren\b', case=False, na=False)
    filtered_df = df.loc[has_gender & ~is_children].copy()

    # Extract the gender label (Male/Female/Both)
    filtered_df['Gender'] = (
        filtered_df['Attribute Name']
        .str.extract(r'Gender\s*-\s*(Male|Female|Both)')[0]
        .str.title()
    )

    
    result = filtered_df.groupby('Attribute Name', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()

    # Calculate the index
    result['Index'] = (result['Persona Attribute Proportion'] / result['Base Adjusted Population Attribute Proportion']) * 100
    return result


 
def index_aggregation_by_generation(df):
    # Keep rows that have "Individual Generation - ..."
    has_gen = df['Attribute Name'].str.contains(r'\bIndividual\s+Generation\s*-\s*', case=False, na=False)
    filtered_df = df.loc[has_gen].copy()

    # Extract the generation label (Gen X / Gen Z / Baby Boomer / Millennials)
    filtered_df['Generation'] = (
        filtered_df['Attribute Name']
        .str.extract(r'Individual\s+Generation\s*-\s*(Gen X|Gen Z|Baby Boomer|Millennials)')[0]
        .str.title()
        .str.replace('Gen X', 'Gen X', regex=False)           # keep exact casing for Gen X
        .str.replace('Gen Z', 'Gen Z', regex=False)           # keep exact casing for Gen Z
        .str.replace('Baby Boomer', 'Baby Boomer', regex=False)
        .str.replace('Millennials', 'Millennials', regex=False)
    )

    # Aggregate and compute Index
    result = (
        filtered_df.groupby('Generation', observed=True)
        .agg({
            'Persona Attribute Proportion': 'sum',
            'Base Adjusted Population Attribute Proportion': 'sum'
        })
        .reset_index()
    )
    result['Index'] = (
        result['Persona Attribute Proportion']
        / result['Base Adjusted Population Attribute Proportion']
    ) * 100

>>>>>>> origin/mike
    return result



def main():
    df = load_pandas_and_format()
<<<<<<< HEAD
    print(index_aggregation_by_age(df))
    print(index_aggregation_by_household_size(df))

=======
    #print(index_aggregation_by_age(df))
    #print(index_aggregation_by_ethnicity(df))
    #print(index_aggregation_by_gender(df))
    print(index_aggregation_by_generation(df))
>>>>>>> origin/mike
if __name__ == "__main__":
    main()

