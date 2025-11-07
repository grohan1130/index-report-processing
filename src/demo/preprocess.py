import pandas as pd
import os


def load_pandas_and_format():
    # Get path relative to this script's location
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, '..', '..', 'raw_input_files', 'raw_index_report.xlsx')
    raw_pandas_df = pd.read_excel(file_path, sheet_name="Index Report", header=1)
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
    
    # Rename columns for consistency
    result = result.rename(columns={'Age_Bin': 'Attribute Name'})
    
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
    
    # Rename columns for consistency
    result = result.rename(columns={'Household_Size_Category': 'Attribute Name'})
    
    return result

def index_aggregation_by_household_income(df):
    # Keep rows that have "Income Tiers="
    mask = df["Attribute Name"].str.contains("Income Tiers=", na=False)
    filtered_df = df.loc[mask].copy()

    # Extract the income text after "Income Tiers="
    filtered_df["Income_Text"] = (
        filtered_df["Attribute Name"]
        .str.replace("Income Tiers=", "", regex=False)
        .str.strip()
    )

    # Helper: convert "$50,000 - $74,999" → 50000
    def get_lower_bound(text):
        text = text.replace("$", "").replace(",", "").lower()
        if "or more" in text:
            return 250000
        if "-" in text:
            return int(text.split("-")[0].strip())
        return 0

    # Determine bin based on lower bound
    def income_bin(text):
        low = get_lower_bound(text)
        if low < 50000:
            return "0–$49,999"
        elif low < 100000:
            return "$50,000–$99,999"
        elif low < 150000:
            return "$100,000–$149,999"
        elif low < 200000:
            return "$150,000–$199,999"
        elif low < 250000:
            return "$200,000–$249,999"
        else:
            return "$250,000+"

    filtered_df["Income_Bin"] = filtered_df["Income_Text"].apply(income_bin)

    # Aggregate and calculate Index
    result = (
        filtered_df.groupby("Income_Bin", observed=True)
        .agg({
            "Persona Attribute Proportion": "sum",
            "Base Adjusted Population Attribute Proportion": "sum"
        })
        .reset_index()
    )

    result["Index"] = (
        result["Persona Attribute Proportion"]
        / result["Base Adjusted Population Attribute Proportion"]
    ) * 100

    # Enforce consistent order
    order = [
        "0–$49,999",
        "$50,000–$99,999",
        "$100,000–$149,999",
        "$150,000–$199,999",
        "$200,000–$249,999",
        "$250,000+",
    ]
    result["Attribute Name"] = pd.Categorical(result["Income_Bin"], categories=order, ordered=True)
    result = result.sort_values("Attribute Name").reset_index(drop=True)

    return result

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

    # Rename columns for consistency
    result = result.rename(columns={'Ethnicity_Bin': 'Attribute Name'})

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
    
    # Rename columns for consistency
    result = result.rename(columns={'Attribute Name': 'Attribute Name'})
    
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
    
    # Rename columns for consistency
    result = result.rename(columns={'Generation': 'Attribute Name'})
    
    return result

def index_aggregation_by_has_kids(df):

    has_kids_mask = df['Attribute Name'].str.contains('Presence of Children -', na=False)
    modeled_rank_mask = df['Attribute Name'].str.contains('Modeled Rank', na=False)

    # Exclude rows that mention Modeled Rank
    filtered_df = df.loc[has_kids_mask & ~modeled_rank_mask].copy()

    # Extract presence of children from the 'Attribute Name' column
    filtered_df['Has_Kids'] = filtered_df['Attribute Name'].str.replace('Presence of Children - ', '', regex=False)

    # Extract presence of children from the 'Attribute Name' column
    filtered_df = filtered_df.copy()

    filtered_df['Has_Kids'] = filtered_df['Attribute Name'].str.replace('Presence of Children - ', '')
    # Group by presence of children and calculate the index
    result = filtered_df.groupby('Has_Kids', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()
    # Calculate the index
    result['Index'] = (result['Persona Attribute Proportion'] / result['Base Adjusted Population Attribute Proportion']) * 100

    # Rename columns for consistency
    result = result.rename(columns={'Has_Kids': 'Attribute Name'})

    return result

def index_aggregation_by_urbanicity(df):
    # Keep rows that have "Census: Rural-Urban County Size Code - ..."
    prefix_re = r'Census:\s*Rural-Urban County Size Code\s*-\s*'
    has_urb = df['Attribute Name'].str.contains(prefix_re, case=False, na=False)
    filtered_df = df.loc[has_urb].copy()

    # Extract the urbanicity label after the hyphen
    filtered_df['Urbanicity'] = (
        filtered_df['Attribute Name']
        .str.extract(prefix_re + r'(.+)$')[0]
        .str.strip()
    )

    # Aggregate and compute Index
    result = (
        filtered_df.groupby('Urbanicity', observed=True)
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

    # Rename columns for consistency
    result = result.rename(columns={'Urbanicity': 'Attribute Name'})

    return result

def index_aggregation_by_household_education(df):
    # Keep rows that have "Household Education -"
    filtered_df = df[df['Attribute Name'].str.contains('Household Education -', na=False)].copy()
    
    # Extract education level from the 'Attribute Name' column
    filtered_df['Education_Level'] = filtered_df['Attribute Name'].str.replace('Household Education - ', '', regex=False)
    
    # Define the education level mapping
    education_mapping = {
        'Some high school or less': 'Some high school or less',
        'High school': 'High school',
        'Some college': 'Some college',
        'College': 'College',
        'Graduate school': 'Graduate school'
    }
    
    # Map education levels to standard categories
    filtered_df['Education_Category'] = filtered_df['Education_Level'].map(education_mapping)
    
    # Group by education category and calculate the index
    result = filtered_df.groupby('Education_Category', observed=True).agg({
        'Persona Attribute Proportion': 'sum',
        'Base Adjusted Population Attribute Proportion': 'sum'
    }).reset_index()
    
    # Calculate the index
    result['Index'] = (
        result['Persona Attribute Proportion']
        / result['Base Adjusted Population Attribute Proportion']
    ) * 100
    
    # Rename columns for consistency
    result = result.rename(columns={'Education_Category': 'Attribute Name'})
    
    return result


if __name__ == "__main__":
    df = load_pandas_and_format()
    print(index_aggregation_by_household_income(df))
