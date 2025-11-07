import pandas as pd
from preprocess import (
    load_pandas_and_format,
    index_aggregation_by_age,
    index_aggregation_by_household_size,
    index_aggregation_by_household_income,
    index_aggregation_by_ethnicity,
    index_aggregation_by_gender,
    index_aggregation_by_generation,
    index_aggregation_by_has_kids,
    index_aggregation_by_urbanicity,
    index_aggregation_by_household_education
)


def merge_all_index_aggregations(df):
    """
    Merges all index aggregation results into a single dataframe.
    
    Args:
        df: The raw dataframe from load_pandas_and_format()
        
    Returns:
        A single dataframe with all index aggregation results concatenated
    """
    # Get all index aggregation results
    age_df = index_aggregation_by_age(df)
    household_size_df = index_aggregation_by_household_size(df)
    household_income_df = index_aggregation_by_household_income(df)
    ethnicity_df = index_aggregation_by_ethnicity(df)
    gender_df = index_aggregation_by_gender(df)
    generation_df = index_aggregation_by_generation(df)
    has_kids_df = index_aggregation_by_has_kids(df)
    urbanicity_df = index_aggregation_by_urbanicity(df)
    household_education_df = index_aggregation_by_household_education(df)
    
    # Add a column to identify the aggregation type
    age_df['Aggregation Type'] = 'Age'
    household_size_df['Aggregation Type'] = 'Household Size'
    household_income_df['Aggregation Type'] = 'Household Income'
    ethnicity_df['Aggregation Type'] = 'Ethnicity'
    gender_df['Aggregation Type'] = 'Gender'
    generation_df['Aggregation Type'] = 'Generation'
    has_kids_df['Aggregation Type'] = 'Has Kids'
    urbanicity_df['Aggregation Type'] = 'Urbanicity'
    household_education_df['Aggregation Type'] = 'Household Education'
    
    # Concatenate all dataframes
    merged_df = pd.concat([
        age_df,
        household_size_df,
        household_income_df,
        ethnicity_df,
        gender_df,
        generation_df,
        has_kids_df,
        urbanicity_df,
        household_education_df
    ], ignore_index=True)
    
    # Reorder columns for better readability
    merged_df = merged_df[[
        'Aggregation Type',
        'Attribute Name',
        'Persona Attribute Proportion',
        'Base Adjusted Population Attribute Proportion',
        'Index'
    ]]
    
    # Convert proportions to whole-number percentages
    merged_df['Persona Attribute Proportion'] = (merged_df['Persona Attribute Proportion'] * 100).round(0).astype(int)
    merged_df['Base Adjusted Population Attribute Proportion'] = (merged_df['Base Adjusted Population Attribute Proportion'] * 100).round(0).astype(int)

    # Round Index to a whole number (no decimals)
    merged_df['Index'] = merged_df['Index'].round(0).astype(int)

    return merged_df


def main():
    df = load_pandas_and_format()
    merged_df = merge_all_index_aggregations(df)
    print(merged_df)

    # Optionally save to CSV
    merged_df.to_csv('merged_index_aggregations.csv', index=False)


if __name__ == "__main__":
    main()
