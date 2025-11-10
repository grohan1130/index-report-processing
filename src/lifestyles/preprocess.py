import pandas as pd
import os


def load_pandas_and_format():
    # Get path relative to this script's location
    script_dir = os.path.dirname(__file__)
    file_path = os.path.join(script_dir, '..', '..', 'raw_input_files', 'raw_index_report.xlsx')
    raw_pandas_df = pd.read_excel(file_path, sheet_name="Index Report", header=1)
    return raw_pandas_df


def load_attribute_category_map(
    mapping_file_path: str | None = None,
    sheet_name: str = "Lifestyles",
    attribute_col: str | None = None,
    category_col: str = "Category"
) -> pd.DataFrame:
    """
    Load a mapping of attribute names to categories from an Excel file.

    Parameters
    ----------
    mapping_file_path : str | None, optional
        Path to the mapping Excel file. If None, defaults to
        'raw_input_files/raw_index_categories.xlsx' relative to this script.
    sheet_name : str, default "Index Report"
        Name of the sheet to read from the Excel file.
    attribute_col : str | None, optional
        Name of the attribute column. If None, auto-detects the first column
        whose lowercase name contains both "attribute" and "name".
    category_col : str, default "Category"
        Name of the category column.

    Returns
    -------
    pd.DataFrame
        A DataFrame with two columns: the attribute column and the category column,
        with whitespace stripped, duplicates removed, and missing values dropped.

    Raises
    ------
    ValueError
        If the attribute column cannot be auto-detected or if the category column
        does not exist in the mapping file.
    """
    if mapping_file_path is None:
        script_dir = os.path.dirname(__file__)
        mapping_file_path = os.path.join(script_dir, '..', '..', 'raw_input_files', 'master_mapping_file.xlsx')

    mapping_df = pd.read_excel(mapping_file_path, sheet_name=sheet_name, header=0)

    if attribute_col is None:
        attribute_col_candidates = [
            col for col in mapping_df.columns
            if isinstance(col, str) and "attribute" in col.lower() and "name" in col.lower()
        ]
        if not attribute_col_candidates:
            raise ValueError(
                "Could not auto-detect attribute column. No column name contains both 'attribute' and 'name'. "
                f"Available columns: {list(mapping_df.columns)}"
            )
        attribute_col = attribute_col_candidates[0]

    if category_col not in mapping_df.columns:
        raise ValueError(
            f"Category column '{category_col}' not found in mapping file. "
            f"Available columns: {list(mapping_df.columns)}"
        )

    mapping_df = mapping_df[[attribute_col, category_col]].copy()

    mapping_df[attribute_col] = mapping_df[attribute_col].astype(str).str.strip()
    mapping_df[category_col] = mapping_df[category_col].astype(str).str.strip()

    mapping_df = mapping_df.dropna(subset=[attribute_col, category_col])

    mapping_df = mapping_df.drop_duplicates(subset=[attribute_col, category_col])

    return mapping_df


def attach_categories_to_index(
    df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    attribute_col: str | None = None,
    category_col: str = "Category"
) -> pd.DataFrame:
    """
    Attach categories to an index DataFrame via inner merge on attribute column.

    Parameters
    ----------
    df : pd.DataFrame
        The main DataFrame containing an attribute column.
    mapping_df : pd.DataFrame
        The mapping DataFrame with attribute and category columns.
    attribute_col : str | None, optional
        Name of the attribute column in df. If None, auto-detects the first column
        whose lowercase name contains both "attribute" and "name".
    category_col : str, default "Category"
        Name of the category column to add to df.

    Returns
    -------
    pd.DataFrame
        A new DataFrame with only the rows from df that have a matching category
        in the mapping file. Only includes rows with valid (non-NaN) Category values.

    Raises
    ------
    ValueError
        If the attribute column cannot be auto-detected in df.
    """
    if attribute_col is None:
        attribute_col_candidates = [
            col for col in df.columns
            if isinstance(col, str) and "attribute" in col.lower() and "name" in col.lower()
        ]
        if not attribute_col_candidates:
            raise ValueError(
                "Could not auto-detect attribute column in df. No column name contains both 'attribute' and 'name'. "
                f"Available columns: {list(df.columns)}"
            )
        attribute_col = attribute_col_candidates[0]

    # Drop existing Category or Categories column if it exists to avoid conflicts
    df_clean = df.copy()
    if category_col in df_clean.columns:
        df_clean = df_clean.drop(columns=[category_col])
    if 'Categories' in df_clean.columns:
        df_clean = df_clean.drop(columns=['Categories'])

    mapping_attribute_col = [
        col for col in mapping_df.columns
        if col != category_col
    ][0]

    # Use inner merge to only keep rows with matching categories
    enriched_df = df_clean.merge(
        mapping_df,
        left_on=attribute_col,
        right_on=mapping_attribute_col,
        how="inner"
    )

    if mapping_attribute_col != attribute_col and mapping_attribute_col in enriched_df.columns:
        enriched_df = enriched_df.drop(columns=[mapping_attribute_col])

    # Filter out any rows with NaN in the Category column (extra safety)
    enriched_df = enriched_df.dropna(subset=[category_col])

    return enriched_df


def calculate_index_per_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate index per row using the formula:
    Index = (Audience Attribute Proportion / Base Adjusted Population Attribute Proportion) x 100

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing 'Audience Attribute Proportion' and
        'Base Adjusted Population Attribute Proportion' columns.

    Returns
    -------
    pd.DataFrame
        The input DataFrame with an additional 'Index' column.
    """
    df = df.copy()
    df['Calculated Index'] = (df['Audience Attribute Proportion'] / df['Base Adjusted Population Attribute Proportion']) * 100
    return df


def main():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)
    pd.set_option('display.width', None)

    df = load_pandas_and_format()
    print(f"Original df shape: {df.shape}")
    print(f"Original df columns: {df.columns.tolist()}")

    mapping_df = load_attribute_category_map(None, "Lifestyles", "Attribute Name", "Category")
    print(f"\nMapping df shape: {mapping_df.shape}")

    df_with_cat = attach_categories_to_index(df, mapping_df, "Attribute Name", "Category")
    print(f"\nFiltered df shape: {df_with_cat.shape}")
    print(f"Filtered df columns: {df_with_cat.columns.tolist()}")
    print(f"\nSample rows:")
    print(df_with_cat[['Attribute Name', 'Category']].head(10))

    # Verify no NaN categories
    nan_count = df_with_cat['Category'].isna().sum()
    print(f"\nNumber of rows with NaN Category: {nan_count}")

    # Calculate index per row
    #df_with_index = calculate_index_per_row(df_with_cat)
    print(f"\n{'='*80}")
    #print("INDEX CALCULATION RESULTS")
    #print(f"{'='*80}")
    index_cols = ['Attribute Name', 'Category', 'Audience Attribute Proportion',
                  'Base Adjusted Population Attribute Proportion', 'Index']
    print(df_with_cat[index_cols].head(10).to_string(index=False))


if __name__ == "__main__":
   main() 
