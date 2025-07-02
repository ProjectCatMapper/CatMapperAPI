import ast
import pandas as pd
import os

def pivot_property_value_columns(filepath):
    """
    Loads a CSV, expands list-like strings in the 'value' column,
    filters out unwanted 'property' values, pivots the table wider,
    and overwrites the original file.

    Parameters:
        filepath (str): Path to the input CSV file.
    """

    # Check if file exists
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")

    # Load CSV
    df = pd.read_csv(filepath)

    # Check required columns
    required_cols = {'property', 'value'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"Missing required columns: {required_cols - set(df.columns)}")

    # Function to safely parse list-like strings
    def parse_list(val):
        try:
            parsed = ast.literal_eval(val)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except:
            return [val]

    # Filter out unwanted properties
    ignore_props = {'log', 'logID', 'geoPolygon', 'names'}
    df_filtered = df[~df['property'].isin(ignore_props)].copy()

    # Convert stringified lists to actual lists
    df_filtered['value'] = df_filtered['value'].apply(parse_list)

    # Fill NaNs to prevent explode/join issues
    df_filtered.fillna('', inplace=True)

    # Convert list columns to semicolon-separated strings
    for col in df_filtered.columns:
        if df_filtered[col].apply(lambda x: isinstance(x, list)).any():
            df_filtered[col] = df_filtered[col].apply(
                lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x
            )

    # Identify index columns (all except 'property' and 'value')
    index_cols = [col for col in df_filtered.columns if col not in ['property', 'value']]

    # Pivot to wide format
    df_wide = df_filtered.pivot_table(
        index=index_cols,
        columns='property',
        values='value',
        aggfunc=lambda x: ', '.join(x.astype(str))  # join duplicates
    ).reset_index()

    df_wide.columns.name = None  # Remove column name from pivot
    df_wide.fillna('', inplace=True)  # Replace NaNs with empty strings

    # Save the resulting DataFrame
    df_wide.to_csv(filepath, index=False)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pivot CSV based on 'property' and 'value' columns.")
    parser.add_argument("filepath", help="Path to the CSV file to process.")
    args = parser.parse_args()

    pivot_property_value_columns(args.filepath)
    print(f"Processed and saved: {args.filepath}")