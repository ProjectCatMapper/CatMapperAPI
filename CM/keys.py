import pandas as pd

def createKey(nodes, cols):
    """
    Create a new 'Key' column by concatenating specified columns with their values.

    Parameters:
        nodes (pd.DataFrame): DataFrame of new nodes to create a key for.
        cols (str or list of str): Name of column(s) to create key for.

    Returns:
        pd.DataFrame: DataFrame with a new 'Key' column.
    """
    # Ensure 'cols' is a list
    if isinstance(cols, str):
        cols = [cols]
    
    # Validate that all specified columns exist
    missing_cols = [c for c in cols if c not in nodes.columns]
    if missing_cols:
        raise ValueError(f"The following columns are missing in the DataFrame: {missing_cols}")
    
    # Create the 'Key' column by concatenating "{ColumnName}: {Value}" for each specified column
    nodes['Key'] = nodes[cols].astype(str).apply(
        lambda row: '; '.join([f"{col}: {row[col]}" for col in cols]),
        axis=1
    )
    
    return nodes

def extract_key(nodes, col="Key", sep=None):
    """ Extracts and formats key values from the given column """
    if col not in nodes.columns:
        raise ValueError(f"Column {col} not found in DataFrame")

    error_nodes = nodes[nodes[col].isna()]
    if not error_nodes.empty:
        print("Warning: NA present")
    
    if sep:
        nodes = nodes.dropna(subset=[col])
        nodes[col] = nodes[col].astype(str).str.split(sep)
        nodes = nodes.explode(col)
    
    result = nodes.dropna(subset=[col]).copy()
    result["tmp_key"] = result[col]
    
    result[['primary', 'alternate']] = result['tmp_key'].str.split(';', n=1, expand=True).reindex(columns=['primary', 'alternate'])
    result[["KeyName", "KeyVal"]] = result["primary"].astype(str).str.split(":", n=1, expand=True).reindex(columns=["KeyName", "KeyVal"])
    result[["altKeyName", "altKeyVal"]] = result["alternate"].astype(str).str.split(":", n=1, expand=True).reindex(columns=["altKeyName", "altKeyVal"])
    
    result = result.drop(columns=["tmp_key", "primary", "alternate"], errors='ignore')
    result = result.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    
    result = pd.concat([result, error_nodes.astype(str)], ignore_index=True)
    
    if result["altKeyName"].dropna().eq("").all():
        print("Removing empty columns")
        result = result.drop(columns=["altKeyName"], errors='ignore')
    
    if result["altKeyVal"].dropna().eq("").all():
        print("Removing empty columns")
        result = result.drop(columns=["altKeyVal"], errors='ignore')

    return result