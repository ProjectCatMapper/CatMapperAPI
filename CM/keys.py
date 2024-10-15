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
