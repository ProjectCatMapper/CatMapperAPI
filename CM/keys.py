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
        raise ValueError(
            f"The following columns are missing in the DataFrame: {missing_cols}")

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

    if 'index' in nodes.columns:
        # drop index column
        nodes = nodes.drop(columns=['index'])
        print("Warning: 'index' column dropped")

    error_nodes = nodes[nodes[col].isna()]
    if not error_nodes.empty:
        print("Warning: NA present")

    if sep:
        nodes = nodes.dropna(subset=[col])
        nodes[col] = nodes[col].astype(str).str.split(sep)
        nodes = nodes.explode(col)

    nodes = nodes.reset_index()

    tmp = nodes[["index", col]].dropna(subset=[col]).copy()
    tmp[col] = tmp[col].str.split(";")
    tmp = tmp.explode(col)
    tmp[col] = tmp[col].str.strip()
    tmp[['KeyName', 'KeyVal']] = tmp[col].str.split(': ', n=1, expand=True)
    tmp['KeyName'] = tmp['KeyName'].str.strip()
    tmp['KeyVal'] = tmp['KeyVal'].str.strip()
    tmp.drop(columns=[col], inplace=True)
    tmp = tmp.pivot(index='index', columns='KeyName', values='KeyVal')

    result = pd.merge(nodes, tmp, how='left', on="index")
    result.drop(columns=["index"], inplace=True)
    result = pd.concat([result, error_nodes], ignore_index=True)
    return result
