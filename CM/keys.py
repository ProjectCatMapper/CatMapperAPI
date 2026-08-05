import pandas as pd

KEY_FORMAT_GUIDANCE = (
    'Use spaces around delimiters: variable == value. '
    'For multiple pairs, join pairs with " && ", for example: '
    'variable == value && other_variable == other_value.'
)


def _reserved_token_message(token, part):
    return (
        f'Does the original {part} contain "{token}"? '
        f'Please confirm it was intentional. {KEY_FORMAT_GUIDANCE}'
    )


def _missing_spaces_message(token, part=None):
    location = f" in the {part}" if part else ""
    return f'Found "{token}" without spaces around it{location}. {KEY_FORMAT_GUIDANCE}'


def key_format_issue(value):
    """Return a user-facing validation issue, or None when value is a valid Key."""
    if not isinstance(value, str):
        return f"Key must be a text value. {KEY_FORMAT_GUIDANCE}"

    if value.rstrip().endswith(" &&"):
        return f'Key contains an empty pair. Remove extra "&&" delimiters. {KEY_FORMAT_GUIDANCE}'

    text = value.strip()
    if not text:
        return f"Key must not be empty. {KEY_FORMAT_GUIDANCE}"
    segments = text.split(" && ")
    if any(not segment.strip() for segment in segments):
        return f'Key contains an empty pair. Remove extra "&&" delimiters. {KEY_FORMAT_GUIDANCE}'

    for segment in segments:
        if " == " not in segment:
            if " == " in value and segment.startswith("=="):
                return f'Key variable name must not be empty before " == ". {KEY_FORMAT_GUIDANCE}'
            if " == " in value and segment.endswith("=="):
                return f'Key value must not be empty after " == ". {KEY_FORMAT_GUIDANCE}'
            if "==" in segment:
                return _missing_spaces_message("==")
            return f'Each Key pair must include " == " between variable and value. {KEY_FORMAT_GUIDANCE}'

        key_name, key_value = segment.split(" == ", 1)
        key_name = key_name.strip()
        key_value = key_value.strip()

        if not key_name:
            return f'Key variable name must not be empty before " == ". {KEY_FORMAT_GUIDANCE}'
        if not key_value:
            return f'Key value must not be empty after " == ". {KEY_FORMAT_GUIDANCE}'

    return None


def key_format_warnings(value):
    """Return user-facing warnings for valid Keys with reserved tokens in parts."""
    if key_format_issue(value):
        return []

    warnings = []
    for segment in value.strip().split(" && "):
        key_name, key_value = segment.split(" == ", 1)
        key_name = key_name.strip()
        key_value = key_value.strip()

        if "==" in key_name:
            warnings.append(_reserved_token_message("==", "variable name"))
        if "==" in key_value:
            warnings.append(_reserved_token_message("==", "variable value"))
        if "&&" in key_name:
            warnings.append(_reserved_token_message("&&", "variable name"))
        if "&&" in key_value:
            warnings.append(_reserved_token_message("&&", "variable value"))

    return warnings


def is_valid_key_format(value):
    """Return True when value follows CatMapper's Key segment format."""
    return key_format_issue(value) is None


def invalid_key_row_details(values):
    """Return 1-based row numbers and messages for values that are not valid Keys."""
    series = pd.Series(values)
    details = []
    for position, value in enumerate(series.tolist()):
        issue = key_format_issue(value)
        if issue:
            details.append({"row": position + 1, "message": issue})
    return details


def invalid_key_row_numbers(values):
    """Return 1-based row numbers for values that are not valid Keys."""
    return [detail["row"] for detail in invalid_key_row_details(values)]


def invalid_key_format_error(values, column="Key"):
    """Return an upload-ready error message for invalid Keys, or None."""
    details = invalid_key_row_details(values)
    if not details:
        return None

    rows = [detail["row"] for detail in details]
    row_messages = "\n".join(
        f"Row {detail['row']}: {detail['message']}" for detail in details
    )
    return f"Invalid '{column}' format in rows:\n{rows}. {KEY_FORMAT_GUIDANCE}\n{row_messages}"


def key_format_warning_messages(values, column="Key"):
    """Return upload-ready warning messages for valid Keys with reserved tokens."""
    series = pd.Series(values)
    messages = []
    seen = set()
    for position, value in enumerate(series.tolist()):
        for warning in key_format_warnings(value):
            message = f"{column} row {position + 1}: {warning}"
            if message not in seen:
                seen.add(message)
                messages.append(message)
    return messages


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
        lambda row: ' && '.join([f"{col} == {row[col]}" for col in cols]),
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
    tmp[col] = tmp[col].str.split(" && ")
    tmp = tmp.explode(col)
    tmp[col] = tmp[col].str.strip()
    tmp[['KeyName', 'KeyVal']] = tmp[col].str.split(' == ', n=1, expand=True)
    tmp['KeyName'] = tmp['KeyName'].str.strip()
    tmp['KeyVal'] = tmp['KeyVal'].str.strip()
    tmp.drop(columns=[col], inplace=True)
    tmp = tmp.pivot(index='index', columns='KeyName', values='KeyVal')

    result = pd.merge(nodes, tmp, how='left', on="index")
    result.drop(columns=["index"], inplace=True)
    result = pd.concat([result, error_nodes], ignore_index=True)
    return result
