from CM import *
import pandas as pd
import ast
def parse_list(val):
    try:
        parsed = ast.literal_eval(val)
        if isinstance(parsed, list):
            return parsed
        return [parsed]
    except:
        return [val] 
    
fn = "../db/archamap1/backups/download/USESties_2025-06-10.csv"
df = pd.read_csv(fn)
len(df)
df_filtered = df[~df['property'].isin(['log', 'logID','geoPolygon','names'])].copy()
df_filtered['value'] = df_filtered['value'].apply(parse_list)
df_filtered.fillna('', inplace=True)
for col in df_filtered.columns:
    if df_filtered[col].apply(lambda x: isinstance(x, list)).any():
        df_filtered[col] = df_filtered[col].apply(lambda x: '; '.join(map(str, x)) if isinstance(x, list) else x)
index_cols = [col for col in df_filtered.columns if col not in ['property', 'value']]
df_wide = df_filtered.pivot_table(
    index=index_cols,
    columns='property',
    values='value',
    aggfunc=lambda x: ', '.join(x.astype(str))  # handle duplicates if any
).reset_index()
df_wide.columns.name = None
# convert NaN values to empty strings
df_wide.columns
df_wide.to_csv(fn, index=False)