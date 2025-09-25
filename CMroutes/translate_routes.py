import pandas as pd
from flask import request, Blueprint
import json

translate_bp = Blueprint('translate', __name__)

@translate_bp.route('/split_column', methods=['POST'])
def get_split_column():
    data = request.get_data()
    data = json.loads(data)
    table = data.get('table')
    column = data.get('column')
    separator = data.get('separator', ' ')
    if table is None or column is None:
        return "table and column are required", 400
    try:
        df = pd.DataFrame(table)
        if column not in df.columns:
            return f"Column '{column}' not found in table", 400
        new_cols = df[column].str.split(separator, expand=True)
        new_col_names = [f"{column}_{i+1}" for i in range(new_cols.shape[1])]
        new_cols.columns = new_col_names
        df = pd.concat([df.drop(columns=[column]), new_cols], axis=1)
        return df.to_dict(orient='records')
    except Exception as e:
        return str(e), 500
    