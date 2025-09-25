import pandas as pd
from flask import request, Blueprint
import json
from CM import translate, unlist

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
    
@translate_bp.route('/translate2', methods=['POST'])
def getTranslate2():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get("database"))
        property = unlist(data.get("property"))
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = unlist(data.get("domain"))
        key = unlist(data.get("key"))
        term = unlist(data.get("term"))
        country = unlist(data.get('country'))
        context = unlist(data.get('context'))
        dataset = unlist(data.get('dataset'))
        yearStart = unlist(data.get('yearStart'))
        yearEnd = unlist(data.get('yearEnd'))
        query = unlist(data.get("query"))
        table = data.get("table")
        countsamename = data.get("uniqueRows")

        data, desired_order = translate(
            database=database,
            property=property,
            domain=domain,
            key=key,
            term=term,
            country=country,
            context=context,
            dataset=dataset,
            yearStart=yearStart,
            yearEnd=yearEnd,
            query=query,
            table=table,
            countsamename=countsamename)

        data_dict = data.to_dict(orient='records')

        print(data_dict)

        return jsonify({"file": data_dict, "order": desired_order})

    except Exception as e:
        return str(e), 500
