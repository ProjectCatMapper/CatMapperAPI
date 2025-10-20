import pandas as pd
from flask import request, Blueprint, jsonify
import json
from CM import translate, unlist

translate_bp = Blueprint('translate', __name__)
    
@translate_bp.route('/translate', methods=['POST'])
def getTranslate():
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
            countsamename=countsamename,
            uniqueRows=True)

        data_dict = data.to_dict(orient='records')

        print(data_dict)

        return jsonify({"file": data_dict, "order": desired_order})

    except Exception as e:
        return str(e), 500
