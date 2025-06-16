import os
from flask import request, jsonify
import json


def get_merge_syntax_route(database):
    try:
        from CM.merge import createSyntax
        data = request.get_data()
        data = json.loads(data)
        template = data.get("template")
        result = createSyntax(template=template, database=database)

        if result.get("hash") != "":
            return {"msg": "Syntax created successfully", "download": result}, 200
        else:
            return {"msg": "Syntax creation failed"}, 500
    except Exception as e:
        result = str(e)
        return result, 500


def get_merge_template(database, datasetID):
    try:
        from CM.merge import getMergingTemplate
        template = getMergingTemplate(datasetID, database)
        return template
    except Exception as e:
        result = str(e)
        return result, 500

def get_moveUSESValidate(database,relid):
    try:
        from CM.admin import moveUSESValidate
        result = moveUSESValidate(relid, database)
        return result, 200
    except Exception as e:
        result = str(e)
        return result, 500