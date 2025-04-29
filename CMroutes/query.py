from flask import request, jsonify
import json
from CM import getDriver, getQuery, login, unlist


def getRouteQuery(database):
    try:
        rows = request.get_data()
        rows = json.loads(rows)
        query = rows.get("query")
        user = unlist(rows.get("user"))
        pwd = unlist(rows.get("pwd"))
        params = rows.get("params")

        driver = getDriver(database)

        credentials = login(database, user, pwd)

        if credentials.get('role') == "admin":
            data = getQuery(query, driver, params)
            return jsonify(data)
        else:
            raise Exception(f"error: User is not verified")

    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)

        return data, 500
