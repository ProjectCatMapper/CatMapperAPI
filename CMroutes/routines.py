from CM import *
from flask_mail import Mail
from flask import request

mail = Mail()


def get_routines(routine, database):
    # this route will not be documented in swagger
    # it is intended for automatic routines only
    try:
        fun = routine
        result = "Nothing returned"
        if fun == "addLog":
            result = addLog(database)
        elif fun == "checkDomains":
            data = unlist(request.args.get('data'))
            result = checkDomains(data=data, database=database)
        elif fun == "processUSES":
            CMID = request.args.get('CMID')
            result = processUSES(database=database, CMID=CMID)
        elif fun == "backup2CSV":
            result = backup2CSV(database, mail)
        elif fun == "getBadJSON":
            result = getBadJSON(database, mail)
        elif fun == "getBadCMID":
            result = getBadCMID(database, mail)
        elif fun == "getMultipleLabels":
            result = getMultipleLabels(database, mail)
        elif fun == "getBadDomains":
            result = getBadDomains(database, mail)
        elif fun == "getBadRelations":
            result = getBadRelations(database, mail)
        elif fun == "CMNameNotInName":
            result = CMNameNotInName(database, mail)
        else:
            result = "function not found"
        return result
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500
