import inspect
from flask import request
from flask_mail import Mail
import CM.routines as routines_module
import CM.USES as uses_module

mail = Mail()

def get_routines(routine, database):
    try:
        # Dynamically get function by name from the routines module
        modules = [
            routines_module,
            uses_module
        ]
        func = None
        for mod in modules:
            if hasattr(mod, routine):
                func = getattr(mod, routine)
                break
        if not callable(func):
            return "function not found"

        # Define available arguments
        available_args = {
            'database': database,
            'databases': database or "all",
            'mail': request.args.get('mail') or globals().get('mail'), 
            'CMID': request.args.get('CMID'),
            'datasetID': request.args.get('datasetID'),
            'Key': request.args.get('Key'),
            'properties': request.args.get('properties') or None,
            'dateStart': request.args.get('dateStart') or None,
            'dateEnd': request.args.get('dateEnd') or None ,
            'user': request.args.get('user') or None,
            'action': request.args.get('action') or "default",
            'return_type': request.args.get('return_type') or "info",
            'save': request.args.get('save') or True
        }

        # Match args to function signature
        sig = inspect.signature(func)
        kwargs = {
            k: v for k, v in available_args.items()
            if k in sig.parameters
        }

        return func(**kwargs)

    except Exception as e:
        return str(e), 500

def get_runRoutinesStream(databases):
    try:
        return routines_module.runRoutinesStream(databases, mail)
    except Exception as e:
        return str(e), 500