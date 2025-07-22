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
            'mail': globals().get('mail'),  # or pass explicitly if preferred
            'CMID': request.args.get('CMID'),
            'datasetID': request.args.get('datasetID'),
            'Key': request.args.get('Key'),
            'properties': request.args.get('properties')
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
