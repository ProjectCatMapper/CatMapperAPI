import inspect
from flask import request, Blueprint
import CM.routines as routines_module
import CM.USES as uses_module
from .extensions import mail

routine_bp = Blueprint('routine', __name__)


def _parse_bool(value, default=True):
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


@routine_bp.route('/routines/<routine>/<database>', methods=['GET'])
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
            'save': _parse_bool(request.args.get('save'), True),
            'property': request.args.get('property') or None,
            'path': request.args.get('path') or None,
            'value': request.args.get('value') or None,
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

@routine_bp.route('/runRoutines/<databases>', methods=['GET'])
def get_runRoutinesStream(databases):
    try:
        return routines_module.runRoutinesStream(databases, mail)
    except Exception as e:
        return str(e), 500
