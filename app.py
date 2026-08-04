"""
app.py
The main application file for the CatMapperAPI Flask application.
endpoints are registered here and imported from the CMroutes directory.
Use `grep -R "endpoint" CMroutes/` to find specific endpoints.
e.g., `grep -R "/uploadInputNodes" CMroutes/` to find the /uploadInputNodes endpoint.
Specific functions called by endpoints can be found in the CM directory.
This directory can also be searched using `grep -R "function_name" CM/`.
"""
import atexit
from CM import closeAllDrivers
from CMroutes import *

app = create_app()
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB for api calls

ROUTE_BLUEPRINTS = [
    root_bp,
    merge_bp,
    admin_bp,
    metadata_bp,
    logs_bp,
    explore_bp,
    upload_bp,
    user_bp,
    dev_bp,
    routine_bp,
    download_bp,
    homepage_bp,
    search_bp,
]


def register_routes(flask_app):
    for blueprint in ROUTE_BLUEPRINTS:
        flask_app.register_blueprint(blueprint)
        flask_app.register_blueprint(
            blueprint,
            name=f"{blueprint.name}_api",
            url_prefix="/api",
        )


register_routes(app)

atexit.register(closeAllDrivers) # closes all active web drivers on exit

# run the app from pythong (development mode)
if __name__ == "__main__":
    app.run(debug=True, port=5001)
