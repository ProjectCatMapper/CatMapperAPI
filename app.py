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

# routes
app.register_blueprint(root_bp)  
app.register_blueprint(merge_bp)  
app.register_blueprint(admin_bp)  
app.register_blueprint(metadata_bp)  
app.register_blueprint(logs_bp)  
app.register_blueprint(explore_bp)  
app.register_blueprint(upload_bp)  
app.register_blueprint(geojson_upload_bp)
app.register_blueprint(user_bp)  
app.register_blueprint(dev_bp)  
app.register_blueprint(routine_bp)  
app.register_blueprint(download_bp)  
app.register_blueprint(homepage_bp)  
app.register_blueprint(search_bp)  
app.register_blueprint(reconciliation_bp)  

def _register_api_legacy_aliases(flask_app):
    """Expose existing routes under /api while canonical REST paths are adopted."""
    existing_rules = {rule.rule for rule in flask_app.url_map.iter_rules()}
    for rule in list(flask_app.url_map.iter_rules()):
        if rule.endpoint == "static" or rule.rule.startswith("/api"):
            continue

        alias_rule = "/api" if rule.rule == "/" else f"/api{rule.rule}"
        if alias_rule in existing_rules:
            continue

        view_func = flask_app.view_functions.get(rule.endpoint)
        if view_func is None:
            continue

        methods = sorted(rule.methods - {"HEAD", "OPTIONS"})
        endpoint = f"api_legacy__{rule.endpoint.replace('.', '__')}"
        flask_app.add_url_rule(
            alias_rule,
            endpoint=endpoint,
            view_func=view_func,
            methods=methods,
            defaults=rule.defaults,
            strict_slashes=rule.strict_slashes,
        )
        existing_rules.add(alias_rule)


_register_api_legacy_aliases(app)

atexit.register(closeAllDrivers) # closes all active web drivers on exit

# run the app from pythong (development mode)
if __name__ == "__main__":
    app.run(debug=True, port=5001)
