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
app.register_blueprint(user_bp)  
app.register_blueprint(dev_bp)  
app.register_blueprint(routine_bp)  
app.register_blueprint(download_bp)  
app.register_blueprint(homepage_bp)  
app.register_blueprint(search_bp)  

atexit.register(closeAllDrivers) # closes all active web drivers on exit

# run the app from pythong (development mode)
if __name__ == "__main__":
    app.run(debug=True, port=5001)
