"""First layer of contact between the frontend and backend, some functions diverge from here to other files but the rest are computed here."""

import atexit
from CM import closeAllDrivers
from CMroutes import *

app = create_app()
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50 MB

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

atexit.register(closeAllDrivers)

if __name__ == "__main__":
    app.run(debug=True, port=5001)
