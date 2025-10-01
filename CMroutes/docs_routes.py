from flask import Blueprint, make_response, render_template, send_file
import json

root_bp = Blueprint('root', __name__)

@root_bp.route("/")
def root():
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('api.html'), 200, headers)


@root_bp.route('/docs')
def swagger_ui():
    return make_response(render_template('index.html'))


@root_bp.route('/swagger')
def swagger_yaml():
    return send_file('/app/swagger.yml', mimetype='application/x-yaml')