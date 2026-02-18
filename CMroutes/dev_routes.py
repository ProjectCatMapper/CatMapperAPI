from turtle import up
from CM import *
from flask import Blueprint, jsonify, render_template
from .extensions import mail
import pandas as pd

dev_bp = Blueprint('dev', __name__)

@dev_bp.route('/testmsg/<database>/<msg>', methods=['GET'])
def testmsg(database, msg):
    return "This is a test message from the " + database + " database that says: " + msg

@dev_bp.route('/send_test_email/<email>', methods=['GET'])
def send_test_email(email):
    try:
        msg = sendEmail(mail, "Test Email", [
            email], "This is a test email sent from a Flask application. Have fun.", "admin@catmapper.org")
        return msg
    except Exception as e:
        return str(e), 500
    
@dev_bp.route('/admin/graph', methods=['GET'])
def get_graph():
    
    data = pd.read_excel("tmp/graph_data.xlsx").to_dict(orient="records")
    # collect all unique nodes
    unique_nodes = sorted(set([d["Caller"] for d in data] + [d["Callee"] for d in data]))

    # assign integer IDs
    node_map = {name: idx for idx, name in enumerate(unique_nodes)}

    # build nodes with integer id
    nodes = [{"id": idx, "label": name} for name, idx in node_map.items()]

    # build edges using the integer ids
    edges = [{"from": node_map[d["Caller"]], "to": node_map[d["Callee"]]} for d in data]

    graph = {"nodes": nodes, "edges": edges}
    return graph

@dev_bp.route('/admin/view_graph', methods=['GET'])
def display_graph():
    return render_template("graph.html")


@dev_bp.route('/health')
def health():
    import os
    APP_VERSION = os.getenv('VERSION', 'dev-build')
    return jsonify({
        "status": "healthy",
        "version": APP_VERSION
    })
