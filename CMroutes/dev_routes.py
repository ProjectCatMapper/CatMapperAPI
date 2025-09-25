from CM import *
from flask import render_template, request, Response, stream_with_context
from flask_mail import Mail
from .extensions import mail
import pandas as pd

def testmsg(database, msg):
    return "This is a test message from the " + database + " database that says: " + msg


def send_test_email():
    try:
        msg = sendEmail(mail, "Test Email", [
            "bischrob@gmail.com"], "This is a test email sent from a Flask application. Have fun.", "admin@catmapper.org")
        return msg
    except Exception as e:
        return str(e), 500

# def testStream():
#     return Response(
#         stream_with_context(runRoutinesStream()),
#         mimetype="text/html"
#     )

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

def display_graph():
    return render_template("graph.html")