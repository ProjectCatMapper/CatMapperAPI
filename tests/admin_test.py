import pytest
import sys
import os
import re

# Make sure the top-level directory is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import app as flask_app

@pytest.fixture
def client():
    with flask_app.test_client() as client:
        yield client
        
def test_admin_nodeproperties(client):
    response = client.get('/admin_add_edit_delete_nodeproperties', query_string={'CMID': 'AM256471', 'database': 'ArchaMap'})
    assert re.match(r'{"error":"","r":{.*},"r1":\[.*\]}', response.get_data(as_text=True))
    
def test_admin_add_edit_delete_usesproperties(client):
    response = client.get('/admin_add_edit_delete_usesproperties', query_string={'CMID': 'AM256471', 'database': 'ArchaMap'})
    assert re.match(r'{"error":"","r":\[.*\],"r1":\[.*\]}', response.get_data(as_text=True))
    
def test_create_label_helper(client):
    response = client.get('/create_label_helper', query_string={'database': 'ArchaMap'})
    assert "DISTRICT" in response.get_data(as_text=True)