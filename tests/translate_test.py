import pytest
import sys
import os

# Make sure the top-level directory is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import app as flask_app

@pytest.fixture
def client():
    with flask_app.test_client() as client:
        yield client
        
def test_translate_endpoint(client):
    # Example payload for the /translate endpoint
    
    example_table = [{"period": "Archaic", "country":"AM22269"}, {"period": "Classical"}, {"period": "Hellenistic"}, {"period": "Roman"},{"period": "Archaic"}]
    
    payload = {
        "database": "ArchaMap",
        "property": "Name",
        "domain": "PERIOD",
        "key": "false",
        "term": "period",
        "country": "",
        "context": "",
        "dataset": "",
        "yearStart": None,
        "yearEnd": None,
        "query": "false",
        "table": example_table,
        "uniqueRows": "true"
    }
    
    response = client.post('/translate', json=payload)
    
    assert response.status_code == 200
    data = response.get_json()
    assert 'file' in data
    assert 'order' in data