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
        
def test_get_separate_rows(client):
    # Test case 1: Valid input
    response = client.post('/separate_rows', json={
        'table': [{'A': 'a ;b; c', 'B': 1}, {'A': 'd e f', 'B': 2}],
        'column': 'A',
        'separator': ';'
    })
    data = response.get_json()
    assert data == [{"A":"a","B":1},{"A":"b","B":1},{"A":"c","B":1},{"A":"d e f","B":2}]