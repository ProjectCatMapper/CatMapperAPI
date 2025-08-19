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

def test_home(client):
    response = client.get('/')
    assert response.status_code == 200
    assert b"Welcome" in response.data
