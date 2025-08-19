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

def test_download(client):
    response = client.get('/routines/backup2CSV/ArchaMap?mail=None')
    assert response == b"backup2CSV completed" 