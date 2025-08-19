import pytest
from CM import getNodeProperties

@pytest.fixture
def database():
    return "ArchaMap"
@pytest.fixture
def domain():
    return "CATEGORY"
@pytest.fixture
def CMID():
    return ["AM1001","AM1002","AM1003","AM1004","AM1005","AM1006","AM1007","AM1008","AM1009","AM1010"]

def test_getNodeProperties(database, domain, CMID):
    response = getNodeProperties(database, domain, CMID)
    assert response == ['Name', 'Key', 'label', 'yearPublished']
