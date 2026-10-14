"""Exercise new-node upload preprocessing before any creation takes place."""
import pytest
import CM.upload as upload


@pytest.mark.parametrize('cmname,name', [
    ('East Harper District', 'Nyomowe; Nyamawe; Nyimewe'),
    ('Washington District', 'Washington, D.C.; District of Columbia'),
    ('Kangalom; Kelea', 'Kelea'),
    ('York', 'New York'),
    ('Bessekeh clan', ''),
])
@pytest.mark.parametrize('dataset_id', ['SD11', 'SD2269'])
def test_new_node_keeps_source_names_without_concatenating_cmname(monkeypatch, cmname, name, dataset_id):
    monkeypatch.setattr(upload, 'updateLog', lambda *args, **kwargs: None)
    monkeypatch.setattr(upload, 'getDriver', lambda _: object())
    monkeypatch.setattr(upload, 'getQuery', lambda *args, **kwargs: [])
    captured = []
    def stop_before_creation(frame, columns):
        captured.append(frame.iloc[0]['Name'])
        raise RuntimeError('preprocessing captured')
    monkeypatch.setattr(upload, 'createKey', stop_before_creation)
    with pytest.raises(RuntimeError, match='preprocessing captured'):
        upload.input_Nodes_Uses([{
            'CMName': cmname, 'Name': name, 'datasetID': dataset_id,
            'Key': 'raw key', 'label': 'AREA',
        }], 'SocioMap', 'add_node', formatKey=True, user='tester')
    assert captured == [name]
