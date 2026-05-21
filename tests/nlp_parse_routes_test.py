import json
import urllib.error

from CMroutes import search_routes


class FakeOllamaResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self):
        return json.dumps(self.payload).encode("utf-8")


def test_nlp_parse_proxies_prompt_to_qwen(client, monkeypatch):
    captured = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        captured["body"] = json.loads(request.data.decode("utf-8"))
        return FakeOllamaResponse({
            "response": json.dumps({
                "term": "Yoruba",
                "domain": "ETHNICITY",
                "property": "Name",
            }),
            "done": True,
        })

    monkeypatch.setattr(search_routes.urllib.request, "urlopen", fake_urlopen)

    response = client.post(
        "/api/nlp/parse",
        json={
            "prompt": "Convert this search. Input: look up Yoruba in Ghana",
            "model": "qwen3-nl2api:q4km",
            "timeoutSeconds": 7,
        },
    )

    assert response.status_code == 200
    payload = response.get_json()
    assert payload["status"] == "ok"
    assert payload["model"] == "qwen3-nl2api:q4km"
    assert "Yoruba" in payload["response"]
    assert captured["url"] == "http://qwen3:11434/api/generate"
    assert captured["timeout"] == 7
    assert captured["body"]["model"] == "qwen3-nl2api:q4km"
    assert captured["body"]["stream"] is False
    assert "look up Yoruba" in captured["body"]["prompt"]


def test_nlp_parse_reports_qwen_unavailable(client, monkeypatch):
    def fake_urlopen(request, timeout):
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(search_routes.urllib.request, "urlopen", fake_urlopen)

    response = client.post(
        "/api/nlp/parse",
        json={"prompt": "Input: Yoruba"},
    )

    assert response.status_code == 502
    payload = response.get_json()
    assert payload["model"] == "qwen3-nl2api:q4km"
    assert "unavailable" in payload["error"]
