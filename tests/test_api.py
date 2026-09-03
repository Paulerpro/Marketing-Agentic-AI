from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


def test_chat_analyze():
    r = client.post("/chat", json={"message": "analyze churn"})
    assert r.status_code == 200
    data = r.json()
    assert "data_analyst" in data["completed_workers"]
    assert data["messages"][-1]["role"] == "assistant"
