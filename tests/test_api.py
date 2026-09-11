import pytest
from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"ok": True}


@pytest.mark.integration
def test_chat_analyze():
    """Needs DB + a Production churn model + an LLM key - the agent picks its own
    tool(s), so just check it did something churn-related and answered."""
    r = client.post("/chat", json={"message": "What's our current churn risk situation?"})
    assert r.status_code == 200
    data = r.json()
    assert any("churn" in w or "segmentation" in w for w in data["completed_workers"])
    assert data["messages"][-1]["role"] == "assistant"
