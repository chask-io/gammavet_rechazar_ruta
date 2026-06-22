import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from backend.function_logic import ACTOR_LAMBDA, REJECTION_ACK, TENANT_DRIVER_UPDATE_PATH, FunctionBackend  # noqa: E402
from chask_foundation.backend.models import OrchestrationEvent  # noqa: E402


EVENT_ID = "11111111-2222-4333-8444-555555555555"
SESSION_ID = "66666666-2222-4333-8444-555555555555"


def _event(args=None, extra_params=None):
    params = {
        "user_phone_number": "+56 9 1111 2222",
        "agent_phone_number": "1051240901403291",
        "tool_calls": [{"args": args or {"nota": "no puedo hoy"}}],
    }
    if extra_params:
        params.update(extra_params)
    return OrchestrationEvent.model_validate(
        {
            "event_id": EVENT_ID,
            "event_type": "function_call",
            "branch": "test",
            "organization_customer_id": None,
            "customer": None,
            "connection_key": "test",
            "organization": {
                "organization_id": "99999999-aaaa-4bbb-8ccc-dddddddddddd",
                "organization_name": "Chask Dev",
            },
            "prompt": "",
            "pipeline_id": 27023,
            "orchestration_session_uuid": SESSION_ID,
            "internal_orchestration_session_uuid": None,
            "channel_id": None,
            "entry_point_channel": "whatsapp",
            "source": "agent",
            "target": "function",
            "plan": None,
            "extra_params": params,
            "access_token": "access-token",
            "target_agent": None,
            "target_operator": None,
            "type": None,
            "status": None,
            "channels": None,
            "whatsapp_template_instance": None,
            "created_at": None,
        }
    )


class FakeTenantClient:
    def __init__(self):
        self.calls = []

    def post(self, path, *, json=None):
        self.calls.append({"path": path, "json": json})
        return {"driver": {"id": "driver-1", "availability": "inactivo", "paused": True}}


class FakeOrchestrator:
    def __init__(self):
        self.calls = []

    def call(self, endpoint, **kwargs):
        self.calls.append({"endpoint": endpoint, **kwargs})
        if endpoint == "evolve_event":
            return {
                "status_code": 201,
                "uuid": "22222222-2222-4222-8222-222222222222",
                "extra_params": kwargs["extra_params"],
            }
        return {"status_code": 200}


def test_rechazar_ruta_marks_unavailable_sends_ack_and_completes_session(monkeypatch):
    tenant = FakeTenantClient()
    orchestrator = FakeOrchestrator()
    backend = FunctionBackend(_event())
    monkeypatch.setattr(backend, "_tenant_client", lambda: tenant)
    monkeypatch.setattr("backend.function_logic.orchestrator_api_manager", orchestrator)

    result = backend.process_request()

    assert tenant.calls[0]["path"] == TENANT_DRIVER_UPDATE_PATH
    payload = tenant.calls[0]["json"]
    assert payload["actor_lambda"] == ACTOR_LAMBDA
    assert payload["availability"] == "inactivo"
    assert payload["active"] is False
    assert payload["paused"] is True
    assert payload["note"] == "no puedo hoy"
    assert "action" not in payload
    assert "completada" in result

    assert any(
        call["endpoint"] == "evolve_event"
        and call.get("event_type") == "response_to_whatsapp_message"
        and call["prompt"] == REJECTION_ACK
        for call in orchestrator.calls
    )
    assert any(
        call["endpoint"] == "change_orchestration_session_status"
        and call["orchestration_session_uuid"] == SESSION_ID
        and call["status"] == "completed"
        for call in orchestrator.calls
    )
    assert any(
        call["endpoint"] == "evolve_event"
        and call.get("event_type") == "dispatch_event"
        and call["extra_params"]["event_type"] == "conductor_session_completed"
        for call in orchestrator.calls
    )


def test_publish_test_mock_does_not_mutate_send_or_close(monkeypatch):
    tenant = FakeTenantClient()
    orchestrator = FakeOrchestrator()
    backend = FunctionBackend(_event(extra_params={"chask_publish_test_mock": True}))
    monkeypatch.setattr(backend, "_tenant_client", lambda: tenant)
    monkeypatch.setattr("backend.function_logic.orchestrator_api_manager", orchestrator)

    result = backend.process_request()

    assert "modo prueba" in result
    assert tenant.calls == []
    assert orchestrator.calls == []
