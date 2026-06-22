"""
Business logic for RechazarRutaFn.

Org-specific Gammavet lambda: mark the driver unavailable/rejected, acknowledge
the rejection over WhatsApp, and complete the conductor orchestration session.
"""

import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID

from chask_foundation.api.tenant_data_requests import TenantDataClient
from chask_foundation.backend.models import OrchestrationEvent

try:
    from api.orchestrator_requests import orchestrator_api_manager
except ModuleNotFoundError:
    from chask_foundation.api.orchestrator_requests import orchestrator_api_manager

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ACTOR_LAMBDA = "gammavet_rechazar_ruta"
BOT_PHONE_ID = "1051240901403291"
DEFAULT_TENANT_BRANCH = "test"
DEFAULT_TENANT_SLUG = "chask"
TENANT_DRIVER_UPDATE_PATH = "gammavet/drivers/update"
REJECTION_ACK = "Ok, registramos que no puedes operar esta ruta. El equipo revisara la asignacion."


def _normalizar_telefono(telefono: str) -> str:
    return "".join(c for c in str(telefono) if c.isdigit())


class FunctionBackend:
    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        logger.info(
            "RechazarRutaFn initialized for org=%s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        payload = self._build_driver_payload()
        payload["availability"] = "inactivo"
        payload["active"] = False
        payload["paused"] = True
        note = str(self._extract_tool_args().get("nota") or "").strip()
        payload["note"] = note or "rechazo_conductor"

        with _tenant_data_public_test_mode():
            result = self._tenant_client().post(TENANT_DRIVER_UPDATE_PATH, json=payload)
        if not isinstance(result, dict):
            raise RuntimeError("/api/gammavet/drivers/update returned an invalid response")

        self._send_whatsapp(REJECTION_ACK)
        self._complete_session("Conductor rechazo o no puede operar la ruta.")
        driver = result.get("driver") if isinstance(result.get("driver"), dict) else {}
        driver_ref = driver.get("id") or payload.get("driver_id") or payload.get("driver_phone")
        return f"Conductor {driver_ref} marcado como no disponible. Sesion completada."

    def _tenant_client(self) -> TenantDataClient:
        branch = (
            os.environ.get("TENANT_BRANCH")
            or os.environ.get("CHASK_TENANT_BRANCH")
            or DEFAULT_TENANT_BRANCH
        )
        client = TenantDataClient(
            org_uuid=self.orchestration_event.organization.organization_id,
            branch=branch,
            lambda_uuid=self._function_uuid(),
            access_token=getattr(self.orchestration_event, "access_token", None),
        )
        client._slug = os.environ.get("TENANT_SLUG") or DEFAULT_TENANT_SLUG
        return client

    def _function_uuid(self) -> str:
        return os.getenv("FUNCTION_UUID") or os.getenv("CHASK_FUNCTION_UUID") or ""

    def _build_driver_payload(self) -> dict[str, Any]:
        event_uuid = self._event_uuid()
        args = self._extract_tool_args()
        payload: dict[str, Any] = {
            "orchestration_event_uuid": str(event_uuid),
            "source_event_uuid": str(event_uuid),
            "actor_lambda": ACTOR_LAMBDA,
        }

        driver_id = self._first_value(args, "driver_id", "conductor_id")
        driver_phone = (
            self._first_value(args, "driver_phone", "telefono_conductor", "telefono", "phone")
            or self._event_phone()
            or self._session_phone()
        )
        if driver_id:
            payload["driver_id"] = str(driver_id).strip()
        if driver_phone:
            payload["driver_phone"] = str(driver_phone).strip()

        ticket_id = self._first_value(args, "ticket_id") or getattr(
            self.orchestration_event, "orchestration_session_uuid", None
        )
        if ticket_id:
            payload["ticket_id"] = str(ticket_id)

        if "driver_id" not in payload and "driver_phone" not in payload:
            raise ValueError("No se encontro driver_id ni driver_phone para rechazar la ruta")
        return payload

    def _complete_session(self, summary: str) -> None:
        session_uuid = self.orchestration_event.orchestration_session_uuid
        if not session_uuid:
            logger.warning("Skipping session close: missing orchestration_session_uuid")
            return
        response = orchestrator_api_manager.call(
            "change_orchestration_session_status",
            orchestration_session_uuid=str(session_uuid),
            status="completed",
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )
        if response.get("status_code") not in (200, 201, None):
            raise RuntimeError(f"Failed to complete orchestration session: {response}")
        self._emit_dispatch_event(
            "conductor_session_completed",
            {"summary": summary, "session_uuid": str(session_uuid)},
        )

    def _emit_dispatch_event(self, event_type: str, metadata: dict[str, Any]) -> None:
        orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(self.orchestration_event.event_id),
            event_type="dispatch_event",
            source="agent",
            target="orchestrator",
            prompt=event_type,
            extra_params={
                "event_type": event_type,
                "actor_lambda": ACTOR_LAMBDA,
                "metadata": metadata,
            },
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )

    def _event_uuid(self) -> UUID:
        raw_event_id = getattr(self.orchestration_event, "event_id", None)
        try:
            return UUID(str(raw_event_id))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid orchestration_event.event_id: {raw_event_id!r}") from exc

    def _event_phone(self) -> str:
        customer = getattr(self.orchestration_event, "customer", None)
        if customer and getattr(customer, "phone", None):
            return str(customer.phone).strip()

        extra_params = self.orchestration_event.extra_params or {}
        value = str(
            self._first_value(extra_params, "driver_phone", "user_phone_number", "phone", "from")
            or ""
        ).strip()
        if value:
            return value

        prompt = str(getattr(self.orchestration_event, "prompt", "") or "")
        digits = "".join(re.findall(r"\d+", prompt))
        return digits if len(digits) >= 8 else ""

    def _session_phone(self) -> str:
        return self._session_phones().get("user_phone_number", "")

    def _phones_for_response(self) -> tuple[str | None, str | None]:
        extra_params = self.orchestration_event.extra_params or {}
        user_phone = extra_params.get("user_phone_number") or self._event_phone()
        agent_phone = extra_params.get("agent_phone_number") or BOT_PHONE_ID
        if not user_phone or not agent_phone:
            phones = self._session_phones()
            user_phone = user_phone or phones.get("user_phone_number")
            agent_phone = agent_phone or phones.get("agent_phone_number")
        return (_normalizar_telefono(user_phone) if user_phone else None, agent_phone)

    def _session_phones(self) -> dict[str, str]:
        session_uuid = self.orchestration_event.orchestration_session_uuid
        if not session_uuid:
            return {}
        try:
            response = orchestrator_api_manager.call(
                "get_orchestration_events",
                orchestration_session_id=str(session_uuid),
                access_token=self.orchestration_event.access_token,
                organization_id=self.orchestration_event.organization.organization_id,
            )
        except Exception as exc:
            logger.error("Error reading session phones: %s", exc)
            return {}

        events = response if isinstance(response, list) else response.get("orchestration_events", [])
        for event in events if isinstance(events, list) else []:
            if event.get("event_type") != "new_ticket":
                continue
            extra_params = event.get("extra_params") or {}
            phones: dict[str, str] = {}
            if extra_params.get("user_phone_number"):
                phones["user_phone_number"] = extra_params["user_phone_number"]
            if extra_params.get("agent_phone_number"):
                phones["agent_phone_number"] = extra_params["agent_phone_number"]
            if phones:
                return phones
        return {}

    def _send_whatsapp(self, text: str) -> None:
        user_phone, agent_phone = self._phones_for_response()
        if not user_phone or not agent_phone:
            logger.warning("Skipping WhatsApp response: user_phone=%s agent_phone=%s", user_phone, agent_phone)
            return
        evolve_response = orchestrator_api_manager.call(
            "evolve_event",
            parent_event_uuid=str(self.orchestration_event.event_id),
            event_type="response_to_whatsapp_message",
            source="agent",
            target="orchestrator",
            prompt=text,
            extra_params={"user_phone_number": user_phone, "agent_phone_number": agent_phone},
            access_token=self.orchestration_event.access_token,
            organization_id=self.orchestration_event.organization.organization_id,
        )
        if evolve_response.get("status_code") not in (200, 201):
            raise RuntimeError(f"Failed to evolve WhatsApp event: {evolve_response.get('error')}")
        whatsapp_event = self.orchestration_event.model_copy(deep=True)
        whatsapp_event.event_id = evolve_response["uuid"]
        whatsapp_event.event_type = "response_to_whatsapp_message"
        whatsapp_event.source = "agent"
        whatsapp_event.target = "orchestrator"
        whatsapp_event.prompt = text
        whatsapp_event.extra_params = evolve_response.get("extra_params", {})
        orchestrator_api_manager.call(
            "forward_oe_to_kafka",
            orchestration_event=whatsapp_event.model_dump(),
            topic="orchestrator",
            access_token=whatsapp_event.access_token,
            organization_id=whatsapp_event.organization.organization_id,
        )

    def _extract_tool_args(self) -> dict[str, Any]:
        extra_params = self.orchestration_event.extra_params or {}
        tool_calls = extra_params.get("tool_calls", [])
        if not tool_calls:
            return {}
        args = tool_calls[0].get("args", {}) or {}
        return args if isinstance(args, dict) else {}

    def _first_value(self, data: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value:
                return value
        return None


@contextmanager
def _tenant_data_public_test_mode() -> Iterator[None]:
    previous_mode = os.environ.get("MODE")
    os.environ["MODE"] = "PRODUCTION"
    try:
        yield
    finally:
        if previous_mode is None:
            os.environ.pop("MODE", None)
        else:
            os.environ["MODE"] = previous_mode
