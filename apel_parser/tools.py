from __future__ import annotations

import json
import logging
import uuid
from pathlib import Path
from typing import Any, Iterable

import requests
from stompest.config import StompConfig
from stompest.error import StompError
from stompest.protocol import StompSpec
from stompest.sync import Stomp

try:  # Package imports
    from . import constants
except ImportError:  # Script-style imports
    import constants

_logger = logging.getLogger("apel_parser.publisher")


class Publisher:
    """Message publisher for sending accounting data to broker."""

    def __init__(self, host: str, port: int, username: str, password: str, topic: str) -> None:
        stomp_config = StompConfig(
            uri=f"tcp://{host}:{port}",
            login=username,
            passcode=password,
        )
        self._client = Stomp(stomp_config)
        self._destination = f"/topic/{topic}"

    def __enter__(self) -> "Publisher":
        """Enter the runtime context related to this object."""
        self._client.connect()
        return self

    def __exit__(self, exc_type, value, traceback) -> bool:
        """Exit the runtime context related to this object."""
        self._client.disconnect()
        return exc_type is None

    def send(self, documents: Iterable[dict[str, Any]]) -> None:
        """Send a list of documents to the message broker."""
        documents = list(documents)
        prefix = str(uuid.uuid4())

        with self._client.transaction(receipt=prefix) as transaction:
            self._expect_receipt(f"{prefix}-begin")
            for entry in documents:
                headers = {StompSpec.TRANSACTION_HEADER: transaction}
                self._client.send(
                    self._destination,
                    json.dumps(entry).encode(),
                    headers,
                )

        self._expect_receipt(f"{prefix}-commit")
        _logger.info("Submitted %d documents", len(documents))

    def _expect_receipt(self, receipt_id: str, timeout: int = 60) -> None:
        """Wait for a RECEIPT frame and verify its ID."""
        if not self._client.canRead(timeout):
            raise StompReceiptError("Read timeout expired")
        frame = self._client.receiveFrame()
        frame.unraw()
        response = dict(frame)

        if response.get("command") != StompSpec.RECEIPT:
            raise StompReceiptError("Frame is not of type RECEIPT")

        headers = response.get("headers", {})
        if headers.get(StompSpec.RECEIPT_ID_HEADER) != receipt_id:
            raise StompReceiptError("Frame has unexpected ID")


class StompReceiptError(StompError):
    """Raised for failing to receive a proper RECEIPT frame."""


class PublishConfigError(RuntimeError):
    """Raised when required publishing configuration is missing."""


def publish(file_path: str | Path) -> None:
    """Publish accounting data from a JSON file to the message broker."""
    config = {
        "host": constants.MQ_HOST,
        "port": constants.MQ_PORT,
        "username": constants.MQ_USERNAME,
        "password": constants.MQ_PASSWORD,
        "topic": constants.MESSAGE_TOPIC,
    }
    with Publisher(**config) as pub:
        resolved_path = Path(file_path)
        _logger.info("Reading from file %s", resolved_path)
        with resolved_path.open(encoding="utf-8") as f:
            pub.send(json.load(f))


def fetch_cric_topology(api: str | None = None) -> dict[str, Any]:
    """Fetch raw CRIC RCSITE topology payload."""
    target_api = api or constants.CRIC_RCSITE_API
    try:
        response = requests.get(
            target_api,
            timeout=constants.CRIC_REQUEST_TIMEOUT_SECONDS,
            verify="/cvmfs/grid.cern.ch/etc/grid-security/certificates"
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            _logger.error("Unexpected CRIC payload type %s from %s", type(payload).__name__, target_api)
            return {}
        return payload
    except requests.exceptions.RequestException as req_err:
        _logger.error("Failed to fetch CRIC topology from %s: %s", target_api, req_err)
        return {}
    except json.JSONDecodeError as json_err:
        _logger.error("Invalid JSON from CRIC topology endpoint %s: %s", target_api, json_err)
        return {}
