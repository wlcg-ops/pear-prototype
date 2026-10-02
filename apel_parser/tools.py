from __future__ import annotations

import json
import logging
import uuid
from csv import DictReader, Error as CSVError
from datetime import datetime, timezone
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

_logger = logging.getLogger("apel_parser.tools")


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


class CricTopologyLookupError(RuntimeError):
    """Raised when CRIC site topology cannot be obtained."""


class HistoricalTopologyLookupError(RuntimeError):
    """Raised when historical site topology cannot be obtained safely."""


def format_timestamp(timestamp_ms: int) -> str:
    """Return a millisecond epoch timestamp as a human-readable UTC string."""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_historical_topology(
    earliest_timestamp: int,
    latest_timestamp: int,
) -> dict[tuple[int, tuple[str, ...]], list[tuple[str, ...]]]:
    """Fetch the site topology of accounting points already stored in InfluxDB.

    Queries the InfluxDB measurement and maps each (timestamp_ms, series identity tags) key
    to the list of site topology tag tuples found for it. More than one tuple means the key
    has duplicate series.
    """
    required_config = {
        "INFLUXDB_DATABASE": constants.INFLUXDB_DATABASE,
        "INFLUXDB_MEASUREMENT": constants.INFLUXDB_MEASUREMENT,
        "MONIT_DATASOURCES_API": constants.MONIT_DATASOURCES_API,
        "MONIT_DATASOURCES_TOKEN": constants.MONIT_DATASOURCES_TOKEN,
    }
    if missing_config := [name for name, value in required_config.items() if not value]:
        raise HistoricalTopologyLookupError(
            f"Missing required environment variables: {', '.join(missing_config)}"
        )

    select_columns = (
        constants.INFLUXDB_SERIES_IDENTITY_TAGS
        + constants.INFLUXDB_SITE_TOPOLOGY_TAGS
        + [constants.COMMON_ACCOUNTING_FIELDS[0]]  # at least one field required by InfluxQL
    )
    select_columns_string = ", ".join(column for column in select_columns)
    earliest_timestamp = int(earliest_timestamp)
    latest_timestamp = int(latest_timestamp)
    if earliest_timestamp > latest_timestamp:
        raise HistoricalTopologyLookupError(
            f"Invalid historical topology time range: "
            f"{format_timestamp(earliest_timestamp)} > {format_timestamp(latest_timestamp)}"
        )
    time_range = f"{format_timestamp(earliest_timestamp)} to {format_timestamp(latest_timestamp)}"
    query = f'''
        SELECT {select_columns_string}
        FROM "{constants.INFLUXDB_MEASUREMENT}"
        WHERE time >= {earliest_timestamp}ms AND time <= {latest_timestamp}ms
    '''

    try:
        with requests.get(
            constants.MONIT_DATASOURCES_API,
            params={"db": constants.INFLUXDB_DATABASE, "q": query},
            headers={
                "Accept": "application/csv",
                "Authorization": f"Bearer {constants.MONIT_DATASOURCES_TOKEN}",
            },
            timeout=constants.MONIT_REQUEST_TIMEOUT_SECONDS,
            stream=True,
        ) as response:
            response.raise_for_status()
            response.encoding = "utf-8"

            reader = DictReader(response.iter_lines(decode_unicode=True))
            if not reader.fieldnames:
                _logger.info(
                    f"Fetched 0 stored topology points from {constants.INFLUXDB_MEASUREMENT} "
                    f"for {time_range}"
                )
                return {}
            # InfluxDB reports query errors with HTTP 200 as a CSV body containing a single "error" column.
            if "error" in reader.fieldnames:
                raise HistoricalTopologyLookupError(
                    f"Historical topology query failed for {time_range}: "
                    f"{next(reader, {}).get('error')}"
                )
            expected_columns = {"time", *select_columns}
            if missing_columns := expected_columns - set(reader.fieldnames):
                raise HistoricalTopologyLookupError(
                    f"Unexpected historical topology response, missing CSV columns: {sorted(missing_columns)}"
                )

            topology: dict[tuple[int, tuple[str, ...]], list[tuple[str, ...]]] = {}
            for row in reader:
                key = (
                    int(row["time"]) // 1_000_000,
                    tuple(row[tag] for tag in constants.INFLUXDB_SERIES_IDENTITY_TAGS),
                )
                topology.setdefault(key, []).append(
                    tuple(row[tag] for tag in constants.INFLUXDB_SITE_TOPOLOGY_TAGS)
                )
        _logger.info(
            f"Fetched {sum(len(records) for records in topology.values())} stored topology points "
            f"({len(topology)} distinct lookup keys) from {constants.INFLUXDB_MEASUREMENT} "
            f"for {time_range}"
        )
        return topology
    except (requests.RequestException, CSVError, KeyError, TypeError, ValueError) as error:
        raise HistoricalTopologyLookupError(
            f"Historical topology lookup failed for {time_range}: {error}"
        ) from error


def publish(file_path: str | Path) -> None:
    """Publish accounting data from a JSON file to the message broker."""
    mq_config = constants.MQ_CONFIG
    if any(value is None for value in mq_config.values()):
        raise PublishConfigError("Missing required MQ environment variables")

    config = {
        "host": str(mq_config["host"]),
        "port": int(mq_config["port"]),
        "username": str(mq_config["username"]),
        "password": str(mq_config["password"]),
        "topic": constants.MESSAGE_TOPIC,
    }
    with Publisher(**config) as pub:
        resolved_path = Path(file_path)
        _logger.info("Reading from file %s", resolved_path)
        with resolved_path.open(encoding="utf-8") as f:
            pub.send(json.load(f))


def fetch_cric_topology(api: str | None = None) -> dict[str, Any]:
    """Fetch CRIC RCSITE topology."""
    target_api = api or constants.CRIC_RCSITE_API
    try:
        response = requests.get(
            target_api,
            timeout=constants.CRIC_REQUEST_TIMEOUT_SECONDS,
            verify=constants.IGTF_TRUST_BUNDLE_PATH,
        )
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as error:
        raise CricTopologyLookupError(
            f"CRIC topology lookup from {target_api} failed: {error}"
        ) from error

    if not isinstance(payload, dict) or not payload:
        raise CricTopologyLookupError(
            f"CRIC topology from {target_api} is empty or not a JSON object"
        )
    return payload
