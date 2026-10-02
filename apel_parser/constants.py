import os
from typing import Final


# ==============================================================================
# APEL - CONSUMED MESSAGES
# ==============================================================================

DEFAULT_MESSAGES_DIR: Final[str] = "/var/spool/apel/grid/incoming"

APEL_DIRQ_SCHEMA: Final[dict[str, str]] = {
    "body": "string",
    "signer": "string",
    "empaid": "string?",
}

# ==============================================================================
# SITE TOPOLOGY
# ==============================================================================

CRIC_RCSITE_API: Final[str] = "https://wlcg-cric.cern.ch/api/core/rcsite/query/?json&state=ANY"
CRIC_REQUEST_TIMEOUT_SECONDS: Final[int] = 30
IGTF_TRUST_BUNDLE_PATH: Final[str] = os.getenv("IGTF_TRUST_BUNDLE_PATH")

UNKNOWN: Final[str] = "UNKNOWN"

LHC_VOS: Final[dict[str, str]] = {
    "atlas": "ATLAS",
    "cms": "CMS",
    "alice": "ALICE",
    "lhcb": "LHCb",
}

WLCG_TIERS: Final[set[int]] = {0, 1, 2, 3}
NON_WLCG_TIER: Final[str] = "NON-WLCG-Tier"

NON_MOU_FEDERATION: Final[str] = "NON-MOU-Federation"
DESY_FEDERATIONS: Final[dict[str, dict[str, object]]] = {
    "DE-DESY-ATLAS-T2": {
        "sites": ["DESY-HH", "DESY-ZN"],
        "vo": "atlas",
    },
    "DE-DESY-LHCB": {
        "sites": ["DESY-HH"],
        "vo": "lhcb",
    },
    "DE-DESY-RWTH-CMS-T2": {
        "sites": ["DESY-HH"],
        "vo": "cms",
    },
}
NON_WLCG_FEDERATION: Final[str] = "NON-WLCG-Federation"

# ==============================================================================
# PRODUCED ACCOUNTING DATA
# ==============================================================================

INFLUXDB_DATABASE: Final[str] = os.getenv("INFLUXDB_DATABASE")
INFLUXDB_MEASUREMENT: Final[str] = os.getenv("INFLUXDB_MEASUREMENT")

INFLUXDB_TAGS: Final[list[str]] = [
    "vo", "tier", "country", "federation", "site", "infra", "benchmark", "ngi"
]
INFLUXDB_SITE_TOPOLOGY_TAGS: Final[list[str]] = ["tier", "country", "federation", "ngi"]
INFLUXDB_SERIES_IDENTITY_TAGS: Final[list[str]] = [
    tag for tag in INFLUXDB_TAGS if tag not in INFLUXDB_SITE_TOPOLOGY_TAGS
]
if unknown_tags := set(INFLUXDB_SITE_TOPOLOGY_TAGS) - set(INFLUXDB_TAGS):
    raise RuntimeError(f"INFLUXDB_SITE_TOPOLOGY_TAGS contains unknown tags: {sorted(unknown_tags)}")

GRID_INFRA: Final[str] = "Grid"
GRID_LOCAL_INFRA: Final[str] = "Grid-Local"
CLOUD_INFRA: Final[str] = "Cloud"
APEL_INFRA_TYPES: Final[dict[str, str]] = {
    "grid": GRID_INFRA,
    "cloud": CLOUD_INFRA,
}
GRID_INFRA_SUBTYPES: Final[dict[str, str]] = {
    "grid": GRID_INFRA,
    "local": GRID_LOCAL_INFRA,
}

COMMON_ACCOUNTING_FIELDS: Final[list[str]] = [
    "raw_wc_time",
    "raw_wc_work",
    "raw_cpu_time",
    "raw_cpu_work",
    "raw_cpu_eff",
]
GRID_ONLY_ACCOUNTING_FIELDS: Final[list[str]] = [
    "job_count",
]
CLOUD_ONLY_ACCOUNTING_FIELDS: Final[list[str]] = [
    "memory_used",
    "disk_used",
    "vm_count",
    "cpu_core_count",
    "network_in",
    "network_out",
]

SITE_METADATA_FIELDS: Final[list[str]] = [
]

PRODUCED_DOC_METADATA_FIELDS: Final[list[str]] = [
    "idb_tags",
    "producer",
    "type",
    "timestamp",
]
PRODUCED_DOC_COMMON_FIELDS: Final[list[str]] = (
    INFLUXDB_TAGS
    + SITE_METADATA_FIELDS
    + COMMON_ACCOUNTING_FIELDS
)
PRODUCED_DOC_FIELDS: Final[dict[str, list[str]]] = {
    GRID_INFRA: (
        PRODUCED_DOC_COMMON_FIELDS
        + GRID_ONLY_ACCOUNTING_FIELDS
        + PRODUCED_DOC_METADATA_FIELDS
    ),
    CLOUD_INFRA: (
        PRODUCED_DOC_COMMON_FIELDS
        + CLOUD_ONLY_ACCOUNTING_FIELDS
        + PRODUCED_DOC_METADATA_FIELDS
    )
}

# ==============================================================================
# MONIT - PUBLISHED MESSAGES & STORED ACCOUNTING DATA
# ==============================================================================

MQ_CONFIG: Final[dict[str, str | None]] = {
    "host": os.getenv("MQ_HOST"),
    "port": os.getenv("MQ_PORT"),
    "username": os.getenv("MQ_USERNAME"),
    "password": os.getenv("MQ_PASSWORD"),
}
MESSAGE_TOPIC: Final[str | None] = os.getenv("MESSAGE_TOPIC")
MESSAGE_PRODUCER: Final[str | None] = os.getenv("MESSAGE_PRODUCER")

MONIT_DATASOURCES_TOKEN: Final[str] = os.getenv("MONIT_DATASOURCES_TOKEN")
MONIT_DATASOURCES_API: Final[str] = os.getenv("MONIT_DATASOURCES_API")
MONIT_REQUEST_TIMEOUT_SECONDS: Final[int] = 30
