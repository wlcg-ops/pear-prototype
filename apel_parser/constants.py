import os
from typing import Final


CRIC_RCSITE_API: Final[str] = "https://wlcg-cric.cern.ch/api/core/rcsite/query/?json&state=ANY"
CRIC_REQUEST_TIMEOUT_SECONDS: Final[int] = 30

MQ_USERNAME: Final[str] = str(os.getenv("MQ_USERNAME"))
MQ_PASSWORD: Final[str] = str(os.getenv("MQ_PASSWORD"))
MQ_HOST: Final[str] = str(os.getenv("MQ_HOST"))
MQ_PORT: Final[int] = int(os.getenv("MQ_PORT"))

MESSAGE_TOPIC: Final[str] = "wlcgops.accounting.space"
MESSAGE_PRODUCER: Final[str] = "wlcgops"
MESSAGE_INFLUXDB_MEASUREMENT: Final[str] = "accounting.wau.summary_apel_2"

UNKNOWN: Final[str] = "UNKNOWN"

GRID_INFRASTRUCTURE: Final[str] = "Grid"

DEFAULT_MESSAGES_DIR: Final[str] = "/var/spool/apel/grid/incoming"

WLCG_VOS: Final[dict[str, str]] = {
    "atlas": "ATLAS",
    "cms": "CMS",
    "alice": "ALICE",
    "lhcb": "LHCb",
}

APEL_DIRQ_SCHEMA: Final[dict[str, str]] = {
    "body": "string",
    "signer": "string",
    "empaid": "string?",
}

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

NON_MOU_FEDERATION: Final[str] = "NON-MOU-Federation"
