import argparse
import json
import logging
import re
import ssl
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator, TypedDict

try:  # Package imports
    from . import constants
    from .tools import fetch_cric_topology, publish
except ImportError:  # Script-style imports
    import constants
    from tools import fetch_cric_topology, publish

from dateutil.relativedelta import relativedelta
from dirq.queue import Queue

LOG = logging.getLogger("apel_parser")

DESY_FEDERATIONS_OVERRIDES: dict[tuple[str, str], str] = {}
for federation_name, override in constants.DESY_FEDERATIONS.items():
    for site_name in override["sites"]:
        DESY_FEDERATIONS_OVERRIDES[(site_name, str(override["vo"]))] = federation_name

RE_NORMALISED_COMPUTING_DURATION = re.compile(
    r'^\s*\{?\s*(?:(?P<benchmark>[^:}]+?)\s*:\s*)?(?P<duration>[-+]?\d+(?:\.\d+)?)\s*\}?\s*$'
)

SECONDS_PER_HOUR = 3600.0


class SiteInfo(TypedDict):
    tier: str
    country: str
    federation: str


class MessagePayload(TypedDict):
    msgid: str
    body: str


MonthKey = tuple[int, int]
PerCeKey = tuple[str, str, str, str]
AggKey = tuple[str, str, str]
Bucket = dict[tuple[str, ...], dict[str, Any]]


@dataclass(frozen=True)
class ParserConfig:
    messages_dir: str
    months: int
    output_dir: Path
    publish: bool = False
    wlcg_only: bool = False


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _canonicalize_vo(raw_vo: str) -> str:
    """Return canonical VO name if known, otherwise return VO name unchanged."""
    return constants.WLCG_VOS.get(raw_vo.lower(), raw_vo)


class APELMessageParser:
    def __init__(self, config: ParserConfig) -> None:
        self.config = config
        self.cutoff = date.today().replace(day=1) - relativedelta(months=max(config.months, 1) - 1)
        self.cric_data = fetch_cric_topology()
        if not self.cric_data:
            LOG.warning("CRIC topology is empty; site enrichment may be incomplete")
        self.warned_sites: set[str] = set()

    @staticmethod
    def parse_normalised_computing_duration(raw: str | None) -> tuple[str, float]:
        """Extract benchmark and numeric value from NormalisedWallDuration/NormalisedCpuDuration."""
        if not raw:
            return constants.UNKNOWN, 0.0

        match = RE_NORMALISED_COMPUTING_DURATION.match(raw)
        if not match:
            return constants.UNKNOWN, 0.0

        benchmark = match.group("benchmark").strip().upper() if match.group("benchmark") else constants.UNKNOWN
        duration = _safe_float(match.group("duration"), default=0.0)
        return benchmark, duration

    @staticmethod
    def parse_apel_body(text: str) -> Iterator[dict[str, str]]:
        """Yield dicts from a %%-separated APEL body file."""
        current: dict[str, str] = {}
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            if line == "%%":
                if current:
                    yield current
                    current = {}
                continue
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            current[key.strip()] = value.strip()

        if current:
            yield current

    def resolve_site(self, site_name: str, vo: str, year: int, month: int) -> SiteInfo | None:
        """Look up tier, country, and federation for an rcsite name."""
        if site_name not in self.cric_data:
            return None

        rcsite = self.cric_data[site_name]
        country = rcsite.get("country_code", constants.UNKNOWN)

        tier_level = rcsite.get("rc_tier_level")
        if site_name == "CERN-PROD" or tier_level == 0:
            tier = "Tier-0"
        elif tier_level == 1:
            tier = "Tier-1"
        elif tier_level == 2:
            tier = "Tier-2"
        else:
            tier = f"Tier-{tier_level}" if tier_level is not None else constants.UNKNOWN
 
        cric_federations = rcsite.get("federations", [])
        federation = cric_federations[0] if cric_federations else constants.NON_MOU_FEDERATION
        if desy_federation_override := DESY_FEDERATIONS_OVERRIDES.get((site_name, vo.lower())):
            federation = desy_federation_override
        elif site_name == "JINR-LCG2":
            federation = "JINR-LCG2" if date(year, month, 1) >= date(2024, 11, 1) else "RU-RDIG"

        return {"tier": tier, "country": country, "federation": federation}

    def load_messages(self) -> tuple[Any, list[MessagePayload]]:
        """Lock and read all available dirq messages, returning payloads."""
        queue = Queue(self.config.messages_dir, schema=constants.APEL_DIRQ_SCHEMA)
        locked_messages: list[MessagePayload] = []

        for msgid in queue:
            if not queue.lock(msgid):
                continue

            try:
                record = queue.get(msgid)
                body = record.get("body", "") if isinstance(record, dict) else ""
                locked_messages.append({"msgid": msgid, "body": str(body or "")})
            except Exception:
                LOG.exception("Failed to read queue message %s; unlocking", msgid)
                try:
                    queue.unlock(msgid)
                except Exception:
                    LOG.exception("Failed to unlock queue message %s", msgid)

        return queue, locked_messages

    def ingest(self, messages: list[MessagePayload]) -> tuple[dict[MonthKey, Bucket], dict[MonthKey, Bucket]]:
        """Parse APEL payloads and return per-CE and aggregated monthly buckets."""
        per_ce: dict[MonthKey, Bucket] = {}
        agg: dict[MonthKey, Bucket] = {}

        for message in messages:
            msgid = message["msgid"]
            for rec in self.parse_apel_body(message["body"]):
                try:
                    year = int(rec.get("Year", 0))
                    month = int(rec.get("Month", 0))
                except ValueError:
                    continue

                if year <= 0 or not 1 <= month <= 12 or date(year, month, 1) < self.cutoff:
                    continue

                site = rec.get("Site", "").strip()
                vo = _canonicalize_vo(rec.get("VO", "").strip())
                ce = rec.get("SubmitHost", "").strip() or "None"
                if not site or not vo:
                    LOG.warning(
                        "Skipping record missing required Site/VO (msgid=%s, year=%s, month=%s)",
                        msgid,
                        rec.get("Year"),
                        rec.get("Month"),
                    )
                    continue

                if self.config.wlcg_only and vo.lower() not in constants.WLCG_VOS:
                    continue

                wc_time = _safe_float(rec.get("WallDuration", 0), default=0.0) / SECONDS_PER_HOUR
                benchmark, wc_work = self.parse_normalised_computing_duration(rec.get("NormalisedWallDuration"))
                wc_work = wc_work / SECONDS_PER_HOUR
                cpu_time = _safe_float(rec.get("CpuDuration", 0), default=0.0) / SECONDS_PER_HOUR
                _, cpu_work = self.parse_normalised_computing_duration(rec.get("NormalisedCpuDuration"))
                cpu_work = cpu_work / SECONDS_PER_HOUR
                cpu_eff = 0
                number_of_jobs = _safe_int(rec.get("NumberOfJobs", 0), default=0)

                site_info = self.resolve_site(site, vo, year, month)
                if site_info is None:
                    if site not in self.warned_sites:
                        LOG.warning("Site %s not found in CRIC - skipping enrichment", site)
                        self.warned_sites.add(site)
                    site_info = {
                        "tier": constants.UNKNOWN,
                        "country": constants.UNKNOWN,
                        "federation": constants.UNKNOWN,
                    }

                meta = {
                    "site": site,
                    "vo": vo,
                    "infrastructure": constants.GRID_INFRASTRUCTURE,
                    "benchmark": benchmark,
                    **site_info,
                }
                month_key: MonthKey = (year, month)

                ce_key: PerCeKey = (site, vo, ce, benchmark)
                ce_bucket = per_ce.setdefault(month_key, {})
                if ce_key not in ce_bucket:
                    ce_bucket[ce_key] = {
                        **meta,
                        "ce": ce,
                        "raw_wc_time": 0.0,
                        "raw_wc_work": 0.0,
                        "raw_cpu_time": 0.0,
                        "raw_cpu_work": 0.0,
                        "raw_cpu_eff": 0,
                        "number_of_jobs": 0,
                    }
                ce_bucket[ce_key]["raw_wc_time"] += wc_time
                ce_bucket[ce_key]["raw_wc_work"] += wc_work
                ce_bucket[ce_key]["raw_cpu_time"] += cpu_time
                ce_bucket[ce_key]["raw_cpu_work"] += cpu_work
                ce_bucket[ce_key]["raw_cpu_eff"] += cpu_eff
                ce_bucket[ce_key]["number_of_jobs"] += number_of_jobs

                agg_key: AggKey = (site, vo, benchmark)
                agg_bucket = agg.setdefault(month_key, {})
                if agg_key not in agg_bucket:
                    agg_bucket[agg_key] = {
                        **meta,
                        "raw_wc_time": 0.0,
                        "raw_wc_work": 0.0,
                        "raw_cpu_time": 0.0,
                        "raw_cpu_work": 0.0,
                        "raw_cpu_eff": 0,
                        "number_of_jobs": 0,
                    }
                agg_bucket[agg_key]["raw_wc_time"] += wc_time
                agg_bucket[agg_key]["raw_wc_work"] += wc_work
                agg_bucket[agg_key]["raw_cpu_time"] += cpu_time
                agg_bucket[agg_key]["raw_cpu_work"] += cpu_work
                agg_bucket[agg_key]["raw_cpu_eff"] += cpu_eff
                agg_bucket[agg_key]["number_of_jobs"] += number_of_jobs

        return per_ce, agg

    @staticmethod
    def build_docs(bucket: Bucket, year: int, month: int, with_ce: bool = False) -> list[OrderedDict[str, Any]]:
        """Turn an accumulated bucket into the JSON document list matching ACC.py schema."""
        dt = datetime(year, month, 1, tzinfo=timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        timestamp = int((dt - epoch).total_seconds() * 1000)

        idb_tags = ["vo", "tier", "country", "federation", "site", "infrastructure", "benchmark"]
        if with_ce:
            idb_tags.append("ce")

        desired_order = ["vo", "tier", "country", "federation", "site", "infrastructure", "benchmark"]
        if with_ce:
            desired_order.append("ce")
        desired_order.extend([
            "raw_wc_time",
            "raw_wc_work",
            "raw_cpu_time",
            "raw_cpu_work",
            "raw_cpu_eff",
            "number_of_jobs",
            "idb_tags",
            "producer",
            "type",
            "timestamp",
        ])

        docs: list[OrderedDict[str, Any]] = []
        for entry in bucket.values():
            doc: OrderedDict[str, Any] = OrderedDict()
            for key in desired_order:
                if key == "idb_tags":
                    doc[key] = idb_tags
                elif key == "producer":
                    doc[key] = constants.MESSAGE_PRODUCER
                elif key == "type":
                    doc[key] = constants.MESSAGE_INFLUXDB_MEASUREMENT
                elif key == "timestamp":
                    doc[key] = timestamp
                else:
                    doc[key] = entry.get(key, 0)
            docs.append(doc)
        return docs

    def write_outputs(self, per_ce: dict[MonthKey, Bucket], agg: dict[MonthKey, Bucket]) -> None:
        all_months = sorted(set(per_ce) | set(agg))

        for year, month in all_months:
            LOG.info("Writing data for %02d/%d", month, year)

            agg_docs = self.build_docs(agg.get((year, month), {}), year, month, with_ce=False)
            ce_docs = self.build_docs(per_ce.get((year, month), {}), year, month, with_ce=True)

            agg_path = self.config.output_dir / f"data_cpu_acc_{year}_{month}.json"
            ce_path = self.config.output_dir / f"data_cpu_acc_ce_{year}_{month}.json"

            agg_path.write_text(json.dumps(agg_docs, indent=4), encoding="utf-8")
            ce_path.write_text(json.dumps(ce_docs, indent=4), encoding="utf-8")

            if self.config.publish:
                publish(str(agg_path))

    def process(self) -> None:
        queue, locked_messages = self.load_messages()

        if not locked_messages:
            LOG.info("No dirq messages found for processing")
            try:
                queue.purge()
            except Exception:
                LOG.exception("Failed to purge dirq queue directories")
            return

        locked_msgids = [message["msgid"] for message in locked_messages]
        LOG.info("Locked %d dirq messages for processing", len(locked_msgids))

        try:
            per_ce, agg = self.ingest(locked_messages)
            self.write_outputs(per_ce, agg)
        except Exception:
            for msgid in locked_msgids:
                try:
                    queue.unlock(msgid)
                except Exception:
                    LOG.exception("Failed to unlock message %s after processing error", msgid)
            raise
        else:
            removed = 0
            for msgid in locked_msgids:
                try:
                    queue.remove(msgid)
                    removed += 1
                except Exception:
                    LOG.exception(
                        "Processing succeeded but failed to remove message %s; it will be retried",
                        msgid,
                    )
            LOG.info("Removed %d/%d processed dirq messages", removed, len(locked_msgids))
        finally:
            try:
                queue.purge()
            except Exception:
                LOG.exception("Failed to purge dirq queue directories")


def get_data_for_period(
    output_dir: str,
    messages_dir: str,
    months: int,
    publish: bool = False,
    wlcg_only: bool = False,
) -> None:
    config = ParserConfig(
        messages_dir=messages_dir,
        months=months,
        output_dir=Path(output_dir),
        publish=publish,
        wlcg_only=wlcg_only,
    )
    APELMessageParser(config).process()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate CPU accounting data from APEL spool files (replaces EGI portal source)"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        metavar="",
        required=True,
        help="Path to output directory"
    )
    parser.add_argument(
        "--messages-dir",
        type=str,
        default=constants.DEFAULT_MESSAGES_DIR,
        help=f"Directory containing APEL dirq messages (default: {constants.DEFAULT_MESSAGES_DIR})",
    )
    parser.add_argument(
        "-m",
        "--months",
        type=int,
        metavar="",
        default=1,
        help="Months before current date to ingest. Default: 1",
    )
    parser.add_argument(
        "--wlcg",
        action="store_true",
        help="Only ingest records of WLCG VOs: ATLAS, CMS, ALICE, LHCb",
    )
    parser.add_argument(
        "-p",
        "--publish",
        action="store_true",
        help="Publish aggregated results to message broker",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s: %(message)s",
    )

    output_timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    output_path = Path(args.output_dir) / output_timestamp
    if not output_path.is_dir():
        LOG.info("Creating output directory %s", output_path)
        output_path.mkdir(parents=True, exist_ok=True)

    get_data_for_period(
        str(output_path),
        args.messages_dir,
        args.months,
        args.publish,
        args.wlcg,
    )


if __name__ == "__main__":
    main()
