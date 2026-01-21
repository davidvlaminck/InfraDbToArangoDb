"""Benchmark runner for the assets fill.

This script is intended for local benchmarking against an ArangoDB instance.
It can (optionally) wipe the database using the existing CreateDBStep logic,
then run a limited assets fill (stop after N assets) and report throughput.

We keep semantics identical to the normal pipeline: same EMSON endpoint,
uses InitialFillStep._insert_assets and Arango import_bulk.

Usage (examples):

- Baseline run (no reset), ingest 50k assets:
  python bench_fill_assets.py --limit 50000

- Reset DB first (DANGEROUS), then ingest 200k assets:
  python bench_fill_assets.py --reset --confirm "I UNDERSTAND" --limit 200000

- Compare settings:
  python bench_fill_assets.py --limit 200000 --page-size 2000 --asset-chunk 2000

Results are written to Analysis/bench_results/ as JSON.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from API.APIEnums import AuthType, Environment
from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from ArangoDBConnectionFactory import ArangoDBConnectionFactory
from CreateDBStep import CreateDBStep
from Enums import DBStep, ResourceEnum
from GenericDbFunctions import set_db_step
import InitialFillStep as initial_fill_mod
from InitialFillStep import InitialFillStep


RESULTS_DIR = Path(__file__).resolve().parent / "Analysis" / "bench_results"


@dataclass
class BenchConfig:
    env: str
    db_name: str
    page_size: int
    limit: int
    asset_chunk_size: int
    bestek_chunk_size: int
    reset: bool
    prep_small: bool


@dataclass
class BenchResult:
    config: BenchConfig
    started_at_utc: str
    finished_at_utc: str
    seconds: float
    assets_inserted: int
    assets_per_sec: float
    last_cursor: Optional[str]
    prereq_seconds: float


def load_settings(settings_path: Path) -> Dict[str, Any]:
    return json.loads(settings_path.read_text())


def build_factory(settings: Dict[str, Any], env: Environment) -> ArangoDBConnectionFactory:
    db_settings = settings["databases"][env.name.lower()]
    return ArangoDBConnectionFactory(db_settings["database"], db_settings["user"], db_settings["password"])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--settings",
        default=str(Path("/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json")),
        help="Path to settings_SyncToArangoDB.json",
    )
    p.add_argument("--env", default="PRD", choices=["PRD", "TEI", "DEV", "AIM"], help="API environment")
    p.add_argument("--page-size", type=int, default=1000, help="EMSON page size")
    p.add_argument("--limit", type=int, default=50000, help="Stop after ingesting this many assets")

    p.add_argument("--asset-chunk", type=int, default=initial_fill_mod.ASSET_IMPORT_CHUNK_SIZE, help="Chunk size for assets import_bulk")
    p.add_argument("--bestek-chunk", type=int, default=initial_fill_mod.BESTEK_IMPORT_CHUNK_SIZE, help="Chunk size for bestekkoppelingen import_bulk")

    p.add_argument("--reset", action="store_true", help="WIPE the db first using CreateDBStep")
    p.add_argument(
        "--confirm",
        default="",
        help="Required when using --reset. Must be exactly: I UNDERSTAND",
    )

    # Fast benchmark reset (keeps prereqs)
    p.add_argument(
        "--truncate-assets-only",
        action="store_true",
        help=(
            "Truncate only the hot collections for assets benchmarking (keeps prereq collections). "
            "This resets params/fill_assets cursor + fill flag."
        ),
    )
    p.add_argument(
        "--truncate-edges",
        action="store_true",
        help=(
            "When used with --truncate-assets-only, also truncate edge collections that depend on assets "
            "(assetrelaties, betrokkenerelaties, bestekkoppelingen)."
        ),
    )

    # Benchmark hygiene / UX
    p.add_argument("--progress-every", type=int, default=5000, help="Print progress every N inserted assets (0 disables)")

    prep_group = p.add_mutually_exclusive_group()
    prep_group.add_argument(
        "--prep-small",
        dest="prep_small",
        action="store_true",
        help="Fill prerequisite collections first (assettypes, relatietypes, ...) like the main pipeline.",
    )
    prep_group.add_argument(
        "--no-prep-small",
        dest="prep_small",
        action="store_false",
        help="Skip prerequisite fill (not recommended for apples-to-apples with main pipeline).",
    )
    p.set_defaults(prep_small=True)

    return p.parse_args()


def guarded_reset(factory: ArangoDBConnectionFactory, confirm: str):
    if confirm != "I UNDERSTAND":
        raise SystemExit("Refusing to reset DB. Add --confirm 'I UNDERSTAND'.")

    # Only allow when explicitly requested
    step = CreateDBStep(factory)

    # Force reset by deleting params if it exists (this follows your existing reset mechanism)
    db = factory.create_connection()
    if db.has_collection("params"):
        db.delete_collection("params", ignore_missing=True)
    step.execute()

    # Ensure pipeline step is INITIAL_FILL
    set_db_step(db, DBStep.INITIAL_FILL)


def _ensure_prerequisites(db, step: InitialFillStep) -> float:
    """Fill the small prerequisite collections exactly like the main pipeline initial batch.

    Returns wall time in seconds.
    """
    started = time.perf_counter()

    prereq = [
        ResourceEnum.assettypes.value,
        ResourceEnum.relatietypes.value,
        ResourceEnum.toezichtgroepen.value,
        ResourceEnum.bestekken.value,
        ResourceEnum.identiteiten.value,
        ResourceEnum.beheerders.value,
    ]

    for resource in prereq:
        step._fill_resource_using_em_infra(resource)

    # Warm the lookups so the assets ingest doesn't spam warnings.
    step.assettype_lookup = {at["uri"]: at["_key"] for at in db.collection("assettypes")}
    step.beheerders_lookup = {b["referentie"]: b["_key"] for b in db.collection("beheerders")}

    return time.perf_counter() - started


def _reset_fill_cursor(db, resource: str):
    """Reset fill cursor for a resource to start from scratch."""
    params = db.collection("params")
    key = f"fill_{resource}"
    doc = params.get(key)
    if doc is None:
        params.insert({"_key": key, "fill": True, "from": None})
        return
    db.aql.execute(
        "UPDATE @key WITH { from: @from, fill: true } IN params",
        bind_vars={"key": key, "from": None},
    )


def truncate_assets_only(factory: ArangoDBConnectionFactory, truncate_edges: bool) -> None:
    """Truncate assets (and optionally edges) and reset only the assets fill cursor.

    This is meant for repeatable micro-benchmarks once prereqs are already present.
    """
    db = factory.create_connection()

    if not db.has_collection("assets"):
        raise SystemExit("DB missing 'assets' collection. Did you run CreateDBStep at least once?")

    db.collection("assets").truncate()

    if truncate_edges:
        for name in ("assetrelaties", "betrokkenerelaties", "bestekkoppelingen"):
            if db.has_collection(name):
                db.collection(name).truncate()

    if not db.has_collection("params"):
        # CreateDBStep normally creates this, but be defensive.
        db.create_collection("params")

    _reset_fill_cursor(db, "assets")


def run_benchmark(
    settings: Dict[str, Any],
    env: Environment,
    page_size: int,
    limit: int,
    asset_chunk: int,
    bestek_chunk: int,
    reset: bool,
    confirm: str,
    prep_small: bool,
    progress_every: int,
    # new
    truncate_assets_only_flag: bool = False,
    truncate_edges: bool = False,
) -> BenchResult:
    factory = build_factory(settings, env)
    db = factory.create_connection()

    # Patch chunk sizes for this run (module-level constants)
    initial_fill_mod.ASSET_IMPORT_CHUNK_SIZE = asset_chunk
    initial_fill_mod.BESTEK_IMPORT_CHUNK_SIZE = bestek_chunk

    if reset and truncate_assets_only_flag:
        raise SystemExit("Choose either --reset or --truncate-assets-only, not both.")

    if reset:
        guarded_reset(factory, confirm=confirm)
        db = factory.create_connection()

    if truncate_assets_only_flag:
        truncate_assets_only(factory, truncate_edges=truncate_edges)
        db = factory.create_connection()

    eminfra = EMInfraClient(auth_type=AuthType.JWT, env=env, settings=settings)
    emson = EMSONClient(auth_type=AuthType.JWT, env=env, settings=settings)

    step = InitialFillStep(factory, eminfra_client=eminfra, emson_client=emson, page_size=page_size)

    prereq_seconds = 0.0
    if reset and prep_small:
        prereq_seconds = _ensure_prerequisites(db, step)

    inserted = 0
    cursor: Optional[str] = None

    started = time.perf_counter()
    started_at = datetime.now(timezone.utc).isoformat()

    start_cursor: Optional[str] = None

    # start cursor:
    # - full reset: always None
    # - truncate-assets-only: params/fill_assets is reset to None so still None
    for cursor, assets in step.emson_client.get_resource_by_cursor("assets", cursor=start_cursor, page_size=page_size):
        if not assets:
            if cursor is None:
                break
            continue

        remaining = limit - inserted
        batch = assets if remaining >= len(assets) else assets[:remaining]

        step._insert_assets(db, batch)
        inserted += len(batch)

        if progress_every and inserted % progress_every == 0:
            elapsed = time.perf_counter() - started
            rate = inserted / elapsed if elapsed else 0.0
            print(f"progress: {inserted:,}/{limit:,} assets ({rate:,.1f} assets/s)", flush=True)

        if inserted >= limit:
            break

        if cursor is None:
            break

    seconds = time.perf_counter() - started
    finished_at = datetime.now(timezone.utc).isoformat()

    cfg = BenchConfig(
        env=env.name,
        db_name=db.name,
        page_size=page_size,
        limit=limit,
        asset_chunk_size=asset_chunk,
        bestek_chunk_size=bestek_chunk,
        reset=reset,
        prep_small=prep_small,
    )

    rps = inserted / seconds if seconds > 0 else 0.0
    return BenchResult(
        config=cfg,
        started_at_utc=started_at,
        finished_at_utc=finished_at,
        seconds=seconds,
        assets_inserted=inserted,
        assets_per_sec=rps,
        last_cursor=cursor,
        prereq_seconds=prereq_seconds,
    )


def main() -> int:
    args = parse_args()

    settings_path = Path(args.settings)
    if not settings_path.exists():
        raise SystemExit(f"settings file not found: {settings_path}")

    settings = load_settings(settings_path)
    env = Environment[args.env]

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    result = run_benchmark(
        settings=settings,
        env=env,
        page_size=args.page_size,
        limit=args.limit,
        asset_chunk=args.asset_chunk,
        bestek_chunk=args.bestek_chunk,
        reset=args.reset,
        confirm=args.confirm,
        prep_small=args.prep_small,
        progress_every=args.progress_every,
        truncate_assets_only_flag=args.truncate_assets_only,
        truncate_edges=args.truncate_edges,
    )

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = RESULTS_DIR / f"bench_assets_{ts}.json"
    out_path.write_text(json.dumps(asdict(result), indent=2, ensure_ascii=False))

    print(json.dumps(asdict(result), indent=2, ensure_ascii=False), flush=True)
    print(f"\nSaved: {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
