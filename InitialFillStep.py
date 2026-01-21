import logging
import time
import uuid
from concurrent.futures import as_completed, ThreadPoolExecutor
from queue import Queue
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Callable, cast

from pyproj import Transformer
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import transform

from API.EMInfraClient import EMInfraClient
from API.EMSONClient import EMSONClient
from Enums import ResourceEnum, colorama_table


DEFAULT_PAGE_SIZE = 1000
MAX_WORKERS = 8
RETRY_DELAY_SECONDS = 30

# Tune bulk import chunking (keeps behavior, reduces memory spikes)
ASSET_IMPORT_CHUNK_SIZE = 1000
BESTEK_IMPORT_CHUNK_SIZE = 2000


class InitialFillStep:
    """Step that performs the initial fill of the ArangoDB.

    Two families of sources exist:

    - EMSON (cursor-based): `assets`, `assetrelaties`, `betrokkenerelaties`
    - EMInfra (page-based): everything else

    The hot path is assets ingestion. For that we keep the logic identical, but we try to keep the
    code understandable by splitting the enrichment work into small helpers.
    """

    def __init__(self, factory, eminfra_client: EMInfraClient, emson_client: EMSONClient,
                 page_size: int = DEFAULT_PAGE_SIZE,
                 use_pipeline: bool = False,
                 pipeline_queue_size: int = 3):
        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client
        self.default_page_size = page_size

        self.use_pipeline = use_pipeline
        self.pipeline_queue_size = max(1, pipeline_queue_size)

        self.assettype_lookup: Optional[Dict[str, str]] = None
        self.relatietype_lookup: Optional[Dict[str, str]] = None
        self.beheerders_lookup: Optional[Dict[str, str]] = None

        # transformer from Belgian Lambert2008 / EPSG:3812 to WGS84 / EPSG:4326
        self.transformer: Transformer = Transformer.from_crs("EPSG:3812", "EPSG:4326", always_xy=True)

        # resource handler registry
        self._resource_handlers: Dict[str, Callable[[Any, Iterable[Dict[str, Any]]], None]] = {
            ResourceEnum.assettypes.value: self._handle_assettypes,
            ResourceEnum.assets.value: self._handle_assets,
            ResourceEnum.relatietypes.value: self._handle_relatietypes,
            ResourceEnum.assetrelaties.value: self._handle_assetrelaties,
            ResourceEnum.agents.value: self._handle_agents,
            ResourceEnum.betrokkenerelaties.value: self._handle_betrokkenerelaties,
            ResourceEnum.toezichtgroepen.value: self._handle_toezichtgroepen,
            ResourceEnum.identiteiten.value: self._handle_identiteiten,
            ResourceEnum.beheerders.value: self._handle_beheerders,
            ResourceEnum.bestekken.value: self._handle_bestekken,
        }

    # -----------------------
    # Public entry
    # -----------------------
    def execute(self, fill_resource_groups: List[List[ResourceEnum]]):
        db = self.factory.create_connection()
        docs = self._get_docs_to_update(db)

        # no longer need to update these
        # if docs:
        #     docs_to_update = self._build_docs_to_update(docs)
        #     self._update_params_collection(db, docs_to_update)

        self.fill_tables(fill_resource_groups=fill_resource_groups)
        self._removed_fill_params(db)

    # -----------------------
    # params helpers
    # -----------------------
    @staticmethod
    def _get_docs_to_update(db) -> List[Dict[str, Any]]:
        """Fetch params documents where page == -1."""
        cursor = db.aql.execute(
            """
            FOR doc IN params
                FILTER doc.page == -1
                RETURN doc
            """
        )
        return list(cursor)

    def _build_docs_to_update(self, docs: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build updated params documents: attempt to get last page and last event id
        for every feed referenced in params (params keys like 'feed_<feedname>').
        """
        docs_to_update: List[Dict[str, Any]] = []
        for d in docs:
            feed_name = d["_key"][5:]  # strip leading "feed_"
            logging.debug("Updating feed %s", feed_name)

            resource_page = self.eminfra_client.get_last_feedproxy_page(feed_name)
            self_page = next(p for p in resource_page["links"] if p["rel"] == "self")
            page_number = self_page["href"].split("/")[1]

            # pick the latest entry by updated timestamp
            last_entry = sorted(
                resource_page["entries"],
                key=lambda p: datetime.fromisoformat(p["updated"]).astimezone(timezone.utc),
            )[-1]

            logging.debug("Last entry id: %s", last_entry["id"])
            docs_to_update.append(
                {"_id": f"params/feed_{feed_name}", "page": int(page_number), "event_uuid": last_entry["id"]}
            )
        return docs_to_update

    @staticmethod
    def _update_params_collection(db, docs_to_update: List[Dict[str, Any]]):
        """Bulk update params collection with new page/event values."""
        if not docs_to_update:
            return
        db.collection("params").update_many(docs_to_update)

    # -----------------------
    # fill resources (parallel batches with retry)
    # -----------------------
    def _fill_resource_worker(self, resource: ResourceEnum) -> ResourceEnum:
        color = colorama_table[resource]
        logging.info("%sFilling %s table", color, resource.value)
        self._fill_resource(resource.value)
        return resource

    def fill_tables(self, fill_resource_groups: List[List[ResourceEnum]]):
        """
        Execute groups of fill tasks. Each group is attempted in parallel.
        Failed tasks are retried indefinitely with a fixed wait between attempts.
        """
        for group_index, fill_resource_group in enumerate(fill_resource_groups):
            remaining = list(fill_resource_group)
            attempt = 1

            while remaining:
                logging.info("=== Batch attempt %d with %d task(s) ===", attempt, len(remaining))
                failed: List[ResourceEnum] = []
                max_workers = min(len(remaining), MAX_WORKERS)

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(self._fill_resource_worker, r): r for r in remaining}
                    for future in as_completed(futures):
                        resource = futures[future]
                        color = colorama_table[resource]
                        try:
                            res = future.result()
                            logging.info("%sFinished filling %s table", color, res.value)
                        except Exception as e:
                            logging.error("%sError filling %s: %s", color, resource.value, e)
                            failed.append(resource)

                if failed:
                    logging.warning("%d task(s) failed in attempt %d. Retrying in %s seconds...", len(failed), attempt, RETRY_DELAY_SECONDS)
                    time.sleep(RETRY_DELAY_SECONDS)
                    remaining = failed
                    attempt += 1
                else:
                    logging.info("✅ All tasks for group %d completed successfully!", group_index)
                    break

    def _fill_resource(self, resource: str):
        """Dispatch to the appropriate client-based filler."""
        if resource in {ResourceEnum.assets.value, ResourceEnum.assetrelaties.value, ResourceEnum.betrokkenerelaties.value}:
            self._fill_resource_using_emson(resource)
        else:
            self._fill_resource_using_em_infra(resource)

    # -----------------------
    # Fill using EMSON (assets / asset relations)
    # -----------------------
    def _fill_resource_using_emson(self, resource: str):
        """Fill an EMSON cursor-based resource.

        This method persists its cursor inside the `params` collection under `fill_<resource>`.

        When `self.use_pipeline` is enabled, it uses a small producer/consumer pipeline:
        - producer: fetches pages sequentially (cursor semantics remain intact)
        - consumer: transforms and writes to Arango

        This overlaps network I/O with CPU/DB work without breaking ordering.
        """
        color = colorama_table[resource]
        logging.info("%sFilling resource: %s", color, resource)

        db = self.factory.create_connection()
        params_key = f"fill_{resource}"
        params_collection = db.collection("params")

        # ensure params doc exists
        params_resource = params_collection.get(params_key)
        if params_resource is None:
            params_collection.insert({"_key": params_key, "fill": True, "from": None})
            params_resource = params_collection.get(params_key)

        if not params_resource.get("fill", True):
            logging.info("%sSkipping %s, already filled.", color, resource)
            return

        start_cursor = params_resource.get("from")

        # Sequential default (existing behavior)
        if not self.use_pipeline:
            cursor = start_cursor
            for cursor, dicts in self.emson_client.get_resource_by_cursor(resource, cursor=cursor, page_size=self.default_page_size):
                if dicts:
                    self._insert_resource_data(db, resource, dicts)

                # persist progress
                db.aql.execute(
                    """
                    UPDATE @key WITH { from: @start_from } IN params
                    """,
                    bind_vars={"key": params_key, "start_from": cursor},
                )
                logging.info("%sInserted %d records for %s. Next cursor: %s", color, len(dicts) if dicts else 0, resource, cursor)

                if cursor is None:
                    logging.info("%sNo more data for %s. Marking as filled.", color, resource)
                    db.aql.execute(
                        """
                        UPDATE @key WITH { from: @start_from, fill: @fill } IN params
                        """,
                        bind_vars={"key": params_key, "start_from": None, "fill": False},
                    )
                    break
            return

        # Pipeline mode: producer fetches cursor sequentially; consumer transforms+writes.
        q: Queue[Optional[tuple[Optional[str], list[Dict[str, Any]]]]] = Queue(maxsize=self.pipeline_queue_size)

        def producer():
            cursor = start_cursor
            for cursor, dicts in self.emson_client.get_resource_by_cursor(resource, cursor=cursor, page_size=self.default_page_size):
                if dicts:
                    q.put((cursor, list(dicts)))
                else:
                    # still update progress cursor even if page empty
                    q.put((cursor, []))

                if cursor is None:
                    break

            q.put(None)  # sentinel

        def consumer():
            while True:
                item = q.get()
                if item is None:
                    return
                cursor, dicts = item

                if dicts:
                    self._insert_resource_data(db, resource, dicts)

                # persist progress
                db.aql.execute(
                    """
                    UPDATE @key WITH { from: @start_from } IN params
                    """,
                    bind_vars={"key": params_key, "start_from": cursor},
                )

                if cursor is None:
                    db.aql.execute(
                        """
                        UPDATE @key WITH { from: @start_from, fill: @fill } IN params
                        """,
                        bind_vars={"key": params_key, "start_from": None, "fill": False},
                    )
                    return

        with ThreadPoolExecutor(max_workers=2) as ex:
            f_prod = ex.submit(producer)
            f_cons = ex.submit(consumer)
            f_prod.result()
            f_cons.result()

        logging.info("%sCompleted filling %s (pipeline mode).", color, resource)
        return

    # -----------------------
    # Fill using EMInfra (all other resources)
    # -----------------------
    def _fill_resource_using_em_infra(self, resource: str):
        color = colorama_table[resource]
        logging.info("%sFilling resource: %s", color, resource)

        db = self.factory.create_connection()
        params_key = f"fill_{resource}"
        params_collection = db.collection("params")

        # ensure params doc exists
        params_resource = params_collection.get(params_key)
        if params_resource is None:
            params_collection.insert({"_key": params_key, "fill": True, "from": None})
            params_resource = params_collection.get(params_key)

        if not params_resource.get("fill", True):
            logging.info("%sSkipping %s, already filled.", color, resource)
            return

        start_from = params_resource.get("from")
        page_size = self.default_page_size
        generator = self._select_eminfra_generator(resource, start_from, page_size)

        for cursor, dicts in generator:
            if dicts:
                self._insert_resource_data(db, resource, dicts)

            # persist progress
            db.aql.execute(
                """
                UPDATE @key WITH { from: @start_from } IN params
                """,
                bind_vars={"key": params_key, "start_from": cursor},
            )

            # debug count
            result = db.aql.execute(f"RETURN LENGTH({resource})")
            try:
                count = list(result)[0]
                logging.debug("%sTotal records in %s collection: %d", color, resource, count)
            except Exception:
                logging.debug("%sUnable to fetch count for %s", color, resource)

            logging.info("%sInserted %d records for %s. Next cursor: %s", color, len(dicts) if dicts else 0, resource, cursor)

            if cursor is None:
                logging.info("%sNo more data for %s. Marking as filled.", color, resource)
                db.aql.execute(
                    """
                    UPDATE @key WITH { from: @start_from, fill: @fill } IN params
                    """,
                    bind_vars={"key": params_key, "start_from": None, "fill": False},
                )
                return

    def _select_eminfra_generator(self, resource: str, start_from: Optional[str], page_size: int):
        """Return the appropriate EMInfra generator for the resource."""
        sf = cast(Optional[str], start_from)
        if resource in {ResourceEnum.agents.value, ResourceEnum.betrokkenerelaties.value}:
            # these require cursor-based iteration with contactInfo expansion
            return self.eminfra_client.get_resource_by_cursor(resource, sf, page_size, expansion_strings=["contactInfo"])
        if resource in {ResourceEnum.toezichtgroepen.value, ResourceEnum.identiteiten.value}:
            return self.eminfra_client.get_identity_resource_page(resource, page_size, sf)
        if resource == ResourceEnum.bestekken.value:
            return self.eminfra_client.get_resource_page("bestekrefs", page_size, sf)
        return self.eminfra_client.get_resource_page(resource, page_size, sf)

    # -----------------------
    # Data insertion and transformations
    # -----------------------
    def _insert_resource_data(self, db, resource: str, dicts: Iterable[Dict[str, Any]]):
        """
        Dispatch to registered handlers to insert or bulk-import records.
        This keeps behavior identical but separates concerns per resource.
        """
        if handler := self._resource_handlers.get(resource):
            handler(db, dicts)
        else:
            raise NotImplementedError(f"Resource '{resource}' not implemented for insertion.")

    # ----- per-resource handlers -----
    def _handle_assettypes(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("assettypes")
        docs = [
            {
                "_key": r["uuid"][:8],
                "uuid": r["uuid"],
                "naam": r["naam"],
                "label": r["afkorting"],
                "uri": r["uri"],
                "short_uri": r["korteUri"],
                "definitie": r.get("definitie"),
                "actief": r.get("actief"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_assets(self, db, dicts: Iterable[Dict[str, Any]]):
        self._insert_assets(db, dicts)

    def _handle_relatietypes(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("relatietypes")
        docs = [
            {
                "_key": r["uuid"][:4],
                "uuid": r["uuid"],
                "naam": r["naam"],
                "label": r.get("label"),
                "uri": r.get("uri"),
                "short": None if r.get("uri") is None else r["uri"].split("#")[-1],
                "definitie": r.get("definitie"),
                "actief": r.get("actief", True),
                "gericht": r.get("gericht"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_assetrelaties(self, db, dicts: Iterable[Dict[str, Any]]):
        self._insert_asset_relations(db, dicts)

    def _handle_agents(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("agents")
        docs = []
        for obj in dicts:
            try:
                obj = self._transform_keys(obj)
                obj["_key"] = obj.get("@id", "").split("/")[-1][:13]
                obj["uuid"] = obj.get("@id", "").split("/")[-1][:36]
                docs.append(obj)
            except Exception as e:
                logging.error("Error processing agent %s: %s", obj.get("@id", "unknown"), e)
                raise
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_betrokkenerelaties(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("betrokkenerelaties")
        docs = []
        for obj in dicts:
            try:
                obj = self._transform_keys(obj)
                obj["_key"] = obj.get("@id", "").split("/")[-1][:36]

                bron = obj["RelatieObject_bron"]
                doel = obj["RelatieObject_doel"]

                if bron["@type"] == "http://purl.org/dc/terms/Agent":
                    obj["_from"] = "agents/" + bron.get("@id", "").split("/")[-1][:13]
                else:
                    obj["_from"] = "assets/" + bron.get("@id", "").split("/")[-1][:36]

                obj["_to"] = "agents/" + doel.get("@id", "").split("/")[-1][:13]

                if "AIMDBStatus_isActief" not in obj:
                    obj["AIMDBStatus_isActief"] = True

                if "HeeftBetrokkene_rol" in obj and "/" in obj["HeeftBetrokkene_rol"]:
                    obj["rol"] = obj["HeeftBetrokkene_rol"].split("/")[-1]
                docs.append(obj)
            except Exception as e:
                logging.error("Error processing betrokkenrelatie %s: %s", obj.get("@id", "unknown"), e)
                raise
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_toezichtgroepen(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("toezichtgroepen")
        docs = [
            {
                "_key": r["uuid"][:8],
                "uuid": r["uuid"],
                "naam": r["naam"],
                "actiefInterval": r.get("actiefInterval"),
                "actief": self.actief_interval_to_actief(r.get("actiefInterval")),
                "contactFiche": r.get("contactFiche"),
                "omschrijving": r.get("omschrijving"),
                "type": r.get("_type"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_identiteiten(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("identiteiten")
        docs = [
            {
                "_key": r["uuid"][:8],
                "uuid": r["uuid"],
                "type": r.get("_type"),
                "naam": r.get("naam"),
                "voornaam": r.get("voornaam"),
                "gebruikersnaam": r.get("gebruikersnaam"),
                "systeem": r.get("systeem"),
                "voId": r.get("voId"),
                "bron": r.get("bron"),
                "actief": r.get("actief"),
                "contactFiche": r.get("contactFiche"),
                "gebruikersrechtOrganisaties": r.get("gebruikersrechtOrganisaties"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_beheerders(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("beheerders")
        docs = [
            {
                "_key": r["uuid"][:8],
                "uuid": r["uuid"],
                "type": r.get("_type"),
                "naam": r.get("naam"),
                "referentie": r.get("referentie"),
                "actiefInterval": r.get("actiefInterval"),
                "actief": self.actief_interval_to_actief(r.get("actiefInterval")),
                "contactFiche": r.get("contactFiche"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    def _handle_bestekken(self, db, dicts: Iterable[Dict[str, Any]]):
        collection = db.collection("bestekken")
        docs = [
            {
                "_key": r["uuid"][:8],
                "uuid": r["uuid"],
                "type": r.get("type"),
                "awvId": r.get("awvId"),
                "eDeltaDossiernummer": r.get("eDeltaDossiernummer"),
                "eDeltaBesteknummer": r.get("eDeltaBesteknummer"),
                "aannemerNaam": r.get("aannemerNaam"),
                "aannemerReferentie": r.get("aannemerReferentie"),
                "actief": r.get("actief"),
            }
            for r in dicts
        ]
        if docs:
            collection.import_bulk(docs, overwrite=False, on_duplicate="update")

    # ----- complex handlers reused from previous refactor -----
    def _insert_assets(self, db, dicts: Iterable[Dict[str, Any]]):
        """Transform and upsert assets (+ derived bestekkoppelingen).

        Contract (kept stable):
        - Keys are normalized (namespace buckets + '.' -> '_', recursively)
        - Minimal derived fields are added on the asset document:
          - `_key`, `assettype_key`
          - `wkt`, `geometry` (if geometry is present)
          - `toestand`, `naampad_parts`, `naampad_parent`
          - `toezichtgroep_key`, `toezichter_key`, `beheerder_key`
        - Bestek koppelingen are written as edges in `bestekkoppelingen` with `_from/_to`.

        Notes:
        - This is the hottest part of the pipeline. Keep allocations low and flush in chunks.
        """
        collection = db.collection("assets")
        kopp_collection = db.collection("bestekkoppelingen")

        # lazy-load lookups
        if self.assettype_lookup is None:
            self.assettype_lookup = {at["uri"]: at["_key"] for at in db.collection("assettypes")}
        if self.beheerders_lookup is None:
            self.beheerders_lookup = {b["referentie"]: b["_key"] for b in db.collection("beheerders")}

        docs_batch: List[Dict[str, Any]] = []
        kopp_batch: List[Dict[str, Any]] = []

        unknown_type_uris = 0

        def flush_batches():
            if docs_batch:
                collection.import_bulk(docs_batch, overwrite=False, on_duplicate="update")
                docs_batch.clear()
            if kopp_batch:
                kopp_collection.import_bulk(kopp_batch, overwrite=False, on_duplicate="update")
                kopp_batch.clear()

        for raw in dicts:
            try:
                # assets-hotpath: do top-level namespace bucketing + '.'→'_' first
                obj = self._normalize_asset_top_level_keys(raw)

                # Generic namespace cleanup: recursively normalize all top-level buckets (dict/list payloads)
                # This ensures namespaces like tz/loc/bs/ins/ond/vtc/geo/... all behave consistently:
                # - remove nested "ns:" prefixes
                # - replace '.' with '_'
                for k, v in list(obj.items()):
                    if k.startswith("@"):  # keep metadata as-is
                        continue
                    if isinstance(v, (dict, list)):
                        obj[k] = self._normalize_nested_keys(v)

                uri = obj.get("@type")
                obj["_key"] = obj.get("@id", "").split("/")[-1][:36]

                # assettype resolution (skip unknown types)
                assettype_key = self.assettype_lookup.get(uri)
                if not assettype_key:
                    unknown_type_uris += 1
                    continue
                obj["assettype_key"] = assettype_key

                self._enrich_geometry(obj)
                self._enrich_state_and_naampad(obj)
                self._enrich_toezicht_keys(obj)
                self._collect_bestekkoppelingen(obj, kopp_batch)

                docs_batch.append(obj)
                if len(docs_batch) >= ASSET_IMPORT_CHUNK_SIZE:
                    flush_batches()

            except Exception as e:
                logging.error("Error processing asset %s: %s", raw.get("@id", "unknown"), e)
                raise

        flush_batches()

        if unknown_type_uris:
            logging.warning("Skipped %d asset(s) with unknown @type (missing in assettypes lookup).", unknown_type_uris)

    def _enrich_geometry(self, obj: Dict[str, Any]) -> None:
        """If a WKT geometry is present, add `wkt` and a GeoJSON `geometry` field."""
        wkt_string = self._extract_wkt_from_obj(obj)
        if not wkt_string:
            return

        obj["wkt"] = wkt_string

        # fast-path for typical POINTs
        geojson = self._fast_point_wgs84_from_wkt3812(wkt_string, self.transformer)
        if geojson is None:
            # fallback to shapely for complex geometry
            w = wkt_string
            if w.upper().startswith("SRID="):
                w = w.split(";", 1)[1]
            geom = wkt.loads(w)
            geom_wgs84 = transform(self.transformer.transform, geom)
            geojson = mapping(geom_wgs84)
            if geojson.get("type") == "Point" and len(geojson.get("coordinates", [])) >= 3:
                geojson["coordinates"] = geojson["coordinates"][:2]

        obj["geometry"] = geojson

    @staticmethod
    def _enrich_state_and_naampad(obj: Dict[str, Any]) -> None:
        """Add convenience fields derived from toestand + naampad."""
        if toestand := obj.get("AIMToestand_toestand"):
            obj["toestand"] = toestand.split("/")[-1]

        if naampad := obj.get("NaampadObject_naampad"):
            parts = naampad.split("/")
            obj["naampad_parts"] = parts
            if len(parts) >= 2:
                obj["naampad_parent"] = "/".join(parts[:-1])

    def _enrich_toezicht_keys(self, obj: Dict[str, Any]) -> None:
        """Derive shortcut keys used elsewhere in queries/reports.

        - toezichtgroep_key comes from tz->Toezicht_toezichtgroep->DtcToezichtGroep_id
        - toezichter_key comes from tz->Toezicht_toezichter->DtcToezichter_id
        - beheerder_key maps tz->Schadebeheerder_schadebeheerder->DtcBeheerder_referentie
        """
        tzg = obj.get("tz")
        if not isinstance(tzg, dict):
            return

        tg_id = tzg.get("Toezicht_toezichtgroep", {}).get("DtcToezichtGroep_id")
        if tg_id:
            obj["toezichtgroep_key"] = tg_id[:8]

        toez_id = tzg.get("Toezicht_toezichter", {}).get("DtcToezichter_id")
        if toez_id:
            obj["toezichter_key"] = toez_id[:8]

        sb_ref = tzg.get("Schadebeheerder_schadebeheerder", {}).get("DtcBeheerder_referentie")
        if sb_ref:
            sb_key = self.beheerders_lookup.get(sb_ref) if self.beheerders_lookup else None
            if sb_key:
                obj["beheerder_key"] = sb_key

    def _collect_bestekkoppelingen(self, obj: Dict[str, Any], kopp_batch: List[Dict[str, Any]]) -> None:
        """Convert bestekkoppelingen on the asset into edge documents.

        Important: we do not normalise here. `obj['bs']` is already recursively normalised.
        """
        bestek_koppelingen = obj.get("bs", {}).get("Bestek_bestekkoppeling")
        if not bestek_koppelingen:
            return

        for koppeling in bestek_koppelingen:
            koppeling["_from"] = "assets/" + obj["_key"]
            koppeling["_to"] = "bestekken/" + koppeling["DtcBestekkoppeling_bestekId"].get(
                "DtcIdentificator_identificator"
            )[:8]
            koppeling["_key"] = str(uuid.uuid4())

            # Keep legacy behavior: store last URI segment (or None)
            status_uri = koppeling.get("status")
            koppeling["status"] = status_uri.split("/")[-1] if status_uri else None

            kopp_batch.append(koppeling)

    # -----------------------
    # Utilities
    # -----------------------
    @staticmethod
    def to_short_uri(object_uri: str) -> str:
        """Convert a full URI to a short prefixed form. Mirrors original logic."""
        if object_uri == "http://purl.org/dc/terms/Agent":
            return "dcmi:Agent"
        # try common split pattern first
        if "/ns/" in object_uri:
            shorter_uri = object_uri.split("/ns/")[1]
            if object_uri.startswith("https://wegenenverkeer."):
                return shorter_uri
            prefix = object_uri.split("://", 1)[1].split(".")[0]
            return f"{prefix}:{shorter_uri}"
        # fallback: return the original as-is
        return object_uri

    @staticmethod
    def _transform_keys(data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform keys.

        Semantics (must remain identical):
        - At top-level (depth==0): split namespace 'ns:field' into nested dict: result['ns'][field] = value
        - Replace '.' with '_' in field names
        - Recurses inside lists and dicts
        """

        def process(obj: Any, depth: int = 0) -> Any:
            # fast paths
            if isinstance(obj, list):
                return [process(i, depth) for i in obj]
            if not isinstance(obj, dict):
                return obj

            result: Dict[str, Any] = {}

            for key, value in obj.items():
                value = process(value, depth + 1)

                if depth == 0:
                    colon = key.find(":")
                    if colon != -1:
                        ns = key[:colon]
                        field = key[colon + 1 :]
                        if "." in field:
                            field = field.replace(".", "_")
                        bucket = result.get(ns)
                        if bucket is None:
                            bucket = {}
                            result[ns] = bucket
                        bucket[field] = value
                        continue

                    # no namespace key at top-level
                    if "." in key:
                        result[key.replace(".", "_")] = value
                    else:
                        result[key] = value
                    continue

                # nested levels: drop namespace prefix if present, replace '.'
                colon = key.find(":")
                clean_key = key[colon + 1 :] if colon != -1 else key
                if "." in clean_key:
                    clean_key = clean_key.replace(".", "_")
                result[clean_key] = value

            return result

        return process(data)

    @staticmethod
    def _normalize_nested_keys(obj: Any) -> Any:
        """Normalize keys recursively for nested dicts/lists.

        This is like the *nested* branch of `_transform_keys`:
        - remove namespace prefix if present (split on the first ':')
        - replace '.' with '_'
        - recurse into dict/list

        Important: unlike `_transform_keys` it does NOT create namespace buckets at the top level.
        """
        if isinstance(obj, list):
            # Fast path: if the list has no dict/list children, nothing to normalize
            if not any(isinstance(i, (dict, list)) for i in obj):
                return obj
            return [InitialFillStep._normalize_nested_keys(i) for i in obj]

        if not isinstance(obj, dict):
            return obj

        # Fast path: if no key contains ':' or '.' AND there are no nested containers, return as-is
        needs_key_change = False
        has_child_containers = False
        for k, v in obj.items():
            if (':' in k) or ('.' in k):
                needs_key_change = True
                break
            if isinstance(v, (dict, list)):
                has_child_containers = True

        if not needs_key_change and not has_child_containers:
            return obj

        out: Dict[str, Any] = {}
        for k, v in obj.items():
            colon = k.find(":")
            k2 = k[colon + 1 :] if colon != -1 else k
            if "." in k2:
                k2 = k2.replace(".", "_")
            out[k2] = InitialFillStep._normalize_nested_keys(v)
        return out

    @staticmethod
    def _extract_wkt_from_obj(obj: Dict[str, Any]) -> Optional[str]:
        """Extract WKT string from known locations in asset object."""
        # emson format with geo.Geometrie_log -> DtcLog_geometrie -> first value
        if "geo" in obj and obj["geo"].get("Geometrie_log"):
            geometrie_dict = obj["geo"]["Geometrie_log"][0].get("DtcLog_geometrie")
            if geometrie_dict:
                return next(iter(geometrie_dict.values()), None)

        # location-based entries
        if "loc" in obj:
            loc = obj["loc"]
            if loc.get("Locatie_geometrie"):
                val = loc.get("Locatie_geometrie")
                if val:
                    return val
            pl = loc.get("Locatie_puntlocatie")
            if pl and pl != "":
                geom_container = pl.get("3Dpunt_puntgeometrie")
                if geom_container and geom_container != "":
                    if 'DtcCoord.lambert72' in geom_container:
                        coords = geom_container['DtcCoord.lambert72']
                        wkt_string = f"POINT Z ({coords['DtcCoordLambert72.xcoordinaat']} {coords['DtcCoordLambert72.ycoordinaat']} {coords['DtcCoordLambert72.zcoordinaat']})"
                    elif 'DtcCoord.lambert2008' in geom_container:
                        coords = geom_container['DtcCoord.lambert2008']
                        wkt_string = f"POINT Z ({coords['DtcCoordLambert2008.xcoordinaat']} {coords['DtcCoordLambert2008.ycoordinaat']} {coords['DtcCoordLambert2008.zcoordinaat']})"
                    else:
                        logging.error(f"Unknown geometry type: {geom_container}")
                        return None
                    return wkt_string
        return None

    def actief_interval_to_actief(self, actief_interval: Optional[Dict[str, Any]]) -> bool:
        """Return True when the interval indicates active now, else False."""
        if not actief_interval:
            return False
        van = actief_interval.get("van")
        tot = actief_interval.get("tot")
        if van is None:
            return False
        van_date = datetime.fromisoformat(van).astimezone(timezone.utc)
        now = datetime.now(timezone.utc)
        if van_date < now:
            if tot is None:
                return True
            tot_date = datetime.fromisoformat(tot).astimezone(timezone.utc)
            if tot_date > now:
                return True
        return False

    # -----------------------
    # Cleanup
    # -----------------------
    def _removed_fill_params(self, db):
        """Remove all params docs that start with 'fill_'."""
        db.aql.execute(
            """
            FOR doc IN params
                FILTER LIKE(doc._key, "fill_%", true)
                REMOVE doc IN params
            """
        )

    @staticmethod
    def _normalize_asset_top_level_keys(raw: Dict[str, Any]) -> Dict[str, Any]:
        """Faster key normalization for assets.

        For assets we need the same *top-level* semantics as `_transform_keys` (namespace grouping + '.'→'_'),
        but we avoid deep recursion for performance.

        Rules:
        - keep '@id' and '@type' verbatim
        - top-level 'ns:field' -> out['ns'][field] = value (field also '.'→'_')
        - otherwise, replace '.' with '_' in the key
        - nested dicts/lists are left as-is (we access them through their container namespaces)
        """
        out: Dict[str, Any] = {}
        for k, v in raw.items():
            if not k or k.startswith("@"):
                out[k] = v
                continue

            colon = k.find(":")
            if colon != -1:
                ns = k[:colon]
                field = k[colon + 1 :]
                if "." in field:
                    field = field.replace(".", "_")
                bucket = out.get(ns)
                if bucket is None or not isinstance(bucket, dict):
                    bucket = {}
                    out[ns] = bucket
                bucket[field] = v
                continue

            if "." in k:
                out[k.replace(".", "_")] = v
            else:
                out[k] = v
        return out

    @staticmethod
    def _fast_point_wgs84_from_wkt3812(wkt_string: str, transformer: Transformer) -> Optional[Dict[str, Any]]:
        """Fast-path for common POINT/POINT Z WKT in EPSG:3812.

        Returns a GeoJSON dict or None when it can't parse fast.
        """
        s = wkt_string.strip()
        if s.upper().startswith("SRID="):
            # SRID=3812;POINT Z(...)
            try:
                s = s.split(";", 1)[1].strip()
            except Exception:
                return None

        up = s.upper()
        if not up.startswith("POINT"):
            return None

        # Support: POINT( x y ) and POINT Z( x y z ) and POINT Z (x y z)
        try:
            start = s.find("(")
            end = s.rfind(")")
            if start == -1 or end == -1 or end <= start:
                return None
            nums = s[start + 1 : end].replace(",", " ").split()
            if len(nums) < 2:
                return None
            x = float(nums[0])
            y = float(nums[1])
            lon, lat = transformer.transform(x, y)
            return {"type": "Point", "coordinates": [lon, lat]}
        except Exception:
            return None
