import logging
import time
import uuid
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple, Callable

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


class InitialFillStep:
    def __init__(self, factory, eminfra_client: EMInfraClient, emson_client: EMSONClient,
                 page_size: int = DEFAULT_PAGE_SIZE):
        self.factory = factory
        self.eminfra_client = eminfra_client
        self.emson_client = emson_client
        self.default_page_size = page_size

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

        cursor = params_resource.get("from")
        # emson client yields (cursor, dicts)
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
        if resource in {ResourceEnum.agents.value, ResourceEnum.betrokkenerelaties.value}:
            # these require cursor-based iteration with contactInfo expansion
            return self.eminfra_client.get_resource_by_cursor(resource, start_from, page_size, expansion_strings=["contactInfo"])
        if resource in {ResourceEnum.toezichtgroepen.value, ResourceEnum.identiteiten.value}:
            return self.eminfra_client.get_identity_resource_page(resource, page_size, start_from)
        if resource == ResourceEnum.bestekken.value:
            return self.eminfra_client.get_resource_page("bestekrefs", page_size, start_from)
        return self.eminfra_client.get_resource_page(resource, page_size, start_from)

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
        """Process and import asset records (large, complex transformation)."""
        collection = db.collection("assets")
        kopp_collection = db.collection("bestekkoppelingen")

        # lazy-load lookups
        if self.assettype_lookup is None:
            self.assettype_lookup = {at["uri"]: at["_key"] for at in db.collection("assettypes")}
        if self.beheerders_lookup is None:
            self.beheerders_lookup = {b["referentie"]: b["_key"] for b in db.collection("beheerders")}

        docs_to_insert = []
        docs2_to_insert = []

        for raw in dicts:
            try:
                obj = self._transform_keys(raw)
                uri = obj.get("@type")
                obj["_key"] = obj.get("@id", "").split("/")[-1][:36]

                # extract WKT from several possible locations
                wkt_string = self._extract_wkt_from_obj(obj)
                if wkt_string:
                    full_wkt_string = wkt_string
                    obj["wkt"] = full_wkt_string
                    if full_wkt_string.upper().startswith("SRID="):
                        wkt_string = full_wkt_string.split(";")[1]
                    geom = wkt.loads(wkt_string)
                    geom_wgs84 = transform(self.transformer.transform, geom)
                    geojson = mapping(geom_wgs84)
                    if geojson.get("type") == "Point" and len(geojson.get("coordinates", [])) >= 3:
                        geojson["coordinates"] = geojson["coordinates"][:2]
                    obj["geometry"] = geojson

                # assettype resolution
                if (assettype_key := self.assettype_lookup.get(uri)):
                    obj["assettype_key"] = assettype_key
                else:
                    print(f"⚠️ No matching assettype for URI: {uri}")
                    continue
                    wkt_string = None
                    # extract wkt string
                    if 'geo' in obj and obj['geo']['Geometrie_log']:
                        geometrie_dict = obj['geo']['Geometrie_log'][0]['DtcLog_geometrie']
                        wkt_string = next(iter(geometrie_dict.values()))

                    elif 'loc' in obj:
                        if 'Locatie_geometrie' in obj['loc'] and obj['loc']['Locatie_geometrie'] != '':
                            wkt_string = obj['loc']['Locatie_geometrie']
                        elif 'Locatie_puntlocatie' in obj['loc'] and obj['loc']['Locatie_puntlocatie'] != '' and '3Dpunt_puntgeometrie' in obj['loc']['Locatie_puntlocatie'] and obj['loc']['Locatie_puntlocatie']['3Dpunt_puntgeometrie'] != '':
                            coords = obj['loc']['Locatie_puntlocatie']['3Dpunt_puntgeometrie']
                            if 'DtcCoord.lambert72' in coords:
                                coords = coords['DtcCoord.lambert72']
                                wkt_string = f"POINT Z ({coords['DtcCoordLambert72.xcoordinaat']} {coords['DtcCoordLambert72.ycoordinaat']} {coords['DtcCoordLambert72.zcoordinaat']})"
                            else:
                                coords = coords['DtcCoordLambert2008']
                                wkt_string = f"POINT Z ({coords['DtcCoordLambert2008.xcoordinaat']} {coords['DtcCoordLambert2008.ycoordinaat']} {coords['DtcCoordLambert2008.zcoordinaat']})"

                    if wkt_string is not None:
                        full_wkt_string = wkt_string
                        if wkt_string.upper().startswith("SRID="):
                            srid_part, wkt_string = wkt_string.split(";", 1)
                        geom = wkt.loads(wkt_string)

                        obj['wkt'] = full_wkt_string
                        geom_wgs84 = transform(self.transformer.transform, geom)
                        geojson = mapping(geom_wgs84)
                        # Trim Z coordinate only for Points
                        if geojson.get("type") == "Point" and len(geojson.get("coordinates", [])) >= 3:
                            geojson["coordinates"] = geojson["coordinates"][:2]
                        obj['geometry'] = geojson

                    if assettype_key := self.assettype_lookup.get(uri):
                        obj["assettype_key"] = assettype_key
                    else:
                        print(f"⚠️ No matching assettype for URI: {uri}")
                        continue

                # optional mappings
                if toestand := obj.get("AIMToestand_toestand"):
                    obj["toestand"] = toestand.split("/")[-1]

                if naampad := obj.get("NaampadObject_naampad"):
                    obj["naampad_parts"] = naampad.split("/")
                    if len(obj["naampad_parts"]) >= 2:
                        obj["naampad_parent"] = "/".join(obj["naampad_parts"][:-1])

                tzg_id = obj.get("tz", {}).get("Toezicht_toezichtgroep", {}).get("DtcToezichtGroep_id")
                if tzg_id:
                    obj["toezichtgroep_key"] = tzg_id[:8]

                tz_id = obj.get("tz", {}).get("Toezicht_toezichter", {}).get("DtcToezichter_id")
                if tz_id:
                    obj["toezichter_key"] = tz_id[:8]

                sb_ref = obj.get("tz", {}).get("Schadebeheerder_schadebeheerder", {}).get("DtcBeheerder_referentie")
                if sb_ref and (sb_key := self.beheerders_lookup.get(sb_ref)):
                    obj["beheerder_key"] = sb_key

                # bestek koppelingen
                if "bs" in obj and "Bestek_bestekkoppeling" in obj["bs"] and obj["bs"]["Bestek_bestekkoppeling"]:
                    bestek_koppelingen = obj["bs"]["Bestek_bestekkoppeling"]
                    for koppeling in bestek_koppelingen:
                        koppeling["_from"] = "assets/" + obj["_key"]
                        koppeling["_to"] = "bestekken/" + koppeling["DtcBestekkoppeling_bestekId"].get("DtcIdentificator_identificator")[:8]
                        koppeling["_key"] = str(uuid.uuid4())
                        koppeling["status"] = koppeling.get("status").split("/")[-1] if koppeling.get("status") else None
                        docs2_to_insert.append(koppeling)

                docs_to_insert.append(obj)
            except Exception as e:
                logging.error("Error processing asset %s: %s", raw.get("@id", "unknown"), e)
                raise

        if docs_to_insert:
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")
        if docs2_to_insert:
            kopp_collection.import_bulk(docs2_to_insert, overwrite=False, on_duplicate="update")

    def _insert_asset_relations(self, db, dicts: Iterable[Dict[str, Any]]):
        """Insert asset relations resolving relatietype lookup lazily."""
        collection = db.collection("assetrelaties")
        if self.relatietype_lookup is None:
            self.relatietype_lookup = {rt["uri"]: rt["_key"] for rt in db.collection("relatietypes")}

        docs_to_insert = []
        for raw in dicts:
            try:
                obj = self._transform_keys(raw)
                uri = obj.get("@type")
                obj["_key"] = obj.get("@id", "").split("/")[-1][:36]
                obj["_from"] = "assets/" + obj["RelatieObject_bron"].get("@id", "").split("/")[-1][:36]
                obj["_to"] = "assets/" + obj["RelatieObject_doel"].get("@id", "").split("/")[-1][:36]
                if "AIMDBStatus_isActief" not in obj:
                    obj["AIMDBStatus_isActief"] = True

                if (relatietype_key := self.relatietype_lookup.get(uri)):
                    obj["relatietype_key"] = relatietype_key
                    docs_to_insert.append(obj)
                else:
                    print(f"⚠️ No matching relatietype for URI: {uri}")
            except Exception as e:
                logging.error("Error processing assetrelatie %s: %s", raw.get("@id", "unknown"), e)
                raise

        if docs_to_insert:
            collection.import_bulk(docs_to_insert, overwrite=False, on_duplicate="update")

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
        """
        Transform keys:
        - At top-level split namespace 'ns:field' into nested field: result['ns'][field] = value
        - Replace '.' with '_' in field names
        - Recurses inside lists and dicts
        """
        def process(obj: Any, depth: int = 0) -> Any:
            if not isinstance(obj, dict):
                return [process(i, depth) for i in obj] if isinstance(obj, list) else obj
            result: Dict[str, Any] = {}
            for key, value in obj.items():
                value = process(value, depth + 1)
                if depth == 0 and ":" in key:
                    ns, field = key.split(":", 1)
                    field = field.replace(".", "_")
                    if ns not in result:
                        result[ns] = {}
                    result[ns][field] = value
                else:
                    clean_key = key.split(":", 1)[-1].replace(".", "_")
                    result[clean_key] = value
            return result

        return process(data)

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
                        coords = geom_container['DtcCoordLambert2008']
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
