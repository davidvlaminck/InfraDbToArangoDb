# Database Model — Infra DB → ArangoDB

> **Authoritatief databasemodel** voor de ArangoDB-database (`infra_db`) zoals gevuld door deze repository.
> Dit document is de primaire referentie voor LLMs, agents en ontwikkelaars.
> Een visueel schema is beschikbaar in `datamodel.png`.

## Overzicht

De database is een **graf-model** in ArangoDB. Er zijn 10 document-collecties, 10 edge-collecties en 10 grafen.

### Collectieregels

- **`_key` afkorting**: UUID's worden vaak afgekort voor `_key` (bv. `uuid[:8]`). Het volledige UUID wordt steeds bewaard in een apart veld (`uuid`).
- **`AIMDBStatus_isActief`**: boolean; `true` = actief. Waar deze ontbreekt, wordt ervan uitgegaan dat het actief is.
- **Naampad**: afgeleid van `NaampadObject_naampad` door `InitialFillStep._enrich_state_and_naampad()` → `naampad_parts` (array) en `naampad_parent` (string).

---

## Document-collecties (vertices)

| Collectie | Bron | `_key` | Beschrijving |
|---|---|---|---|
| `params` | Intern | string | Pipeline-status, voortgang, feed-cursor |
| `assets` | EMSON | `uuid[:36]` | Alle infrastructuur-assets |
| `assettypes` | EMInfra | `uuid[:8]` | Type-definities voor assets |
| `relatietypes` | EMInfra | `uuid[:4]` | Type-definities voor relaties |
| `agents` | EMSON/EMInfra | `@id` segment `[:13]` | Agenten (personen/organisaties) |
| `toezichtgroepen` | EMInfra | `uuid[:8]` | Toezichtgroepen (supervisie) |
| `identiteiten` | EMInfra | `uuid[:8]` | Identiteiten (gebruikers/rechten) |
| `beheerders` | EMInfra | `uuid[:8]` | Beheerders (schadebeheerders) |
| `bestekken` | EMInfra | `uuid[:8]` | Bestekken (contracten) |
| `aansluitingrefs` | EMInfra (pending) | — | Aansluitingsreferenties (placeholder) |

### 1. `params`

Opslaan van pipeline-state. Elk document heeft een vaste `_key`.

| `_key` | Velden | Beschrijving |
|---|---|---|
| `feed_<resource>` | `page: int`, `event_uuid: str\|None` | Laatste verwerkte pagina/cursor per feed |
| `fill_<resource>` | `fill: bool`, `from: str\|None` | Voortgang per resource (tijdens vullen) |
| `db_step` | `value: str` | Huidige pipeline-stap (zie `Enums.DBStep`) |
| `finished_at` | `value: str` (ISO-8601) | Timestamp van voltooiing |

Standaard documents bij setup (`CreateDBStep.py:52-59`):
```
feed_assetrelaties, feed_betrokkenerelaties, feed_agents, feed_assets
```

### 2. `assets`

Hoofdcollectie; documenten komen van EMSON (`core/api/otl/assets/search`).

**Sleutelvelden (top-level, na normalisatie):**

|veld|type|afkomst|bere description|
|---|---|---|---|
|`_key`|str|`@id` laatste segment `[:36]`| Unieke asset-ID (UUID) |
|`_id`|str|`assets/<_key>`| Arango interne ID |
|`@type`|str|origineel| Volledige URI van het assettype |
|`@id`|str|origineel| Volledige URI van het asset |
|`assettype_key`|str → `assettypes._key`|opgelost via `assettype_lookup`| FK naar assettypes |
|`AIMDBStatus_isActijf`|bool|naamloos| Actief (altijd `true` bij insert, tenzij aanwezig) |
|`wkt`|str|geo/loc| WKT-representatie (Lambert 2008 / EPSG:3812) |
|`geometry`|dict|geo/loc| GeoJSON Point (WGS84, EPSG:4326) |
|`toestand`|str|afgeleid van `AIMToestand_toestand`| Laatste pathsegment (bv. `in-gebruik`, `verwijderd`) |
|`naampad_parts`|list[str]|afgeleid van `NaampadObject_naampad`| Pad gesplitst op `/` |
|`naampad_parent`|str|afgeleid van `naampad_parts`| Alles behalve laatste segment |
|`toezichtgroep_key`|str → `toezichtgroepen._key`|`tz.Toezicht_toezichtgroep.DtcToezichtGroep_id[:8]`| FK |
|`toezichter_key`|str → `identiteiten._key`|`tz.Toezicht_toezichter.DtcToezichter_id[:8]`| FK |
|`beheerder_key`|str → `beheerders._key`|via `beheerders_lookup` op `DtcBeheerder_referentie`| FK |

**Namespace-buckets (top-level, na `_transform_keys` normalisatie):**

| Bucket | Voorbeeld velden | Beschrijving |
|---|---|---|
| `tz` | `Toezicht_toezichtgroep`, `Toezicht_toezichter`, `Schadebeheerder_schadebeheerder` | Toezicht/instantie relaties |
| `bs` | `Bestek_bestekkoppeling` | Bestekkoppelingen (→ `bestekkoppelingen` edges) |
| `loc` | `Locatie_geometrie`, `Locatie_puntlocatie` | Locatie-geometrie |
| `geo` | `Geometrie_log` | WKT-geometrie |
| `ins` | `ElektrischeKeuring_resultaat`, `KeuringObject_keuringsdatum` | Keuringsinstantie data |
| `ond` | — | Onderdeel-specifieke attributen |
| `vtc` | — | Asset-type specifieke attributen |

**Indexen (`CreateIndicesStep.py:18-34`):**
- `assettype_key` (persistent)
- `toezichter_key` (persistent)
- `toezichtgroep_key` (persistent)
- `beheerder_key` (persistent)
- `naampad_parts` (persistent, sparse)
- `naampad_parent` (persistent, sparse)
- `NaampadObject_naampad` (persistent, sparse)
- `assettype_key, AIMDBStatus_isActief` (persistent, compound)
- `assettype_key, AIMDBStatus_isActief, toestand` (persistent, compound)
- `geometry` (geo)

### 3. `assettypes`

Asset-type definitie. Veldnamen komen van EMInfra (`core/api/assettypes`).

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:8]`|
|`uuid`|str|Volledige UUID|
|`naam`|str|Lange naam|
|`label`|str|`afkorting` (interne afkorting)|
|`uri`|str|Volledige URI (bv. `https://lgc.data.wegenenverkeer.be/ns/installatie#Laagspanningsbord`)|
|`short_uri`|str|`korteUri` (bv. `onderdeel#Laagspanningsbord`)|
|`definitie`|str|Definitie tekst|
|`actief`|bool|Actief|
|`aansluitpunt_kenmerk`|bool|Afgeleid (`ExtraFillStep.fill_assettypes`) — heeft `Elektrisch aansluitpunt` kenmerk?|

Indexen: `short_uri` (persistent), `uri` (persistent)

### 4. `relatietypes`

Relatie-type definitie.

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:4]`|
|`uuid`|str|Volledige UUID|
|`naam`|str|Lange naam (bv. `Voedt`)|
|`label`|str|Label|
|`uri`|str|Volledige URI|
|`short`|str|`uri.split("#")[-1]` (bv. `Voedt`, `Sturing`, `Bevestiging`, `HoortBij`, `HeeftKeuring`, `GemigreerdNaar`)|
|`definitie`|str|Definitie|
|`actief`|bool|Standaard `true`|
|`gericht`|bool|Gericht (directioneel)|

Index: `short` (persistent)

### 5. `agents`

Agenten (personen/organisaties). Veldnamen komen van EMSON (`core/api/otl/agents/search` met expansie `contactInfo`).

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`@id` laatste segment `[:13]`|
|`uuid`|str|`@id` laatste segment `[:36]` (volledige UUID)|
|`naam`|str|Naam|
|`purl`|dict|Naamruimte-bucket (`http://purl.org/dc/terms/Agent` → `purl`)|
|`type`|str|Agent-type|

Indexen: `AIMDBStatus_isActief` (persistent), `purl.Agent_naam` (persistent, sparse)

### 6. `toezichtgroepen`

Toezichtgroepen (supervisie-groepen). Veldnamen komen van EMInfra (`core/api/toezichtgroepen`).

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:8]`|
|`uuid`|str|Volledige UUID|
|`naam`|str|Naam (bv. `V&W-WL`, `V&W-WA`, `V&W-WO`, `V&W-WW`, `V&W-WVB`)|
|`actiefInterval`|dict|`{van: str, tot: str}` (ISO-8601)|
|`actief`|bool|Afgeleid via `actief_interval_to_actief()`|
|`contactFiche`|dict|Contactgegevens|
|`omschrijving`|str|Omschrijving|
|`type`|str|Type|

### 7. `identiteiten`

Identiteiten (gebruikers). Veldnamen komen van EMInfra (`identiteit/api/identiteiten`).

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:8]`|
|`uuid`|str|Volledige UUID|
|`type`|str|Type|
|`naam`|str|Achternaam|
|`voornaam`|str|Voornaam|
|`gebruikersnaam`|str|Gebruikersnaam|
|`systeem`|bool|Systeem-gebruiker?|
|`voId`|str|Vo-Id|
|`bron`|str|Bron|
|`actief`|bool|Actief|
|`contactFiche`|dict|Contactgegevens|
|`gebruikersrechtOrganisaties`|list|Rechtenorganisaties|

### 8. `beheerders`

Beheerders (schadebeheerders). Veldnamen komen van EMInfra.

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:8]`|
|`uuid`|str|Volledige UUID|
|`type`|str|Type|
|`naam`|str|Naam|
|`referentie`|str|Referentie (gebruikt voor op te zoeken via `beheerders_lookup`)|
|`actiefInterval`|dict|`{van: str, tot: str}`|
|`actief`|bool|Afgeleid|
|`contactFiche`|dict|Contactgegevens|

### 9. `bestekken`

Bestekken (contracten). Veldnamen komen van EMInfra (`core/api/bestekrefs`).

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid[:8]`|
|`uuid`|str|Volledige UUID|
|`type`|str|Type|
|`awvId`|str|AWV-ID|
|`eDeltaDossiernummer`|str|eDelta dossiernummer|
|`eDeltaBesteknummer`|str|eDelta besteknummer|
|`aannemerNaam`|str|Aannemer naam|
|`aannemerReferentie`|str|Aannemer referentie|
|`actief`|bool|Actief|

### 10. `aansluitingrefs`

Aansluitingsreferenties. Placeholder in `ExtraFillStep.py:124-127`. Nog geen datavelden gedefinieerd.

---

## Edge-collecties (edges)

| Collectie | From-vertex | To-vertex | Bron | Type | Beschrijving |
|---|---|---|---|---|---|
| `assetrelaties` | `assets` | `assets` | EMSON | edge | Asset-Asset relaties |
| `betrokkenerelaties` | `assets` + `agents` | `agents` | EMSON | edge | Agent-Asset betrokkenis |
| `bestekkoppelingen` | `assets` | `bestekken` | afgeleid | edge | Bestekkoppelingen per asset |
| `aansluitingen` | `assets` | `aansluitingrefs` | EMInfra (pending) | edge | Aansluitingen (placeholder) |
| `voedt_relaties` | `assets` | `assets` | afgeleid | edge | `Voedt` relaties (geëxtraheerd uit assetrelaties) |
| `sturing_relaties` | `assets` | `assets` | afgeleid | edge- | `Sturing` relaties |
| `bevestiging_relaties` | `assets` | `assets` | afgeleid | edge | `Bevestiging` relaties |
| `hoortbij_relaties` | `assets` | `assets` | afgeleid | edge | `HoortBij` relaties |
| `heeftkeuring_relaties` | `assets` | `assets` | afgeleid | edge | `HeeftKeuring` relaties |
| `gemigreerdnaar_relaties` | `assets` | `assets` | afgeleid | edge | `GemigreerdNaar` relaties |

### Gedetailleerde velden per edge

**assetrelaties** (`InitialFillStep._insert_asset_relations`, `ExtraFillStep._fill_derived_edges`):

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`@id` laatste segment `[:36]`|
|`_from`|str|`assets/<_key>`|
|`_to`|str|`assets/<_key>`|
|`relatietype_key`|str → `relatietypes._key`| FK naar relatietype (opgelost via `relatietype_lookup`) |
|`@type`|str|URI van het relatietype|
|`AIMDBStatus_isActief`|bool|Standaard `true`|

Indexen: `relatietype_key` (persistent), `relatietype_key, AIMDBStatus_isActief` (persistent, compound)

**betrokkenerelaties** (`InitialFillStep._handle_betrokkenerelaties`):

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`@id` laatste segment `[:36]`|
|`_from`|str|`agents/<@id[:13]>` of `assets/<@id[:36]>`| Afhankelijk van `bron[@type]`|
|`_to`|str|`agents/<@id[:13]>`|
|`rol`|str|Laatste pathsegment van `HeeftBetrokkene_rol` (bv. `toezichtsgroep`, `toezichter`, `schadebeheerder`)|
|`@type`|str|URI|
|`AIMDBStatus_isActief`|bool|Standaard `true`|

Indexen: `_from, rol` (persistent), `_to, rol` (persistent)

**bestekkoppelingen** (`InitialFillStep._collect_bestekkoppelingen`):

|veld|type|beschrijving|
|---|---|---|
|`_key`|str|`uuid4()` (willekeurig)|
|`_from`|str|`assets/<_key>`|
|`_to`|str|`bestekken/<bestekId[:8]>`|
|`volgnummer`|int|0-based positie in `bs.Bestek_bestekkoppeling` (bronvolgorde)|
|`DtcBestekkoppeling_bestekId`|dict|Identificator|
|`DtcBestekkoppeling_status`|str|Status URI|
|`status`|str|Afgeleid: laatste segment van `status` URI|

Index: `DtcBestekkoppeling_status` (persistent)

**Afgeleide edge-collecties** (`ExtraFillStep._fill_derived_edges`):

|veld|type|beschrijving|
|---|---|---|
|`_from`|str|`assets/<_key>`| Oorspronkelijke assetrelatie `_from` |
|`_to`|str|`assets/<_key>`| Oorspronkelijke assetrelatie `_to` |
|`source_edge_id`|str|`e._id`| Verwijzing naar bron-edge in `assetrelaties` |
|`source_edge_key`|str|`e._key`| Verwijzing naar bron-edge in `assetrelaties` |

Indexen per afgeleide collectie: `_from` (persistent), `_to` (persistent)

**Bouwlogica (`_fill_derived_edges`)**:
- Voor `GemigreerdNaar`: alleen actieve edges (`edge.AIMDBStatus_isActief == true`).
- Voor overige: alleen actieve edges **en** actieve, aanwezige endpoints (`a_from.AIMDBStatus_isActief == true` en `a_to.AIMDBStatus_isActief == true`).

---

## Graf-definities (`CreateIndicesStep.add_graphs`)

| Graf | Edge-collectie | From-verzamelingen | To-verzamelingen |
|---|---|---|---|
| `assetrelaties_graph` | `assetrelaties` | `assets` | `assets` |
| `voedt_graph` | `voedt_relaties` | `assets` | `assets` |
| `sturing_graph` | `sturing_relaties` | `assets` | `assets` |
| `bevestiging_graph` | `bevestiging_relaties` | `assets` | `assets` |
| `hoortbij_graph` | `hoortbij_relaties` | `assets` | `assets` |
| `heeftkeuring_graph` | `heeftkeuring_relaties` | `assets` | `assets` |
| `gemigreerdnaar_graph` | `gemigreerdnaar_relaties` | `assets` | `assets` |
| `betrokkenerelaties_graph` | `betrokkenerelaties` | `assets`, `agents` | `agents` |
| `bestekkoppelingen_graph` | `bestekkoppelingen` | `assets` | `bestekken` |
| `aansluitingen_graph` | `aansluitingen` | `assets` | `aansluitingrefs` |

---

## Pipeline-fases

| Stap | Actie | Verantwoordelijke klasse |
|---|---|---|
| `CREATE_DB` | Maak collecties + standaard `params`-documenten | `CreateDBStep` |
| `INITIAL_FILL` | Vul basisdata via EMSON/EMInfra cursor/paging | `InitialFillStep` |
| `EXTRA_DATA_FILL` | Vul extra velden + bouw afgeleide edges | `ExtraFillStep` |
| `CREATE_INDEXES` | Maak indexen + Grafen + ArangoSearch view | `CreateIndicesStep` |
| `APPLY_CONSTRAINTS` | (Momenteel leeg; geen extra actie) | `DBPipelineController` |
| `SYNC` | (Overgeslagen; DB vult snel genoeg) | `DBPipelineController` |
| `STOP` | Schrijf `finished_at` timestamp naar `params` | `DBPipelineController` |

---

## Assettypes (`short_uri`)

### Mechanisme

Elk `assets`-document heeft een veld `assettype_key` dat verwijst naar `assettypes._key`.  
Deze relatie wordt tijdens `InitialFillStep._insert_assets()` opgezocht via een in-memory lookup-tabel (`assettype_lookup`):

```python
# assettypes._key = uuid[:8]   (bijv. "1a2b3c4d")
# assets.assettype_key verwijst naar dit _key
self.assettype_lookup = {at["uri"]: at["_key"] for at in db.collection("assettypes")}
```

Het veld `short_uri` in `assettypes` is de **korte URI** van het assettype. Deze ontstaat uit `korteUri` in de EMInfra API-response. Voorbeeld:

- Volledige URI: `https://www.wegenenverkeer.be/otl/installatie#Laagspanningsbord`  
- `short_uri`: `onderdeel#Laagspanningsbord` (na namespace-Compressie door `to_short_uri()`)

In AQL-queries filtert men op `assettypes.short_uri`:
```aql
LET at_key = FIRST(FOR at IN assettypes FILTER at.short_uri == "onderdeel#Laagspanningsbord" LIMIT 1 RETURN at._key)
FOR a IN assets FILTER a.assettype_key == at_key
```

### Voorbeeld

| `short_uri` | OTL-entiteit | Gebruikt in |
|---|---|---|
| `onderdeel#Laagspanningsbord` | Laagspanningsbord (LSB) | LSB detail-export, TreeAnalysis |
| `installatie#Laagspanningsbord` | Laagspanningsbord (OTL-conform) | Keuringsexport |
| `installatie#LSDeel` | LSDeel (legacy) | Keuringsexport |
| `lgc:installatie#LS` | LS (legacy) | Keuringsexport |
| `onderdeel#ElektrischeKeuring` | Elektrische keuring | LSB detail-export |
| `onderdeel#HSCabine` | HSCabine | TreeAnalysis |
| `onderdeel#Wegkantkast` | Wegkantkast | TreeAnalysis |
| `onderdeel#DNBLaagspanning` | DNB laagspanning | TreeAnalysis |
| `lgc:installatie#VPLMast` | VPL Mast | TreeAnalysis |
| `onderdeel#Segmentcontroller` | Segmentcontroller | TreeAnalysis |
| `onderdeel#Verkeersregelaar` | Verkeersregelaar | TreeAnalysis |
| `installatie#Stroomkring` | Stroomkring | spec.md voorbeeldqueries |

## Veelgebruikte relatietypes (`short`)

| `short` | Beschrijving | Gebruikt in |
|---|---|---|
| `Voedt` | Voeding relatie (LS → LSDeel) | `voedt_relaties` |
| `Sturing` | Sturing relatie | `sturing_relaties` |
| `Bevestiging` | Bevestiging relatie | `bevestiging_relaties` |
| `HoortBij` | Hoort-bij relatie | `hoortbij_relaties` |
| `HeeftKeuring` | Heeft-keuring relatie | `heeftkeuring_relaties` |
| `GemigreerdNaar` | Gemigreerd-naar relatie | `gemigreerdnaar_relaties` |
| `HoortBij` | Parent-child in boomstructuur | spec.md voorbeeldqueries |

---

## Sleutel-afkortingen / mapping

| Bron | Doel-veld | Regel |
|---|---|---|
| Any `uuid` | `_key` (assettypes/toezichtgroepen/identiteiten/beheerders/bestekken) | `uuid[:8]` |
| Any `uuid` | `_key` (relatietypes) | `uuid[:4]` |
| Any `@id` | `_key` (assets/assetrelaties/betrokkenerelaties) | `@id`.split("/")[-1][:36] |
| Any `@id` | `_key` (agents) | `@id`.split("/")[-1][:13] |
| Any `@id` | `uuid` (agents) | `@id`.split("/")[-1][:36] |
| `NaampadObject_naampad` | `naampad_parts` | `naampad.split("/")` |
| `AIMToestand_toestand` | `toestand` | `uri.split("/")[-1]` |
| `tz.Toezicht_toezichtgroep.DtcToezichtGroep_id` | `toezichtgroep_key` | `id[:8]` |
| `tz.Toezicht_toezichter.DtcToezichter_id` | `toezichter_key` | `id[:8]` |
| `tz.Schadebeheerder_schadebeheerder.DtcBeheerder_referentie` | `beheerder_key` | opgezocht via `beheerders_lookup` |
| WKT (EPSG:3812) | `geometry` | Geo GeoJSON Point (EPSG:4326) via `pyproj` |
| `HeeftBetrokkene_rol` | `rol` (in betrokkenerelaties) | `uri.split("/")[-1]` |
| `DtcBestekkoppeling_status` | `status` (in bestekkoppelingen) | `uri.split("/")[-1]` |

Zie ook: `datamodel.png` (visueel schema), `scripts/main_generate_diagram.py` (diagram-generator).
