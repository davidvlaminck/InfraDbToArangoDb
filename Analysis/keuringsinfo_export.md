# Use case: export keuringsinfo (LS/LSDeel) naar Excel

Dit document is een concrete suggestie/aanpak om de use case uit `spec.md` op te lossen, inclusief een AQL-voorstel en een runnable Python-exporter.

## Contract (wat leveren we op)

**Input**
- ArangoDB database zoals gevuld door dit project (`assets`, `assettypes`, `relatietypes`, `assetrelaties`, `toezichtgroepen`).
- Settings JSON zoals gebruikt in `main_linux_arango.py` (zelfde structuur).

**Output**
- Eén Excel-bestand met:
  - 1 sheet per toezichtgroep: `V&W-WL`, `V&W-WA`, `V&W-WO`, `V&W-WW`, `V&W-WVB`
  - 1 sheet `Andere` voor alle overige toezichtgroepen (incl. null/unknown)
- Per rij: minimaal asset-identiteit + geconsolideerde keuringsinfo + herkomst.

**Business rules**
- Keuringsinfo kan op **LS** of **LSDeel** zitten.
- LS en LSDeel worden indien mogelijk als “paar” beschouwd als:
  1) ze verbonden zijn via een `Voedt` relatie **of**
  2) ze dezelfde parent hebben op basis van `NaampadObject_naampad` (alles behalve laatste segment).
- **Belangrijk:** het hoeft niet altijd een paar te zijn. **Losse LS of losse LSDeel objecten** die niet gelinkt kunnen worden, moeten ook mee worden beschouwd in de export.
- Indien keuringsinfo op beide (in een paar) zit: **meest recente** `EMObject_datumLaatsteKeuring` wint.

## Data aannames in deze repo

- `assets` documenten bevatten o.a.:
  - `NaampadObject_naampad` en ook `naampad_parts` (split op `/`) door `InitialFillStep`.
  - `toezichtgroep_key` (eerste 8 chars van `DtcToezichtGroep_id`).
- `toezichtgroepen` bevat `_key` (= first 8 chars) en `naam`.

> Let op: de exacte veldnamen voor keuringsinfo (`ins.EMObject_resultaatKeuring`, `ins.EMObject_datumLaatsteKeuring`) zijn in jouw spec vastgelegd. Als de keys in de DB iets anders zijn (bv. `ins.resultaatKeuring`), moet de AQL/Python mapping aangepast worden.

## Aanpak in 3 stappen

### Stap 1: id’s ophalen voor asset- en relatietypes
We willen 2 assettypes (LS en LSDeel) én 1 relatietype (Voedt). Gebruik `assettypes.short_uri` en `relatietypes.short` (zoals voorbeeldqueries in `spec.md`).

### Stap 2: kandidaat-matches én singletons opbouwen
We bouwen een gecombineerde set met:
1) LS↔LSDeel matches via `Voedt`
2) LS↔LSDeel matches via dezelfde `parent_path`
3) **LS zonder match** (singleton)
4) **LSDeel zonder match** (singleton)

Daarna `UNION_DISTINCT` om dubbels te vermijden.

### Stap 3: keuringsinfo consolideren en groeperen in Excel
- Voor matches: kies meest recente datum tussen LS en LSDeel (indien beide aanwezig).
- Voor singletons: neem info van het object zelf.
- Bepaal `toezichtgroep` via `toezichtgroep_key` → `toezichtgroepen.naam`.

## AQL-voorstel (1 query, output voor matches én singletons)

We matchen LS ↔ LSDeel **enkel** via de relatie `Voedt` (LS -> LSDeel).
Naampad-gebaseerde matching is bewust weggelaten.

```aql
LET ls_key      = FIRST(FOR at IN assettypes FILTER at.short_uri == "lgc:installatie#LS"     LIMIT 1 RETURN at._key)
LET lsdeel_key  = FIRST(FOR at IN assettypes FILTER at.short_uri == "lgc:installatie#LSDeel" LIMIT 1 RETURN at._key)
LET voedt_key   = FIRST(FOR rt IN relatietypes FILTER rt.short == "Voedt" LIMIT 1 RETURN rt._key)

LET pairs = (
  FOR ls IN assets
    FILTER ls.AIMDBStatus_isActief == true
    FILTER ls.assettype_key == ls_key
    FOR lsdeel, e IN OUTBOUND ls assetrelaties
      FILTER e.relatietype_key == voedt_key
      FILTER lsdeel.assettype_key == lsdeel_key
      RETURN {ls: ls, lsdeel: lsdeel, match: "voedt", rank: 1}
)

LET matched_ls_keys = (
  FOR p IN pairs
    COLLECT k = p.ls._key
    RETURN k
)

LET matched_lsdeel_keys = (
  FOR p IN pairs
    COLLECT k = p.lsdeel._key
    RETURN k
)

LET single_ls = (
  FOR ls IN assets
    FILTER ls.AIMDBStatus_isActief == true
    FILTER ls.assettype_key == ls_key
    FILTER ls._key NOT IN matched_ls_keys
    RETURN {ls: ls, lsdeel: null, match: "single_ls", rank: 3}
)

LET single_lsdeel = (
  FOR ld IN assets
    FILTER ld.AIMDBStatus_isActief == true
    FILTER ld.assettype_key == lsdeel_key
    FILTER ld._key NOT IN matched_lsdeel_keys
    RETURN {ls: null, lsdeel: ld, match: "single_lsdeel", rank: 3}
)

LET all_candidates = UNION_DISTINCT(pairs, single_ls, single_lsdeel)

FOR chosen_doc IN (
  FOR c IN all_candidates
    LET chosen = c.lsdeel != null ? c.lsdeel : c.ls
    COLLECT k = chosen._key INTO grouped = c
    LET best = FIRST(
      FOR g IN grouped
        SORT g.rank ASC
        LIMIT 1
        RETURN g
    )
    RETURN best
)
  LET chosen = chosen_doc.lsdeel != null ? chosen_doc.lsdeel : chosen_doc.ls
  LET type = chosen_doc.lsdeel != null ? "LSDeel" : "LS"
  LET tz = FIRST(FOR t IN toezichtgroepen FILTER t._key == chosen.toezichtgroep_key LIMIT 1 RETURN t)
  LET ins = chosen.ins

  SORT chosen.NaampadObject_naampad ASC

  RETURN {
    toezichtgroep: tz != null ? tz.naam : "UNKNOWN",
    type: type,
    match: chosen_doc.match,

    uuid: chosen._key,
    naam: chosen.AIMNaamObject_naam,
    naampad: chosen.NaampadObject_naampad,

    datum_laatste_keuring: ins != null ? ins.EMObject_datumLaatsteKeuring : null,
    resultaat_keuring: ins != null ? ins.EMObject_resultaatKeuring : null
  }
```

### Opmerking
Als je wél de “beste” keuringsinfo over LS+LSDeel heen wil behouden, maar toch slechts 1 object exporteren, dan kan je hier nog steeds een `best`-selectie doen over `[p.ls.ins, p.lsdeel.ins]` en de gekozen asset `chosen` laten zoals hierboven.

### Edge cases
- datum ontbreekt → exporteer lege datum of filter ze weg (keuze).
- toezichtgroep ontbreekt → naar sheet `Andere`.
- parent match kan veel combinaties geven als een parent veel children heeft. Eerst beperken tot LS/LSDeel assettypes, en als nodig extra criteria toevoegen.

## Implementatie in Python
Zie `Analysis/export_keuringsinfo.py`.
