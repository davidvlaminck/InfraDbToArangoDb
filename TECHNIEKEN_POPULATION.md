# Technieken Attribute Population for Root Installation Assets

## Overview
The `technieken` attribute is added to root installation assets (type `Instal`) after they are created. This attribute is an array of strings that indicates the technical categories applicable to each installation.

## Population Process

### Step 1: Create Root Assets
Root assets are created via `create_instal_root_assets.aql`. Each unique root name from `assets.naampad_parts[0]` becomes an `Instal` asset with a deterministic `_key` (`instal_<rootName>` with slashes replaced by underscores).

### Step 2: Update with Technieken
The `update_instal_root_technieken.aql` query runs after root creation and performs:

1. **Select all root installation assets** - Filter by `@type` = `https://lgc.data.wegenenverkeer.be/ns/installatie#Instal`

2. **Initialize technieken array** - Every root asset receives a `technieken` attribute set to an empty array `[]`

3. **Conditional append for TUNNEL** - For each asset, check if `NaampadObject_naampad` contains "TUNNEL" (case-insensitive):
   - If true: append "Tunnel" to `technieken`

4. **Conditional append for HS** - Check if the root's tree contains any `DNBHoogspanning` assets:
   - Single scan collects `naampad_parts[0]` from all `DNBHoogspanning` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning`)
   - `UNIQUE` deduplicates to get roots that have at least one HS asset
   - If root name is in this set: append "HS" to `technieken`

5. **Conditional append for WV** - Check if the root's tree contains any `WegverlichtingGroep` assets:
   - Single scan collects `naampad_parts[0]` from all `WegverlichtingGroep` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#WegverlichtingGroep`)
   - `UNIQUE` deduplicates to get roots that have at least one WV asset
   - If root name is in this set: append "WV" to `technieken`

6. **Conditional append for VVOP** - Check if the root's tree contains any `VerlichteOversteekplaatsGroep` assets:
   - Single scan collects `naampad_parts[0]` from all `VerlichteOversteekplaatsGroep` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerlichteOversteekplaatsGroep`)
   - `UNIQUE` deduplicates to get roots that have at least one VVOP asset
   - If root name is in this set: append "VVOP" to `technieken`

7. **Conditional append for VL** - Check if the root's tree contains any `VRIBuiteninstallatie` assets:
   - Single scan collects `naampad_parts[0]` from all `VRIBuiteninstallatie` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VRIBuiteninstallatie`)
   - `UNIQUE` deduplicates to get roots that have at least one VL asset
   - If root name is in this set: append "VL" to `technieken`
   - *Note: Chosen over `Verkeersregelaar` because VRIBuiteninstallatie is the logical group/container (1930 roots vs 1833 for Verkeersregelaar), covering 101 additional roots*

8. **Conditional append for SNC** - Check if the root's tree contains any `Flitsgroep` assets:
   - Single scan collects `naampad_parts[0]` from all `Flitsgroep` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Flitsgroep`)
   - `UNIQUE` deduplicates to get roots that have at least one SNC asset
   - If root name is in this set: append "SNC" to `technieken`

9. **Conditional append for ASW** - Check if the root's tree contains any `Asweger` assets:
   - Single scan collects `naampad_parts[0]` from all `Asweger` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Asweger`)
   - `UNIQUE` deduplicates to get roots that have at least one ASW asset
   - If root name is in this set: append "ASW" to `technieken`

10. **Conditional append for Z30** - Check if the root's tree contains any `DynBordZ30` assets:
    - Single scan collects `naampad_parts[0]` from all `DynBordZ30` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordZ30`)
    - `UNIQUE` deduplicates to get roots that have at least one Z30 asset
    - If root name is in this set: append "Z30" to `technieken`

11. **Conditional append for MIV** - Check if the root's tree contains any `MIVModule` assets:
    - Single scan collects `naampad_parts[0]` from all `MIVModule` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule`)
    - `UNIQUE` deduplicates to get roots that have at least one MIV asset
    - If root name is in this set: append "MIV" to `technieken`

12. **Conditional append for DynBord** - Check if the root's tree contains any `DynBordGroep` assets:
    - Single scan collects `naampad_parts[0]` from all `DynBordGroep` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep`)
    - `UNIQUE` deduplicates to get roots that have at least one DynBord asset
    - If root name is in this set: append "DynBord" to `technieken`

13. **Conditional append for ANPR** - Check if the root's tree contains any ANPR cameras:
    - Cameras with type `ANPRCamera` (`https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ANPRCamera`)
    - OR Camera assets with `Camera_beeldverwerkingsinstelling` array containing `DtcCameraBeeldverwerking_typeBeeldverwerking` ending in "anpr"
    - OR assets with `.ANPR` in their `NaampadObject_naampad` (case-insensitive)
    - Single scan collects `naampad_parts[0]` from matching assets
    - `UNIQUE` deduplicates to get roots that have at least one ANPR asset
    - If root name is in this set: append "ANPR" to `technieken`

14. **Conditional append for POS** - Check if the root's tree contains any `Pompstation` assets:
    - Single scan collects `naampad_parts[0]` from all `Pompstation` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Pompstation`)
    - `UNIQUE` deduplicates to get roots that have at least one POS asset
    - If root name is in this set: append "POS" to `technieken`

15. **Conditional append for METEO** - Check if the root's tree contains any `Meteostation` assets:
    - Single scan collects `naampad_parts[0]` from all `Meteostation` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Meteostation`)
    - `UNIQUE` deduplicates to get roots that have at least one METEO asset
    - If root name is in this set: append "METEO" to `technieken`

16. **Conditional append for ZBP** - Check if the root's tree contains any `Zoutbijlaadplaats` assets:
    - Single scan collects `naampad_parts[0]` from all `Zoutbijlaadplaats` assets (`https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Zoutbijlaadplaats`)
    - `UNIQUE` deduplicates to get roots that have at least one ZBP asset
    - If root name is in this set: append "ZBP" to `technieken`

## AQL Logic
```aql
LET rootsWithHS = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DNBHoogspanning"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithHS = UNIQUE(rootsWithHS)

LET rootsWithWV = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#WegverlichtingGroep"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithWV = UNIQUE(rootsWithWV)

LET rootsWithVVOP = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VerlichteOversteekplaatsGroep"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithVVOP = UNIQUE(rootsWithVVOP)

LET rootsWithVL = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#VRIBuiteninstallatie"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithVL = UNIQUE(rootsWithVL)

LET rootsWithSNC = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Flitsgroep"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithSNC = UNIQUE(rootsWithSNC)

LET rootsWithASW = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Asweger"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithASW = UNIQUE(rootsWithASW)

LET rootsWithZ30 = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#DynBordZ30"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithZ30 = UNIQUE(rootsWithZ30)

LET rootsWithMIV = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#MIVModule"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithMIV = UNIQUE(rootsWithMIV)

LET rootsWithDynBord = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#DynBordGroep"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithDynBord = UNIQUE(rootsWithDynBord)

LET rootsWithANPR = (
    FOR a IN assets
        FILTER (
            a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#ANPRCamera"
        ) OR (
            a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/onderdeel#Camera"
            AND HAS(a, "Camera_beeldverwerkingsinstelling")
            AND LENGTH(a.Camera_beeldverwerkingsinstelling) > 0
            AND LENGTH(
                FOR b IN a.Camera_beeldverwerkingsinstelling
                    FILTER b.DtcCameraBeeldverwerking_typeBeeldverwerking LIKE "%anpr"
                    LIMIT 1
                    RETURN 1
            ) > 0
        ) OR (
            HAS(a, "NaampadObject_naampad")
            AND LIKE(a.NaampadObject_naampad, "%.ANPR%", true)
        )
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithANPR = UNIQUE(rootsWithANPR)

LET rootsWithPOS = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Pompstation"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithPOS = UNIQUE(rootsWithPOS)

LET rootsWithMETEO = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Meteostation"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithMETEO = UNIQUE(rootsWithMETEO)

LET rootsWithZBP = (
    FOR a IN assets
        FILTER a["@type"] == "https://wegenenverkeer.data.vlaanderen.be/ns/installatie#Zoutbijlaadplaats"
           AND HAS(a, "naampad_parts")
           AND LENGTH(a.naampad_parts) > 0
        RETURN a.naampad_parts[0]
)

LET uniqueRootsWithZBP = UNIQUE(rootsWithZBP)

FOR a IN instalAssets
    LET hasTunnel = LIKE(a.NaampadObject_naampad, "%TUNNEL%", true)
    LET hasHS = a.NaampadObject_naampad IN uniqueRootsWithHS
    LET hasWV = a.NaampadObject_naampad IN uniqueRootsWithWV
    LET hasVVOP = a.NaampadObject_naampad IN uniqueRootsWithVVOP
    LET hasVL = a.NaampadObject_naampad IN uniqueRootsWithVL
    LET hasSNC = a.NaampadObject_naampad IN uniqueRootsWithSNC
    LET hasASW = a.NaampadObject_naampad IN uniqueRootsWithASW
    LET hasZ30 = a.NaampadObject_naampad IN uniqueRootsWithZ30
    LET hasMIV = a.NaampadObject_naampad IN uniqueRootsWithMIV
    LET hasDynBord = a.NaampadObject_naampad IN uniqueRootsWithDynBord
    LET hasANPR = a.NaampadObject_naampad IN uniqueRootsWithANPR
    LET hasPOS = a.NaampadObject_naampad IN uniqueRootsWithPOS
    LET hasMETEO = a.NaampadObject_naampad IN uniqueRootsWithMETEO
    LET hasZBP = a.NaampadObject_naampad IN uniqueRootsWithZBP
    LET technieken = APPEND(
        APPEND(
            APPEND(
                APPEND(
                    APPEND(
                        APPEND(
                            APPEND(
                                APPEND(
                                    APPEND(
                                        APPEND(
                                            hasTunnel ? ["Tunnel"] : [],
                                            hasHS ? ["HS"] : []
                                        ),
                                        hasWV ? ["WV"] : []
                                    ),
                                    hasVVOP ? ["VVOP"] : []
                                ),
                                hasVL ? ["VL"] : []
                            ),
                            hasSNC ? ["SNC"] : []
                        ),
                        hasASW ? ["ASW"] : []
                    ),
                    hasZ30 ? ["Z30"] : []
                ),
                hasMIV ? ["MIV"] : []
            ),
            hasDynBord ? ["DynBord"] : []
        ),
        APPEND(
            APPEND(
                APPEND(
                    hasANPR ? ["ANPR"] : [],
                    hasPOS ? ["POS"] : []
                ),
                hasMETEO ? ["METEO"] : []
            ),
            hasZBP ? ["ZBP"] : []
        )
    )
    UPDATE a WITH { technieken: technieken } IN assets
```

- `LIKE(..., "%TUNNEL%", true)` - Case-insensitive pattern match
- `APPEND()` concatenates arrays
- `a.NaampadObject_naampad IN uniqueRootsWithHS` - O(1) set membership check

## Result Examples
- Root with TUNNEL in name → `technieken: ["Tunnel"]`
- Root with DNBHoogspanning in tree → `technieken: ["HS"]`
- Root with WegverlichtingGroep in tree → `technieken: ["WV"]`
- Root with VerlichteOversteekplaatsGroep in tree → `technieken: ["VVOP"]`
- Root with VRIBuiteninstallatie in tree → `technieken: ["VL"]`
- Root with Flitsgroep in tree → `technieken: ["SNC"]`
- Root with Asweger in tree → `technieken: ["ASW"]`
- Root with DynBordZ30 in tree → `technieken: ["Z30"]`
- Root with MIVModule in tree → `technieken: ["MIV"]`
- Root with DynBordGroep in tree → `technieken: ["DynBord"]`
- Root with ANPR camera in tree → `technieken: ["ANPR"]`
- Root with Pompstation in tree → `technieken: ["POS"]`
- Root with Meteostation in tree → `technieken: ["METEO"]`
- Root with Zoutbijlaadplaats in tree → `technieken: ["ZBP"]`
- Root with both HS and WV → `technieken: ["HS", "WV"]`
- Root with Tunnel, HS, and WV → `technieken: ["Tunnel", "HS", "WV"]`
- Root with HS, WV, and VVOP → `technieken: ["HS", "WV", "VVOP"]`
- Root with all fourteen → `technieken: ["Tunnel", "HS", "WV", "VVOP", "VL", "SNC", "ASW", "Z30", "MIV", "DynBord", "ANPR", "POS", "METEO", "ZBP"]`
- All other root installations → `technieken: []`

## Performance
- 14 full collection scans (one for Instal assets, one each for DNBHoogspanning, WegverlichtingGroep, VerlichteOversteekplaatsGroep, VRIBuiteninstallatie, Flitsgroep, Asweger, DynBordZ30, MIVModule, DynBordGroep, ANPR cameras, Pompstation, Meteostation, Zoutbijlaadplaats)
- No subqueries per root asset
- Uses index on `naampad_parts` for root extraction
- Sparse index on `naampad_parent` available for tree traversal

## Statistics (Current)
- Total Instal roots: 17,019
- Roots with at least one technique: ~1,807
- Technique distribution:
  - VL: 1,902
  - WV: 1,660
  - ANPR: 1,052
  - MIV: 732
  - VVOP: 624
  - SNC: 604
  - HS: 475
  - DynBord: 401
  - Z30: 293
  - POS: 253
  - METEO: 97
  - ASW: 40
  - Tunnel: 31
  - ZBP: 9