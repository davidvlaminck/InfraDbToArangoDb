# Tree analysis — goal and specification

## Short summary
Create a reusable script (placed in `Analysis/TreeAnalysis`) that discovers and enumerates the different tree structures present in the ArangoDB dataset produced by this repository. Given a single asset in a tree instance, the script should be able to identify and label which structural template (tree pattern) that instance belongs to and collect the uuids of all LSDeel assets inside that instance.

## Scope and high-level requirements
- Only assets that have a `naampad_parts` array are part of a tree and must be considered. (Important: if `naampad_parts` is not present or empty for an asset, that asset is ignored for tree discovery.)
- Assets where `AIMDBStatus_isActief` is present and equals `false` (or False) are considered inactive and must NOT be included in the analysis or outputs.
- Two assets belong to the same tree instance when their first element of `naampad_parts` is identical (the same beheerobject).
- A tree *structure* (sometimes called template or pattern) is defined by the ordered asset type sequence per level (asset `short_uri`), ignoring counts of siblings. Example: a root with 1 WV and 100 VPLMast is the same structure as root with 2 WV and 50 VPLMast each, because both reduce to the same type-layout by level.

## Assumptions
- The database is `infra_db` (see `main_linux_arango.py` / settings file). Use those credentials for connecting.
- Relevant asset documents will always contain `naampad_parts` (an array). The script will not fall back to `NaampadObject_naampad` or `naampad_parent` — those fields are explicitly excluded from tree discovery to avoid ambiguity.
- Asset documents include `assettype_key` which can be resolved to `assettypes.short_uri`. The script will pre-fetch the `assettypes` collection into memory and map keys to short_uris.
- LSDeel assets have `short_uri` "lgc:installatie#LSDeel" (or equivalent entry in `assettypes`).

## Deliverables
1. A JSON file with all discovered tree structures. Each structure entry contains:
   - a stable structural id (generated hash or incrementing id)
   - the canonical ordered list of type short_uris per level (the structure template)
   - an example occurrence (beheerobject and a sample asset _key) where this structure is found
   - optional human label field (empty by default)
2. A mapping file (CSV or JSON) that lists each tree instance (by beheerobject) with:
   - the chosen structure id
   - list of included asset uuids (optionally grouped by level)
   - list of LSDeel uuids that belong to that instance
3. A small CLI runner `Analysis/TreeAnalysis/run_tree_analysis.py` with arguments to:
   - connect to DB (uses repo settings by default)
   - produce the JSON/CSV outputs
   - optionally write examples and debug logs
4. Minimal README in `Analysis/TreeAnalysis/README.md` with usage examples and acceptance criteria.

## Precise definitions
- beheerobject: first element of `naampad_parts` (must be present and non-empty).
- tree instance: set of assets that share the same beheerobject value.
- tree structure (template): an ordered list of sets of asset types per depth level. For clarity the canonical structure representation is:
  - level 0: { root_short_uri }
  - level 1: { short_uri, ... }
  - level 2: { ... }
  This representation intentionally ignores the count of nodes of a certain short_uri on the same level; only presence matters.

## Algorithm / Implementation plan
Checklist (I will implement these steps in code):
- [ ] Resolve `lsdeel_short_uri` and other asset type short_uris from the `assettypes` collection.
- [ ] Query all assets that have a non-empty `naampad_parts` array and are active (filter by `AIMDBStatus_isActief == true` as appropriate). Do not use `NaampadObject_naampad` or `naampad_parent` as fallbacks.
- [ ] Group assets by `beheerobject` (= first element of naampad_parts).
- [ ] For each group (tree instance):
    - Build the tree using the `naampad_parts` array to determine depth (depth = length(parts) - 1). Each asset's depth is the index of its last element in `naampad_parts`.
    - Collect the set of type short_uris present at each depth level. Resolve `assettype_key` -> `assettypes.short_uri` using the pre-fetched map.
    - Reduce the per-level sets to a canonical ordered list-of-sets that defines the structure template.
- [ ] Deduplicate structures across all instances by comparing their canonical representation (deterministic key: JSON dump of ordered list-of-sets sorted internally).
- [ ] For each canonical structure keep the first observed occurrence (beheerobject + sample asset _key) and an empty `label` field.
- [ ] For each tree instance, collect uuids of assets with type LSDeel and include them in the mapping output.

Notes on graph vs. path: since `naampad_parts` gives the path and depth explicitly, it's not necessary to traverse edges — building the tree by parsing `naampad_parts` is sufficient, faster and less error-prone.

Additionally, ensure that mock CSV input supports a column `AIMDBStatus_isActief` (values: true/false or True/False) and that records where this field is false are ignored during the mock run.

## Output formats
1. `tree_structures.json` (array of objects):
   - id: string
   - structure: ordered array where each element is an array of short_uris (level 0 -> level N)
   - example: { "beheerobject": "...", "asset_key": "..." }
   - label: null or string (to be filled by user)

2. `tree_instances.csv` (or `.json`): one row per beheerobject
   - beheerobject, structure_id, asset_keys (JSON array), lsdeel_keys (JSON array), num_assets, created_at

## Performance considerations and AQL hints
- Don't do per-asset nested scans. Use the `naampad_parts` array for grouping and depth calculation.
- Ensure there is an index on `assets.naampad_parts` and on `assets.assettype_key` and `AIMDBStatus_isActief` for performant filtering. The repo already contains an index suggestion.
- Prefer streaming cursors in python when pulling large resultsets (use batch_size / ttl and stream=True with the `python-arango` driver).
- Resolve `assettype_key` -> `short_uri` in a single lookup: pre-fetch the `assettypes` collection into a dict in memory.

## CLI examples
- Basic run (uses settings in repo):

```bash
PYTHONPATH=$(pwd) .venv/bin/python Analysis/TreeAnalysis/run_tree_analysis.py --out-dir Analysis/TreeAnalysis/output
```

- Run with DB host/creds override:

```bash
PYTHONPATH=$(pwd) .venv/bin/python Analysis/TreeAnalysis/run_tree_analysis.py --db-host localhost --db-name infra_db --db-user sync_user --db-pass "..."
```

- Local/mock run (no DB):

```bash
PYTHONPATH=$(pwd) .venv/bin/python Analysis/TreeAnalysis/run_tree_analysis.py --mock-csv some_assets.csv --out-dir Analysis/TreeAnalysis/output
```

## Validation / tests
- Unit test verifying that two tree instances with the same per-level type sets are deduplicated into one structure.
- Integration test (mocked DB) that produces the JSON and CSV and checks counts and a few sample mappings.
- Sanity checks: number of unique beheerobjects equals number of produced instances; sum of assets across instances equals number of assets with naampad used in the run.

## Acceptance criteria
- Script produces `tree_structures.json` and `tree_instances.csv` in the requested output directory.
- The number of produced tree instances equals the number of unique beheerobject values found in the DB input.
- Each structure has a deterministic canonical representation and identical templates are deduplicated.
- LSDeel uuids are collected per instance and included in the mapping output.
- Script runs in a reasonable time for the dataset size (works with streaming, doesn't require full in-memory copies of all assets except the resolved type map).

## Next steps (after delivering the initial script)
- Add an interactive labeling UI (small CLI prompt) to assign human labels to discovered structures and persist labels back into `tree_structures.json`.
- Add a quick-lookup function: given any asset `_key`, return the instance's beheerobject, structure id and LSDeel uuids.
- Optionally extend the script to detect near-equal structures (similarity scoring) and present merge suggestions.

---

Place this file at `Analysis/TreeAnalysis/goal.md`. When you confirm this structure I will implement `run_tree_analysis.py`, the README and tests according to the spec.
