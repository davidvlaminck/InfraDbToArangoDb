Tree analysis
===============

This folder contains a small tool to discover canonical tree structures from assets in the ArangoDB dataset.

Files
- `tree_analysis.py`: core pure functions to build structures and instances from asset dicts.
- `run_tree_analysis.py`: CLI runner (reads DB or a mock CSV) and writes JSON outputs.
- `test_tree_analysis.py`: unit tests for the core logic.

Usage (mock mode)

PYTHONPATH=$(pwd) .venv/bin/python Analysis/TreeAnalysis/run_tree_analysis.py --mock-csv <csv>

Outputs
- `output/tree_structures.json`: array of discovered structures
- `output/tree_instances.json`: mapping beheerobject -> instance data

Conventions
- Only assets that contain a non-empty `naampad_parts` array are considered.
- Asset type resolution requires the `assettypes` collection; when running in mock mode, assettype resolution may be empty and short URIs will fallback to the `assettype_key` value.

