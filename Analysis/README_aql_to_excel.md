# AQL to Excel

Generic helper script to execute an AQL query and export the result to one Excel tab.

- Script: `Analysis/aql_to_excel.py`
- Output: one sheet (`result` by default)
- JSON rows are flattened to columns (`nested.key` becomes `nested_key`)
- Lists are stored as JSON strings in one cell

## Quick run (PyCharm / terminal)

```bash
/home/davidlinux/PycharmProjects/InfraDbToArangoDb/.venv/bin/python -u /home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/aql_to_excel.py --use-example --limit 100
```

## Run with query file

```bash
/home/davidlinux/PycharmProjects/InfraDbToArangoDb/.venv/bin/python -u /home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/aql_to_excel.py \
  --query-file /home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/example_aql_to_excel_query.aql \
  --bind-vars-json '{"limit": 200}'
```

## Run with inline query

```bash
/home/davidlinux/PycharmProjects/InfraDbToArangoDb/.venv/bin/python -u /home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/aql_to_excel.py \
  --query "FOR a IN assets LIMIT 10 RETURN {uuid: a._key, naam: a.AIMNaamObject_naam}"
```

## One-click PyCharm entrypoint

Run `Analysis/main_aql_to_excel.py` directly in PyCharm.
It uses:
- `Analysis/example_aql_to_excel_query.aql`
- `/home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json`
- timestamped output in `Analysis/aql_export_YYYYmmdd_HHMMSS.xlsx`

Equivalent terminal command:

```bash
/home/davidlinux/PycharmProjects/InfraDbToArangoDb/.venv/bin/python -u /home/davidlinux/PycharmProjects/InfraDbToArangoDb/Analysis/main_aql_to_excel.py
```
