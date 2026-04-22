# LSB Keuring Detail Export

This export creates one Excel file with one worksheet containing:

- all active `onderdeel#Laagspanningsbord` assets
- all active `onderdeel#ElektrischeKeuring` assets linked by an active `HeeftKeuring` relation
- repeated Laagspanningsbord columns per keuringsrecord

If a Laagspanningsbord has no active linked keuring, it is still included once with empty keuring columns.

## Files

- `Analysis/export_lsb_keuring_details.py` - CLI export script
- `Analysis/main_export_lsb_keuring_details.py` - PyCharm-friendly entrypoint

## Columns

Fixed columns:

- `uuid`
- `naampad`
- `toezichtgroep`
- `toezichter`
- `techniek`

Plus all attributes from the keuring document, flattened recursively with prefix `keuring_`.

## Run

```bash
cd /home/davidlinux/PycharmProjects/InfraDbToArangoDb
./.venv/bin/python -u Analysis/export_lsb_keuring_details.py \
  --settings /home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json \
  --env PRD
```

Optional quick test:

```bash
cd /home/davidlinux/PycharmProjects/InfraDbToArangoDb
./.venv/bin/python -u Analysis/export_lsb_keuring_details.py \
  --settings /home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json \
  --env PRD \
  --limit 20 \
  --out /tmp/lsb_keuring_details_test.xlsx
```

