# Keuringsinfo export

Doel: exporteer geconsolideerde keuringsinfo voor LS/LSDeel naar een Excel met 1 sheet per toezichtgroep.

## Bestanden
- `keuringsinfo_export.md`: aanpak + AQL-voorstel
- `export_keuringsinfo.py`: exporter (CLI)
- `main_export_keuringsinfo.py`: **PyCharm entrypoint** (run zonder CLI)

## Install
De repo gebruikt `requirements.txt`. Zorg dat `openpyxl` geïnstalleerd is.

## Run (PyCharm - aanbevolen)
1. Open `Analysis/main_export_keuringsinfo.py`
2. Pas bovenaan aan:
   - `SETTINGS_PATH`
   - `LS_SHORT_URI` (jouw LS assettype short_uri)
3. Run het bestand.

Output: `Analysis/keuringsinfo_YYYYmmdd_HHMMSS.xlsx`.

## Run (CLI)
Voorbeeld:

```bash
python Analysis/export_keuringsinfo.py \
  --settings /home/davidlinux/Documenten/AWV/resources/settings_SyncToArangoDB.json \
  --env PRD \
  --auth JWT \
  --ls-short-uri 'https://lgc.data.wegenenverkeer.be/ns/installatie#LS' \
  --lsdeel-short-uri 'lgc:installatie#LSDeel'
```

## Troubleshooting
- Krijg je 0 rows? Controleer de correcte `LS_SHORT_URI` / `--ls-short-uri` en of je DB gevuld is.
- Krijg je een auth error (401)? Dan klopt user/pass in je settings mogelijk niet of is de DB niet bereikbaar.

