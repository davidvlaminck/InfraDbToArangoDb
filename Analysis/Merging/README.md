README for Analysis/Merging steps
================================

Doel
----
In de map `Analysis/Merging` voegen we stappen toe die het resultaat van `main_export_keuringsinfo` aanvullen met extra kolommen uit andere bronnen.

Regel
-----
Telkens wanneer er iets verandert in de flow of er een extra stap bijkomt, voeg je hier een korte documentatie-regel toe met datum en wat er veranderd is.

Huidige stappen
---------------
1) 2026-06-05 - main_01_toevoegen_locatie_klassen.py
   - Na het creëren van het exportbestand (bijv. het resultaat van `main_export_keuringsinfo`) voegen we uit het bestand
     `kastenVlaanderen_l72_Merge_20260421_220143_1_ExportFeatures1_ZonderNullLocatie_TableToExcel.xlsx` de kolommen
     `eindscore` en `klassen_log_5` toe.
   - Deze worden hernoemd naar `score_locatie` en `klasse_locatie_tot_5`.
   - Mapping gebeurt op basis van de `uuid` (kolom A) in keuringsinfo met `uuid` in kasten bestand.
   - De `uuid` kolom wordt bewaard in het output bestand.

2) 2026-06-08 - main_02_toevoegen_inbreuken.py
   - Neemt de output van main_01 als input.
   - Voegt informatie toe vanuit `MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx` (sheet: latest_asset_summary).
   - Voegt de volgende kolommen toe:
     - `full_infraction_texts_latest`
     - `cat_F0` t/m `cat_F67` (alle cat_F* kolommen die aanwezig zijn in het inbreuken bestand)
   - Mapping gebeurt op basis van `uuid` in keuringsinfo met `arango_uuid` in inbreuken bestand.

Hoe te gebruiken
-----------------
Stap 1: main_01_toevoegen_locatie_klassen.py
  - Plaats de volgende bestanden in een toegankelijke map of geef het pad door aan het script:
    - Het exportbestand van `main_export_keuringsinfo` (input keuringsinfo excel / csv)
    - `kastenVlaanderen_..._TableToExcel.xlsx` (bron voor locatie-scores)
  - Run in PyCharm of vanaf de command line:

   python Analysis/Merging/main_01_toevoegen_locatie_klassen.py \
       --input-keuringsinfo path/to/keuringsinfo.xlsx \
       --input-kasten path/to/kastenVlaanderen_..._TableToExcel.xlsx \
       --output path/to/keuringsinfo_met_locatie.xlsx

  - Het script voegt de twee kolommen toe en schrijft een nieuw bestand weg met de toegevoegde kolommen.

Stap 2: main_02_toevoegen_inbreuken.py
  - Plaats de volgende bestanden in een toegankelijke map of geef het pad door aan het script:
    - Het output van main_01 (keuringsinfo_met_locatie.xlsx)
    - `MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx` (bron voor inbreuken)
  - Run in PyCharm of vanaf de command line:

   python Analysis/Merging/main_02_toevoegen_inbreuken.py \
       --input-keuringsinfo path/to/keuringsinfo_met_locatie.xlsx \
       --input-inbreuken path/to/list.detaillijst-latest-keuring.xlsx \
       --output path/to/keuringsinfo_met_inbreuken.xlsx

  - Het script voegt de inbreuken kolommen toe en schrijft een nieuw bestand weg.

Opmerkingen voor ontwikkelaars
-----------------------------
  - De scripts gebruiken `pandas` om Excel/CSV te lezen en te schrijven. Zorg dat `pandas` en `openpyxl` in je omgeving aanwezig zijn.
  - De kernlogica staat in `_merge_logic.py` zodat andere scripts of tests die logica kunnen hergebruiken.
  - Voor development/testing: gebruik `run_me_for_dev.py` en `run_me_for_dev_02.py` om de scripts in PyCharm uit te voeren.
