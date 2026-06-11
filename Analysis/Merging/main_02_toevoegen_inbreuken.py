#!/usr/bin/env python3
"""Executable script to add infraction data to keuringsinfo export (placed in Analysis/Merging).

This script takes the output from main_01_toevoegen_locatie_klassen.py and enriches it
with infraction data from the MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx file.

Usage (example):
  python Analysis/Merging/main_02_toevoegen_inbreuken.py \
      --input-keuringsinfo path/to/keuringsinfo_met_locatie.xlsx \
      --input-inbreuken path/to/list.detaillijst-latest-keuring.xlsx \
      --output path/to/keuringsinfo_met_inbreuken.xlsx
"""
import argparse
from pathlib import Path
import sys

import pandas as pd
import re

from _merge_logic import load_sheet_as_df, merge_all_sheets_with_inbreuken, find_uuid_column


def main(argv=None):
    parser = argparse.ArgumentParser(description="Voeg inbreuken data toe op basis van inbreuken bestand")
    parser.add_argument(
        "--input-keuringsinfo",
        required=False,
        default="../keuringsinfo_20260611_114736_met_locatie.xlsx",
        help="Path naar keuringsinfo excel met locatie (default: ../keuringsinfo_20260611_114736_met_locatie.xlsx)",
    )
    parser.add_argument(
        "--input-inbreuken",
        required=False,
        default="/home/davidlinux/Documenten/AWV/Keuringen/MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx",
        help="Path naar inbreuken excel (default: /home/davidlinux/Documenten/AWV/Keuringen/MyVinotte Min.Vl.Gemeenschap Reports list.detaillijst-latest-keuring.xlsx)",
    )
    parser.add_argument(
        "--output",
        required=False,
        default="../keuringsinfo_20260611_114736_met_inbreuken.xlsx",
        help="Output path voor verrijkte keuringsinfo (xlsx or csv) (default: ../keuringsinfo_20260611_114736_met_inbreuken.xlsx)",
    )
    parser.add_argument("--uuid-col-keuring", default="uuid", help="Kolomnaam uuid in keuringsinfo (default: uuid)")
    parser.add_argument("--uuid-col-inbreuken", default="arango_uuid", help="Kolomnaam uuid in inbreuken bestand (default: arango_uuid)")
    parser.add_argument("--sheet-inbreuken", default="latest_asset_summary", help="Sheetnaam in inbreuken excel (default: latest_asset_summary)")
    parser.add_argument("--sheet-keuringsinfo", default=None, help="(Optioneel) sheetnaam in keuringsinfo excel om te gebruiken (detailtabbladen)")

    args = parser.parse_args(argv)

    # Resolve paths relative to this script directory when given as relative
    script_dir = Path(__file__).parent

    input_k = Path(args.input_keuringsinfo)
    if not input_k.is_absolute():
        input_k = (script_dir / input_k).resolve()

    input_inbreuken = Path(args.input_inbreuken)
    if not input_inbreuken.is_absolute():
        input_inbreuken = (script_dir / input_inbreuken).resolve()

    output_p = Path(args.output)
    if not output_p.is_absolute():
        output_p = (script_dir / output_p).resolve()

    # Informative checks
    if not input_k.exists():
        print(f"Waarschuwing: keuringsinfo bestand niet gevonden op {input_k}. Controleer pad.")
    if not input_inbreuken.exists():
        print(f"Waarschuwing: inbreuken bestand niet gevonden op {input_inbreuken}. Controleer pad.")

    print(f"Geresolveerde paden: keuringsinfo={input_k}, inbreuken={input_inbreuken}, output={output_p}")

    # Load inbreuken as a single DataFrame
    print(f"Laden inbreuken from {input_inbreuken} (sheet={args.sheet_inbreuken})")
    df_inbreuken = load_sheet_as_df(str(input_inbreuken), sheet_name=args.sheet_inbreuken)

    # Check inbreuken uuid column
    if args.uuid_col_inbreuken not in df_inbreuken.columns:
        found_col_r, found_count_r = find_uuid_column(df_inbreuken)
        print(f"Kolommen in inbreuken: {list(df_inbreuken.columns)[:20]}")
        if found_col_r:
            print(f"Auto-gedetecteerde uuid-kolom in inbreuken: {found_col_r} (matches: {found_count_r})")
            args.uuid_col_inbreuken = found_col_r
        else:
            print("Kon geen uuid-kolom detecteren in inbreuken. Geef --uuid-col-inbreuken op met de juiste kolomnaam.")
            sys.exit(2)

    # Check for required columns in inbreuken
    if "full_infraction_texts_latest" not in df_inbreuken.columns:
        print("Waarschuwing: kolom 'full_infraction_texts_latest' niet gevonden in inbreuken bestand.")
    cat_cols = [c for c in df_inbreuken.columns if c.startswith("cat_F")]
    if not cat_cols:
        print("Waarschuwing: geen cat_F* kolommen gevonden in inbreuken bestand.")
    else:
        print(f"Gevonden cat_F kolommen: {cat_cols}")

    print(f"Mergen: verrijking van alle detail-tabbladen (niet-Pivot) met inbreuken data...")
    try:
        result_sheets = merge_all_sheets_with_inbreuken(
            str(input_k),
            df_inbreuken,
            uuid_col_keuring=args.uuid_col_keuring,
            uuid_col_inbreuken=args.uuid_col_inbreuken,
            sheet_keuringsinfo=args.sheet_keuringsinfo,
        )
    except Exception as e:
        print(f"Fout bij samenvoegen: {e}")
        sys.exit(2)

    # Write all sheets back to Excel
    out_s = str(output_p)
    if out_s.lower().endswith(".csv"):
        # CSV output doesn't support multiple sheets; just take the first non-Pivot sheet
        for sname, df_sheet in result_sheets.items():
            if not sname.startswith("Pivot"):
                df_sheet.to_csv(out_s, index=False)
                break
    else:
        with pd.ExcelWriter(out_s, engine="openpyxl") as writer:
            for sname, df_sheet in result_sheets.items():
                df_sheet.to_excel(writer, sheet_name=sname, index=False)

    print(f"Gereed. Output met alle tabbladen (verrijkt met inbreuken) weggeschreven naar {out_s}")


if __name__ == "__main__":
    main()