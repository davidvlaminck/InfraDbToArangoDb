#!/usr/bin/env python3
"""Executable script to add locatie score and klasse to keuringsinfo export (placed in Analysis/Merging).

Usage (example):
  python Analysis/Merging/main_01_toevoegen_locatie_klassen.py \
      --input-keuringsinfo path/to/keuringsinfo.xlsx \
      --input-kasten path/to/kastenVlaanderen.xlsx \
      --output path/to/keuringsinfo_met_locatie.xlsx
"""
import argparse
from pathlib import Path
import sys

import pandas as pd
import re

from _merge_logic import load_sheet_as_df, add_locatie_klassen, merge_all_sheets


def find_uuid_column(df):
    uuid_re = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
    best_col = None
    best_count = 0
    for c in df.columns:
        try:
            sample = df[c].dropna().astype(str).head(200)
        except Exception:
            continue
        count = sample.apply(lambda v: bool(uuid_re.search(v))).sum()
        if count > best_count:
            best_count = int(count)
            best_col = c
    return best_col, best_count


def main(argv=None):
    parser = argparse.ArgumentParser(description="Voeg locatie score en klasse toe op basis van kasten bestand")
    parser.add_argument(
        "--input-keuringsinfo",
        required=False,
        default="../keuringsinfo_20260611_114736.xlsx",
        help="Path naar keuringsinfo excel/csv (default: ../keuringsinfo_20260611_114736.xlsx)",
    )
    parser.add_argument(
        "--input-kasten",
        required=False,
        default="kastenVlaanderen_l72_Merge_20260421_220143_1_ExportFeatures1_ZonderNullLocatie_TableToExcel.xlsx",
        help="Path naar kastenVlaanderen TableToExcel excel (default: ../kastenVlaanderen_...xlsx)",
    )
    parser.add_argument(
        "--output",
        required=False,
        default="../keuringsinfo_20260611_114736_met_locatie.xlsx",
        help="Output path voor verrijkte keuringsinfo (xlsx or csv) (default: ../keuringsinfo_20260611_114736_met_locatie.xlsx)",
    )
    parser.add_argument("--uuid-col-keuring", default="uuid", help="Kolomnaam uuid in keuringsinfo (default: uuid)")
    parser.add_argument("--uuid-col-kasten", default="uuid", help="Kolomnaam uuid in kasten bestand (default: uuid)")
    parser.add_argument("--sheet-kasten", default=None, help="(Optioneel) sheetnaam in kasten excel om te gebruiken")
    parser.add_argument("--sheet-keuringsinfo", default=None, help="(Optioneel) sheetnaam in keuringsinfo excel om te gebruiken (detailtabbladen)")

    args = parser.parse_args(argv)

    # Resolve paths relative to this script directory when given as relative
    script_dir = Path(__file__).parent

    input_k = Path(args.input_keuringsinfo)
    if not input_k.is_absolute():
        input_k = (script_dir / input_k).resolve()

    input_kasten = Path(args.input_kasten)
    if not input_kasten.is_absolute():
        input_kasten = (script_dir / input_kasten).resolve()

    output_p = Path(args.output)
    if not output_p.is_absolute():
        output_p = (script_dir / output_p).resolve()

    # Informative checks
    if not input_k.exists():
        print(f"Waarschuwing: keuringsinfo bestand niet gevonden op {input_k}. Controleer pad.")
    if not input_kasten.exists():
        print(f"Waarschuwing: kasten bestand niet gevonden op {input_kasten}. Controleer pad.")

    print(f"Geresolveerde paden: keuringsinfo={input_k}, kasten={input_kasten}, output={output_p}")

    # Load kasten as a single DataFrame
    print(f"Laden kasten from {input_kasten} (sheet={args.sheet_kasten})")
    df_kasten = load_sheet_as_df(str(input_kasten), sheet_name=args.sheet_kasten)

    # Check kasten uuid column
    if args.uuid_col_kasten not in df_kasten.columns:
        found_col_r, found_count_r = find_uuid_column(df_kasten)
        print(f"Kolommen in kasten: {list(df_kasten.columns)[:20]}")
        if found_col_r:
            print(f"Auto-gedetecteerde uuid-kolom in kasten: {found_col_r} (matches: {found_count_r})")
            args.uuid_col_kasten = found_col_r
        else:
            print("Kon geen uuid-kolom detecteren in kasten. Geef --uuid-col-kasten op met de juiste kolomnaam.")
            sys.exit(2)

    print(f"Mergen: verrijking van alle detail-tabbladen (niet-Pivot) met locatie info...")
    try:
        result_sheets = merge_all_sheets(
            str(input_k),
            df_kasten,
            uuid_col_keuring=args.uuid_col_keuring,
            uuid_col_kasten=args.uuid_col_kasten,
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

    print(f"Gereed. Output met alle tabbladen (verrijkt) weggeschreven naar {out_s}")


if __name__ == "__main__":
    main()
