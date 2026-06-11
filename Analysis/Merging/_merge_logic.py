"""Core merge logic for adding locatie score and klasse columns (Analysis/Merging).

This module is intended to be imported by scripts in the same folder.
"""
from typing import Optional
import pandas as pd
import re


def load_sheet_as_df(path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Load Excel or CSV into a DataFrame.

    If `sheet_name` is None, read the first sheet (sheet 0) to avoid pandas returning a dict.
    """
    if str(path).lower().endswith(".csv"):
        return pd.read_csv(path, dtype=object)
    if sheet_name is None:
        return pd.read_excel(path, sheet_name=0, dtype=object)
    return pd.read_excel(path, sheet_name=sheet_name, dtype=object)


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


def add_locatie_klassen(
    keuringsinfo_df: pd.DataFrame,
    kasten_df: pd.DataFrame,
    uuid_col_keuring: str = "uuid",
    uuid_col_kasten: str = "uuid",
    col_eindscore: str = "eindscore",
    col_klassen_log_5: str = "klassen_log_5",
    out_col_score: str = "score_locatie",
    out_col_klasse: str = "klasse_locatie_tot_5",
) -> pd.DataFrame:
    df_left = keuringsinfo_df.copy()
    df_right = kasten_df.copy()
    # Ensure string uuids
    df_left[uuid_col_keuring] = df_left[uuid_col_keuring].astype(object).astype(str)
    df_right[uuid_col_kasten] = df_right[uuid_col_kasten].astype(object).astype(str)

    missing = [c for c in (col_eindscore, col_klassen_log_5) if c not in df_right.columns]
    if missing:
        raise KeyError(f"Source kasten dataframe is missing columns: {missing}")

    # Select only the columns we need from kasten, excluding any uuid columns that might conflict
    right_select = df_right[[uuid_col_kasten, col_eindscore, col_klassen_log_5]].copy()
    right_select = right_select.rename(
        columns={
            uuid_col_kasten: "_merge_key_temp",
            col_eindscore: out_col_score,
            col_klassen_log_5: out_col_klasse,
        }
    )

    merged = df_left.merge(
        right_select,
        how="left",
        left_on=uuid_col_keuring,
        right_on="_merge_key_temp",
        suffixes=(None, "_r"),
    )

    # Drop the temporary merge key
    if "_merge_key_temp" in merged.columns:
        merged = merged.drop(columns=["_merge_key_temp"])

    return merged


def merge_all_sheets(
    keuringsinfo_path: str,
    kasten_df: pd.DataFrame,
    uuid_col_keuring: str = "uuid",
    uuid_col_kasten: str = "uuid",
    col_eindscore: str = "eindscore",
    col_klassen_log_5: str = "klassen_log_5",
    out_col_score: str = "score_locatie",
    out_col_klasse: str = "klasse_locatie_tot_5",
    sheet_keuringsinfo: Optional[str] = None,
) -> dict:
    """Load all sheets from keuringsinfo Excel, enrich detail sheets (non-Pivot) with locatie columns, return dict of sheets.

    Pivot sheets are left untouched. Detail sheets (those not starting with 'Pivot') are enriched with location info
    if they contain the uuid_col_keuring column.

    If `sheet_keuringsinfo` is provided, only that sheet is enriched (plus any Pivot sheets are preserved).
    If a detail sheet does not have `uuid_col_keuring`, auto-detect a UUID column via regex.
    """
    # Load all sheets
    all_sheets = pd.read_excel(keuringsinfo_path, sheet_name=None, dtype=object)

    result_sheets = {}
    for sheet_name, df in all_sheets.items():
        if sheet_name.startswith("Pivot"):
            # Keep Pivot sheets as-is
            result_sheets[sheet_name] = df
            continue

        # If a specific sheet is requested, only process that one (skip other detail sheets)
        if sheet_keuringsinfo is not None and sheet_name != sheet_keuringsinfo:
            result_sheets[sheet_name] = df
            continue

        # Determine uuid column for this sheet
        effective_uuid_col = uuid_col_keuring
        if uuid_col_keuring not in df.columns:
            found_col, found_count = find_uuid_column(df)
            if found_col:
                print(f"  Auto-gedetecteerde uuid-kolom in sheet '{sheet_name}': {found_col} (matches: {found_count})")
                effective_uuid_col = found_col
            else:
                print(f"  Waarschuwing: geen uuid-kolom gevonden in sheet '{sheet_name}', overslaan.")
                result_sheets[sheet_name] = df
                continue

        # Try to enrich detail sheet
        try:
            enriched = add_locatie_klassen(
                df,
                kasten_df,
                uuid_col_keuring=effective_uuid_col,
                uuid_col_kasten=uuid_col_kasten,
                col_eindscore=col_eindscore,
                col_klassen_log_5=col_klassen_log_5,
                out_col_score=out_col_score,
                out_col_klasse=out_col_klasse,
            )
            result_sheets[sheet_name] = enriched
        except KeyError as e:
            print(f"  Waarschuwing: kon sheet '{sheet_name}' niet verrijken: {e}")
            result_sheets[sheet_name] = df

    return result_sheets


def add_inbreuken(
    keuringsinfo_df: pd.DataFrame,
    inbreuken_df: pd.DataFrame,
    uuid_col_keuring: str = "uuid",
    uuid_col_inbreuken: str = "arango_uuid",
    inbreuken_sheet: str = "latest_asset_summary",
) -> pd.DataFrame:
    """Add infraction columns from inbreuken data to keuringsinfo DataFrame.

    Merges on uuid column, adding:
    - full_infraction_texts_latest
    - cat_F0 through cat_F67 (all cat_F* columns present in inbreuken)

    Args:
        keuringsinfo_df: DataFrame from keuringsinfo (must have uuid column)
        inbreuken_df: DataFrame from inbreuken latest_asset_summary sheet
        uuid_col_keuring: Column name in keuringsinfo for UUID (default: uuid)
        uuid_col_inbreuken: Column name in inbreuken for UUID (default: arango_uuid)
        inbreuken_sheet: Sheet name (for documentation purposes)

    Returns:
        Merged DataFrame with infraction columns added
    """
    df_left = keuringsinfo_df.copy()
    df_right = inbreuken_df.copy()

    # Ensure string uuids
    df_left[uuid_col_keuring] = df_left[uuid_col_keuring].astype(object).astype(str)
    df_right[uuid_col_inbreuken] = df_right[uuid_col_inbreuken].astype(object).astype(str)

    # Find all cat_F columns
    cat_cols = [c for c in df_right.columns if c.startswith("cat_F")]
    if not cat_cols:
        raise KeyError("No cat_F* columns found in inbreuken dataframe")

    # Check for full_infraction_texts_latest
    if "full_infraction_texts_latest" not in df_right.columns:
        raise KeyError("Source inbreuken dataframe is missing column: full_infraction_texts_latest")

    # Select columns to merge, using a temporary key to avoid column conflicts
    cols_to_select = [uuid_col_inbreuken, "full_infraction_texts_latest"] + cat_cols
    right_select = df_right[cols_to_select].copy()
    right_select = right_select.rename(columns={uuid_col_inbreuken: "_merge_key_temp"})

    merged = df_left.merge(
        right_select,
        how="left",
        left_on=uuid_col_keuring,
        right_on="_merge_key_temp",
        suffixes=(None, "_r"),
    )

    # Drop the temporary merge key
    if "_merge_key_temp" in merged.columns:
        merged = merged.drop(columns=["_merge_key_temp"])

    return merged


def merge_all_sheets_with_inbreuken(
    keuringsinfo_path: str,
    inbreuken_df: pd.DataFrame,
    uuid_col_keuring: str = "uuid",
    uuid_col_inbreuken: str = "arango_uuid",
    sheet_keuringsinfo: Optional[str] = None,
) -> dict:
    """Load all sheets from keuringsinfo Excel, enrich detail sheets with infraction columns.

    Pivot sheets are left untouched. Detail sheets (those not starting with 'Pivot') are enriched
    with infraction data if they contain the uuid_col_keuring column.

    Args:
        keuringsinfo_path: Path to keuringsinfo Excel file
        inbreuken_df: DataFrame from inbreuken latest_asset_summary sheet
        uuid_col_keuring: Column name in keuringsinfo for UUID (default: uuid)
        uuid_col_inbreuken: Column name in inbreuken for UUID (default: arango_uuid)
        sheet_keuringsinfo: Optional specific sheet to process

    Returns:
        Dictionary of sheet names to DataFrames
    """
    # Load all sheets
    all_sheets = pd.read_excel(keuringsinfo_path, sheet_name=None, dtype=object)

    result_sheets = {}
    for sheet_name, df in all_sheets.items():
        if sheet_name.startswith("Pivot"):
            # Keep Pivot sheets as-is
            result_sheets[sheet_name] = df
            continue

        # If a specific sheet is requested, only process that one (skip other detail sheets)
        if sheet_keuringsinfo is not None and sheet_name != sheet_keuringsinfo:
            result_sheets[sheet_name] = df
            continue

        # Determine uuid column for this sheet
        effective_uuid_col = uuid_col_keuring
        if uuid_col_keuring not in df.columns:
            found_col, found_count = find_uuid_column(df)
            if found_col:
                print(f"  Auto-gedetecteerde uuid-kolom in sheet '{sheet_name}': {found_col} (matches: {found_count})")
                effective_uuid_col = found_col
            else:
                print(f"  Waarschuwing: geen uuid-kolom gevonden in sheet '{sheet_name}', overslaan.")
                result_sheets[sheet_name] = df
                continue

        # Try to enrich detail sheet
        try:
            enriched = add_inbreuken(
                df,
                inbreuken_df,
                uuid_col_keuring=effective_uuid_col,
                uuid_col_inbreuken=uuid_col_inbreuken,
            )
            result_sheets[sheet_name] = enriched
        except KeyError as e:
            print(f"  Waarschuwing: kon sheet '{sheet_name}' niet verrijken: {e}")
            result_sheets[sheet_name] = df

    return result_sheets
