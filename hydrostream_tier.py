"""
===============================================================================
HYDROSTREAM TIER EXTRACTOR  —  TIER 2 & TIER 3 DATASET BUILDER
===============================================================================
Title   : HydroStream Tier Extractor — derives the two specialised subsets
          from the full cleaned parquet produced by HydroStream
Version : 1.0.0
Authors : Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min,
          Xiaohong Li, and Siwei Zheng
Licence : CC-BY-4.0  (same licence as the underlying EA data)
Python  : >= 3.9

PURPOSE
-------
After running HydroStream once in "full" mode — which takes the 26 raw
yearly EA CSV files and produces `EA_clean_2000_2025_full.parquet` —
this utility derives either of the two specialised datasets directly
from that parquet.  There is no need to re-stream the raw CSVs: all
cleaning, coordinate conversion, unit standardisation, category
mapping, and outlier flagging have already been done upstream, so this
step is a fast filter-and-summarise pass (seconds rather than minutes).

TWO OUTPUT MODES
----------------
1. "contaminants"      (Tier 2 — Emerging Contaminant Module)
     Extracts every row whose Category is
     "microplastics, nanoplastic, pfas, insecticide, pesticide, or similar".
     Adds a harmonised `Analyte` column that collapses lexical variants
     such as
         "Perfluorohexanesulphonic acid - linear"
         "Perfluorohexanesulphonic acid - branched"
         "Perfluorohexanesulphonic acid : Wet Wt"
     into a single analyte name, so multi-decadal trend analyses are
     not fragmented by nomenclature shifts.  Chemically-distinct
     positional isomers (e.g. "DDT -pp" vs "DDT -op") are preserved.

2. "electrochemistry"  (Tier 3 — Physicochemical & Electrochemical Subset)
     Extracts the curated set of physicochemical parameters and ionic
     species most relevant to electrochemical sensing and water-
     treatment modelling: dissolved metals, pH, conductivity, major
     anions, nutrients, dissolved oxygen, BOD, and salinity.  The test
     list is identical to the one used by HydroStream in its own
     "electrochemistry" mode, so the two entry points give bit-for-bit
     identical outputs.

HOW TO USE  (from a Jupyter notebook in the same folder as the parquet)
-----------------------------------------------------------------------
    from hydrostream_tier import hydrostream_tier

    # Tier 2 — contaminants
    result = hydrostream_tier(input_dir=".", mode="contaminants")

    # Tier 3 — electrochemistry
    result = hydrostream_tier(input_dir=".", mode="electrochemistry")

OUTPUTS  (saved in <input_dir>/EA_processed_output/)
----------------------------------------------------
  • EA_clean_2000_2025_<mode>.csv       – The subset dataset.
  • EA_clean_2000_2025_<mode>.parquet   – Same data, columnar format.
  • EA_statistics_2000_2025_<mode>.xlsx – Descriptive statistics.
  • EA_qa_report_<mode>.html            – Visual quality-assurance summary.
  • EA_processing_log_<mode>.txt        – Full text log.

DEPENDENCIES  (auto-installed if missing)
-----------------------------------------
  pandas, numpy, pyarrow, openpyxl
===============================================================================
"""

# ============================================================================
# STEP 0 — AUTOMATICALLY INSTALL MISSING LIBRARIES
# ============================================================================

def _ensure_dependencies():
    """Install any missing Python packages required by this script."""
    import subprocess, sys, importlib
    REQUIRED = {
        "pandas": "pandas", "numpy": "numpy",
        "pyarrow": "pyarrow", "openpyxl": "openpyxl",
    }
    missing = [pip for imp, pip in REQUIRED.items()
               if not importlib.util.find_spec(imp)]
    if missing:
        print(f"Installing missing packages: {', '.join(missing)} ...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet",
             "--break-system-packages", *missing],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("  Done.\n")

_ensure_dependencies()

# ============================================================================
# IMPORTS
# ============================================================================

from pathlib import Path
import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
from datetime import datetime
import warnings, io, re

warnings.filterwarnings("ignore")


# ============================================================================
#  TIER 3 — ELECTROCHEMISTRY TEST SET
#  (identical to HydroStream's ELECTROCHEMISTRY_TESTS — keeping these
#  in sync is essential: running HydroStream in electrochemistry mode
#  and running this tier extractor in electrochemistry mode must give
#  identical results.)
# ============================================================================

ELECTROCHEMISTRY_TESTS = {
    # --- Dissolved metals / ionic species (21) ---
    "Magnesium, Dissolved", "Copper, Dissolved", "Nickel, Dissolved",
    "Iron, Dissolved", "Manganese, Dissolved", "Uranium, Dissolved",
    "Lithium, Dissolved", "Potassium, Dissolved", "Sodium, Dissolved",
    "Lead, Dissolved", "Cadmium, Dissolved", "Mercury, Dissolved",
    "Silver, Dissolved", "Barium, Dissolved", "Zinc, Dissolved",
    "Chromium, Dissolved", "Arsenic, Dissolved", "Calcium, Dissolved",
    "Boron, Dissolved", "Aluminium, Dissolved", "Strontium, Filtered",
    # --- Total metals (18) ---
    "Magnesium", "Copper", "Nickel", "Iron", "Manganese",
    "Potassium", "Sodium", "Lead", "Cadmium", "Mercury",
    "Silver", "Barium", "Zinc", "Chromium", "Arsenic",
    "Calcium", "Boron", "Aluminium",
    # --- Physical chemistry (5) ---
    "pH", "Conductivity at 25 C", "Conductivity at 20 C",
    "Temperature of Water", "Turbidity",
    # --- Major anions and nutrients (8) ---
    "Chloride", "Ammoniacal Nitrogen as N",
    "Nitrogen, Total Oxidised as N", "Orthophosphate, reactive as P",
    "Nitrate as N", "Nitrite as N", "Sulphate as SO4", "Fluoride",
    # --- Dissolved oxygen, load & condition indicators (7) ---
    "Oxygen, Dissolved as O2", "Oxygen, Dissolved, % Saturation",
    "Alkalinity to pH 4.5 as CaCO3", "Hardness, Total as CaCO3",
    "Solids, Suspended at 105 C", "BOD : 5 Day ATU",
    "Salinity : In Situ",
}  # ← 59 tests total

# ============================================================================
#  TIER 2 — CONTAMINANT CATEGORY LABEL
# ============================================================================

CONTAMINANTS_CATEGORY = \
    "microplastics, nanoplastic, pfas, insecticide, pesticide, or similar"


# ============================================================================
#  ANALYTE HARMONISATION  —  collapse lexical variants
# ============================================================================
#  Strips trailing matrix qualifiers and isomer-branch labels from Test
#  names so that variants like
#      "Perfluorohexanesulphonic acid - linear"
#      "Perfluorohexanesulphonic acid - branched"
#      "Perfluorohexanesulphonic acid : Wet Wt"
#  all collapse to the same "Perfluorohexanesulphonic acid" analyte.
#
#  IMPORTANT: positional isomers like "DDT -pp" vs "DDT -op" and
#  HCH variants like " -alpha" / " -beta" / " -gamma" are NOT collapsed
#  because they are chemically distinct substances, not lexical variants.

_ANALYTE_STRIP_PATTERNS = [
    r"\s*:\s*Wet\s*Wt\s*$",
    r"\s*:\s*Dry\s*Wt\s*$",
    r"\s*:\s*WW\s*$",
    r"\s*:\s*DW\s*$",
    r"\s*:\s*Wet\s*weight\s*$",
    r"\s*:\s*Dry\s*weight\s*$",
    r"\s*-\s*linear\s*$",
    r"\s*-\s*branched\s*$",
]
_ANALYTE_RE = [re.compile(p, re.IGNORECASE) for p in _ANALYTE_STRIP_PATTERNS]


def _harmonise_analyte(name) -> str:
    """Strip trailing matrix/branch qualifiers from a Test name."""
    x = str(name)
    # Iterate a couple of times to peel off stacked suffixes like
    # "...: Wet Wt : Dry Wt" that occasionally appear in the raw data.
    for _ in range(3):
        changed = False
        for pat in _ANALYTE_RE:
            new_x = pat.sub("", x)
            if new_x != x:
                x = new_x
                changed = True
        if not changed:
            break
    return x.strip()


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def hydrostream_tier(
    input_dir: "str | Path" = ".",
    mode: str = "electrochemistry",
    full_parquet: "str | Path | None" = None,
    years: range = range(2000, 2026),
    add_analyte_column: bool = True,
    generate_stats: bool = True,
    generate_qa_report: bool = True,
    save_log: bool = True,
) -> Dict[str, Any]:
    """
    Derive a Tier 2 (contaminants) or Tier 3 (electrochemistry) subset
    from the HydroStream full clean parquet.

    Parameters
    ----------
    input_dir : str or Path, default "."
        Folder containing `EA_clean_2000_2025_full.parquet` — typically
        the same folder as the Jupyter notebook.

    mode : str, default "electrochemistry"
        "contaminants"      → Tier 2, emerging-contaminants subset.
        "electrochemistry"  → Tier 3, physicochemical & electrochemical subset.

    full_parquet : str, Path, or None, default None
        Explicit path to the full clean parquet.  If None, the function
        auto-detects it in `input_dir` and in `input_dir/EA_processed_output/`.

    years : range, default range(2000, 2026)
        Restrict the output to this range of SourceYears.

    add_analyte_column : bool, default True
        In contaminants mode, add a harmonised `Analyte` column that
        collapses " - linear", " - branched", " : Wet Wt", " : Dry Wt"
        variants.  Ignored in electrochemistry mode.

    generate_stats : bool, default True
    generate_qa_report : bool, default True
    save_log : bool, default True

    Returns
    -------
    dict with output paths and dataset metrics.
    """

    warnings.filterwarnings("ignore")

    # ------------------------------------------------------------------
    # Log capture
    # ------------------------------------------------------------------
    log_buffer = io.StringIO()
    def log(msg: str = ""):
        print(msg); log_buffer.write(msg + "\n")

    # ------------------------------------------------------------------
    # Resolve directories and mode
    # ------------------------------------------------------------------
    input_dir = Path(input_dir).resolve()
    out_dir   = input_dir / "EA_processed_output"
    out_dir.mkdir(parents=True, exist_ok=True)

    mode = mode.strip().lower()
    if mode not in ("electrochemistry", "contaminants"):
        raise ValueError(
            f"mode must be 'electrochemistry' or 'contaminants', got '{mode}'"
        )

    tier_label = ("Tier 2 — Emerging Contaminant Module"
                  if mode == "contaminants"
                  else "Tier 3 — Physicochemical & Electrochemical Subset")

    # ------------------------------------------------------------------
    # Locate the full clean parquet
    # ------------------------------------------------------------------
    if full_parquet is not None:
        parquet_path = Path(full_parquet).resolve()
    else:
        candidates = [
            input_dir / "EA_clean_2000_2025_full.parquet",
            out_dir   / "EA_clean_2000_2025_full.parquet",
        ]
        parquet_path = next((c for c in candidates if c.exists()), None)

    if parquet_path is None or not parquet_path.exists():
        raise FileNotFoundError(
            "\n\n"
            "============================================================\n"
            "  FULL CLEAN PARQUET NOT FOUND\n"
            "============================================================\n"
            f"  Looked in:\n"
            f"    {input_dir}/EA_clean_2000_2025_full.parquet\n"
            f"    {out_dir}/EA_clean_2000_2025_full.parquet\n\n"
            "  This file is produced by running HydroStream in 'full' mode.\n\n"
            "  To fix:\n"
            "    • Run HydroStream first (mode='full'), OR\n"
            "    • Place the existing parquet in the input folder, OR\n"
            "    • Pass its path explicitly via the 'full_parquet' parameter.\n"
            "============================================================\n"
        )

    # ==================================================================
    #  BANNER
    # ==================================================================

    log("=" * 70)
    log("  HYDROSTREAM TIER EXTRACTOR   v1.0")
    log("=" * 70)
    log(f"  {tier_label}")
    log(f"  Mode            : {mode.upper()}")
    log(f"  Years           : {min(years)} – {max(years)}")
    log(f"  Input parquet   : {parquet_path.name}")
    log(f"  Input folder    : {input_dir}")
    log(f"  Output folder   : {out_dir}")
    log(f"  Started at      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)
    log()

    # ==================================================================
    #  LOAD THE FULL CLEAN PARQUET
    # ==================================================================

    log("Loading full clean parquet …")
    df = pd.read_parquet(parquet_path)
    n_input = len(df)
    log(f"  Rows loaded : {n_input:,}")
    log(f"  Columns     : {list(df.columns)}")
    log()

    required = {"Test", "result", "Date", "Sampling Point", "Type", "Unit",
                "Season", "SourceYear"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Input parquet is missing required columns: {sorted(missing)}.\n"
            f"Expected a parquet produced by HydroStream v2.3 or later."
        )

    if mode == "contaminants" and "Category" not in df.columns:
        raise ValueError(
            "Contaminants mode requires a 'Category' column in the parquet.\n"
            "Re-run HydroStream with 'List of tests kept and categories.xlsx' "
            "present in the input folder to populate it."
        )

    # ==================================================================
    #  YEAR FILTER
    # ==================================================================

    yr_set = set(years)
    yr_mask = df["SourceYear"].isin(yr_set)
    n_dropped_years = int((~yr_mask).sum())
    if n_dropped_years:
        df = df[yr_mask]
        log(f"Year filter ({min(years)}–{max(years)}): "
            f"dropped {n_dropped_years:,} rows outside the range.")
        log()

    # ==================================================================
    #  MODE-SPECIFIC FILTER
    # ==================================================================

    if mode == "contaminants":
        log(f"Filtering to Category = '{CONTAMINANTS_CATEGORY}' …")
        mask = df["Category"] == CONTAMINANTS_CATEGORY
        df_sub = df[mask].copy()
        log(f"  Rows matching contaminants category : {len(df_sub):>12,}")
        log(f"  Unique contaminant tests            : {df_sub['Test'].nunique():>12,}")
        log()

        if add_analyte_column and len(df_sub):
            log("Harmonising Analyte column …")
            df_sub["Analyte"] = df_sub["Test"].map(_harmonise_analyte)
            n_variants = int((df_sub["Analyte"] != df_sub["Test"]).sum())
            n_tests    = df_sub["Test"].nunique()
            n_analytes = df_sub["Analyte"].nunique()
            log(f"  Rows with name collapsed          : {n_variants:>12,}")
            log(f"  Unique Test names                 : {n_tests:>12,}")
            log(f"  Unique harmonised Analytes        : {n_analytes:>12,}")
            log(f"  Reduction (Test → Analyte)        : {n_tests - n_analytes:>12,}")
            log()

    else:  # electrochemistry
        log(f"Filtering to electrochemistry test set "
            f"({len(ELECTROCHEMISTRY_TESTS)} tests) …")
        mask = df["Test"].isin(ELECTROCHEMISTRY_TESTS)
        df_sub = df[mask].copy()
        log(f"  Rows matching electrochemistry tests : {len(df_sub):>12,}")
        log(f"  Unique tests in subset               : {df_sub['Test'].nunique():>12,}")

        found = set(df_sub["Test"].unique()) if len(df_sub) else set()
        absent = sorted(ELECTROCHEMISTRY_TESTS - found)
        if absent:
            log(f"  Tests requested but absent in full  : {len(absent)}")
            for t in absent:
                log(f"    · {t}")
        log()

    # ==================================================================
    #  SUMMARY
    # ==================================================================

    n_final = len(df_sub)
    n_removed_mode = n_input - n_dropped_years - n_final
    pct_kept = (n_final / n_input * 100) if n_input else 0

    log("── Extraction summary " + "─" * 47)
    log(f"  Rows in full clean parquet  : {n_input:>12,}")
    if n_dropped_years:
        log(f"  Dropped by year filter      : {n_dropped_years:>12,}")
    log(f"  Dropped by mode filter      : {n_removed_mode:>12,}")
    log(f"  Rows in '{mode}' subset     : {n_final:>12,}  "
        f"({pct_kept:.2f}% of full)")
    log("─" * 70 + "\n")

    if n_final == 0:
        raise ValueError(
            "Filter produced 0 rows.  Verify that the parquet was produced "
            "by HydroStream v2.3+ and that the chosen mode is correct."
        )

    # ==================================================================
    #  OUTPUT PATHS
    # ==================================================================

    tag = mode
    out_csv   = out_dir / f"EA_clean_2000_2025_{tag}.csv"
    out_pq    = out_dir / f"EA_clean_2000_2025_{tag}.parquet"
    out_stats = out_dir / f"EA_statistics_2000_2025_{tag}.xlsx"
    out_qa    = out_dir / f"EA_qa_report_{tag}.html"
    out_log   = out_dir / f"EA_processing_log_{tag}.txt"

    for p in (out_csv, out_pq):
        if p.exists(): p.unlink()

    SEASON_CATS = ["Winter", "Spring", "Summer", "Autumn"]
    if "Season" in df_sub.columns:
        df_sub["Season"] = pd.Categorical(
            df_sub["Season"], categories=SEASON_CATS, ordered=True)

    # ==================================================================
    #  SAVE MAIN OUTPUTS
    # ==================================================================

    log("Saving outputs …")
    df_sub.to_csv(out_csv, index=False)
    log(f"  ✓  CSV saved     : {out_csv.name}")

    wrote_parquet = False
    try:
        df_sub.to_parquet(out_pq, engine="pyarrow", compression="zstd")
        wrote_parquet = True
        log(f"  ✓  Parquet saved : {out_pq.name}")
    except Exception as e:
        log(f"  ⚠  Parquet skipped: {e}")

    # ==================================================================
    #  STATISTICS
    # ==================================================================

    stats_output = None
    if generate_stats:
        log("\nGenerating statistics …")
        try:
            with pd.ExcelWriter(out_stats, engine="openpyxl") as writer:
                # ---- Test-level stats ------------------------------------
                grp_cols = ["Test", "Unit"]
                if "Category" in df_sub.columns:
                    grp_cols = ["Category"] + grp_cols
                test_stats = (df_sub.groupby(grp_cols)["result"]
                    .agg(["count","min","max","mean","median","std",
                          ("p10", lambda x: x.quantile(0.10)),
                          ("p25", lambda x: x.quantile(0.25)),
                          ("p75", lambda x: x.quantile(0.75)),
                          ("p90", lambda x: x.quantile(0.90))])
                    .round(4).reset_index())
                test_stats.to_excel(writer, sheet_name="Test_Statistics", index=False)

                # ---- Analyte-level stats (contaminants only) -------------
                if mode == "contaminants" and "Analyte" in df_sub.columns:
                    analyte_stats = (df_sub.groupby(["Analyte","Unit"])["result"]
                        .agg(["count","min","max","mean","median","std",
                              ("p10", lambda x: x.quantile(0.10)),
                              ("p90", lambda x: x.quantile(0.90))])
                        .round(4).reset_index())
                    analyte_stats.to_excel(
                        writer, sheet_name="Analyte_Statistics", index=False)

                # ---- Type × Test ----------------------------------------
                type_stats = (df_sub.groupby(["Type","Test"])["result"]
                    .agg(["count","mean","median","std"]).round(4).reset_index())
                type_stats.to_excel(writer, sheet_name="Type_Test_Stats", index=False)

                # ---- Seasonal -------------------------------------------
                season_stats = (df_sub.groupby(["Season","Test"])["result"]
                    .agg(["count","mean","median"]).round(4).reset_index())
                season_stats.to_excel(writer, sheet_name="Seasonal_Stats", index=False)

                # ---- Coverage -------------------------------------------
                cov_items = [
                    ("Total Rows", len(df_sub)),
                    ("Unique Sampling Points", df_sub["Sampling Point"].nunique()),
                    ("Unique Tests", df_sub["Test"].nunique()),
                    ("Unique Types", df_sub["Type"].nunique()),
                    ("Unique Units", df_sub["Unit"].nunique()),
                    ("Date Range Start", str(df_sub["Date"].min().date())),
                    ("Date Range End", str(df_sub["Date"].max().date())),
                    ("Years Covered", df_sub["SourceYear"].nunique()),
                    ("Mode", mode.upper()),
                    ("Tier", "Tier 2" if mode == "contaminants" else "Tier 3"),
                ]
                if mode == "contaminants" and "Analyte" in df_sub.columns:
                    cov_items.insert(3, ("Unique Analytes (harmonised)",
                                         df_sub["Analyte"].nunique()))
                coverage = pd.DataFrame(cov_items, columns=["Metric","Value"])
                coverage.to_excel(writer, sheet_name="Coverage", index=False)

                # ---- Outliers -------------------------------------------
                if "outlier_flag" in df_sub.columns and df_sub["outlier_flag"].any():
                    (df_sub[df_sub["outlier_flag"]]
                        .groupby(["Test","Type"])
                        .agg(count=("result","size"),
                             min_val=("result","min"),
                             max_val=("result","max"))
                        .reset_index()
                        .to_excel(writer, sheet_name="Outliers", index=False))

                # ---- Rows per year --------------------------------------
                (df_sub.groupby("SourceYear").size().reset_index(name="rows")
                    .to_excel(writer, sheet_name="Rows_Per_Year", index=False))

                # ---- Extraction summary ---------------------------------
                extraction = pd.DataFrame([
                    ("Full clean parquet rows", n_input),
                    ("Dropped by year filter",  n_dropped_years),
                    ("Dropped by mode filter",  n_removed_mode),
                    (f"'{mode}' subset rows",   n_final),
                    ("Subset as % of full",     f"{pct_kept:.2f}%"),
                ], columns=["Metric","Value"])
                extraction.to_excel(writer, sheet_name="Extraction_Summary", index=False)

            stats_output = out_stats
            log(f"  ✓  Statistics saved : {out_stats.name}")
        except Exception as e:
            log(f"  ⚠  Statistics failed: {e}")

    # ==================================================================
    #  QA REPORT
    # ==================================================================

    qa_output = None
    if generate_qa_report:
        log("\nGenerating QA report …")
        try:
            n_outliers = int(df_sub["outlier_flag"].sum()) if "outlier_flag" in df_sub.columns else 0
            pct_outliers = (n_outliers / len(df_sub)) * 100

            type_rows = "".join(
                f"<tr><td>{t}</td><td>{c:,}</td><td>{c/len(df_sub)*100:.1f}%</td></tr>\n"
                for t, c in df_sub["Type"].value_counts().head(15).items())

            test_rows = "".join(
                f"<tr><td>{t}</td><td>{c:,}</td><td>{c/len(df_sub)*100:.1f}%</td></tr>\n"
                for t, c in df_sub["Test"].value_counts().head(20).items())

            unit_dist_rows = "".join(
                f"<tr><td>{u}</td><td>{c:,}</td><td>{c/len(df_sub)*100:.1f}%</td></tr>\n"
                for u, c in df_sub["Unit"].value_counts().head(25).items())

            analyte_section = ""
            if mode == "contaminants" and "Analyte" in df_sub.columns:
                analyte_rows = "".join(
                    f"<tr><td>{a}</td><td>{c:,}</td><td>{c/len(df_sub)*100:.1f}%</td></tr>\n"
                    for a, c in df_sub["Analyte"].value_counts().head(20).items())
                analyte_section = f"""
<h2>Top Harmonised Analytes</h2>
<p><i>The <b>Analyte</b> column collapses lexical variants such as
"- linear", "- branched", ": Wet Wt", ": Dry Wt" into a single name.
Positional isomers (e.g. DDT -pp vs DDT -op, HCH -alpha/-beta/-gamma)
are intentionally preserved because they are chemically distinct.</i></p>
<table><tr><th>Analyte</th><th>Rows</th><th>%</th></tr>{analyte_rows}</table>"""

            extra_meta = ""
            if mode == "contaminants" and "Analyte" in df_sub.columns:
                extra_meta = (
                    f"<tr><td>Unique harmonised analytes</td>"
                    f"<td>{df_sub['Analyte'].nunique()}</td></tr>\n")

            lat_rows = df_sub["Latitude"].notna().sum() if "Latitude" in df_sub.columns else 0
            lat_pct  = (df_sub["Latitude"].notna().mean() * 100) if "Latitude" in df_sub.columns else 0

            qa_html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>HydroStream — QA Report ({mode.upper()})</title>
<style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 2em; color: #222; }}
  h1 {{ color: #1a5276; }} h2 {{ color: #2c3e50; border-bottom: 2px solid #2980b9; padding-bottom: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1em 0 2em; }}
  th, td {{ border: 1px solid #ccc; padding: 6px 10px; text-align: left; }}
  th {{ background: #2980b9; color: #fff; }} tr:nth-child(even) {{ background: #f4f6f7; }}
  .good {{ color: #27ae60; font-weight: bold; }}
  .bad  {{ color: #c0392b; font-weight: bold; }}
  footer {{ margin-top: 3em; color: #888; font-size: 0.85em; }}
</style></head><body>
<h1>HydroStream — {tier_label}</h1>
<p><b>Mode:</b> {mode.upper()} | <b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>

<h2>Dataset Overview</h2>
<table><tr><th>Metric</th><th>Value</th></tr>
<tr><td>Total rows</td><td>{len(df_sub):,}</td></tr>
<tr><td>Unique sampling points</td><td>{df_sub['Sampling Point'].nunique():,}</td></tr>
<tr><td>Unique tests</td><td>{df_sub['Test'].nunique()}</td></tr>
{extra_meta}<tr><td>Unique water types</td><td>{df_sub['Type'].nunique()}</td></tr>
<tr><td>Unique units</td><td>{df_sub['Unit'].nunique()}</td></tr>
<tr><td>Date range</td><td>{df_sub['Date'].min().date()} → {df_sub['Date'].max().date()}</td></tr>
<tr><td>Years covered</td><td>{df_sub['SourceYear'].min()} – {df_sub['SourceYear'].max()}</td></tr>
<tr><td>Records with coordinates</td><td>{lat_rows:,} ({lat_pct:.1f}%)</td></tr>
</table>

<h2>Extraction Summary</h2>
<table><tr><th>Metric</th><th>Value</th></tr>
<tr><td>Full clean parquet rows</td><td>{n_input:,}</td></tr>
<tr><td>Dropped by year filter</td><td>{n_dropped_years:,}</td></tr>
<tr><td>Dropped by mode filter</td><td>{n_removed_mode:,}</td></tr>
<tr><td>'{mode}' subset rows</td><td>{n_final:,}</td></tr>
<tr><td>Subset as % of full</td><td>{pct_kept:.2f}%</td></tr>
</table>

<h2>Data Quality Checks</h2>
<table><tr><th>Check</th><th>Result</th><th>Status</th></tr>
<tr><td>NaN results</td><td>{df_sub['result'].isna().sum():,}</td><td class="{'good' if df_sub['result'].isna().sum()==0 else 'bad'}">{'✓ PASS' if df_sub['result'].isna().sum()==0 else '⚠'}</td></tr>
<tr><td>NaN dates</td><td>{df_sub['Date'].isna().sum():,}</td><td class="{'good' if df_sub['Date'].isna().sum()==0 else 'bad'}">{'✓ PASS' if df_sub['Date'].isna().sum()==0 else '⚠'}</td></tr>
<tr><td>Flagged outliers (from full dataset)</td><td>{n_outliers:,} ({pct_outliers:.2f}%)</td><td class="{'good' if pct_outliers<5 else 'bad'}">{'✓ OK' if pct_outliers<5 else '⚠ CHECK'}</td></tr>
</table>

<h2>Top Water Types</h2>
<table><tr><th>Type</th><th>Rows</th><th>%</th></tr>{type_rows}</table>
<h2>Top Tests</h2>
<table><tr><th>Test</th><th>Rows</th><th>%</th></tr>{test_rows}</table>
<h2>Top Units</h2>
<table><tr><th>Unit</th><th>Rows</th><th>%</th></tr>{unit_dist_rows}</table>
{analyte_section}
<footer><p>HydroStream Tier Extractor v1.0<br>
Derived from <code>EA_clean_2000_2025_full.parquet</code> (HydroStream v2.3+)<br>
Source: Environment Agency (England) Open Water Quality Archive, 2000–2025</p></footer>
</body></html>"""

            with open(out_qa, "w", encoding="utf-8") as f:
                f.write(qa_html)
            qa_output = out_qa
            log(f"  ✓  QA report saved : {out_qa.name}")
        except Exception as e:
            log(f"  ⚠  QA report failed: {e}")

    # ==================================================================
    #  FINAL SUMMARY
    # ==================================================================

    log("\n" + "=" * 70)
    log("  EXTRACTION COMPLETE")
    log("=" * 70)
    log(f"  Tier / mode    : {tier_label}")
    log(f"  Final rows     : {len(df_sub):,}")
    log(f"  Columns        : {list(df_sub.columns)}")
    log(f"  Years          : {df_sub['SourceYear'].min()} – {df_sub['SourceYear'].max()}")
    log(f"  Tests          : {df_sub['Test'].nunique()}")
    if mode == "contaminants" and "Analyte" in df_sub.columns:
        log(f"  Analytes       : {df_sub['Analyte'].nunique()} (harmonised)")
    log(f"  Water types    : {df_sub['Type'].nunique()}")
    log(f"  Units          : {df_sub['Unit'].nunique()}")
    log(f"  Sampling points: {df_sub['Sampling Point'].nunique():,}")
    if "outlier_flag" in df_sub.columns:
        n_out = int(df_sub["outlier_flag"].sum())
        log(f"  Outliers flagged: {n_out:,} ({n_out/len(df_sub)*100:.2f}%)")
    if "Latitude" in df_sub.columns:
        n_c = int(df_sub["Latitude"].notna().sum())
        log(f"  With lat/lon   : {n_c:,} ({n_c/len(df_sub)*100:.1f}%)")

    log(f"\n  Outputs in: {out_dir}/")
    log(f"    • {out_csv.name}")
    if wrote_parquet: log(f"    • {out_pq.name}")
    if stats_output: log(f"    • {out_stats.name}")
    if qa_output:    log(f"    • {out_qa.name}")

    log(f"\n  Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 70)

    log_path = None
    if save_log:
        with open(out_log, "w", encoding="utf-8") as f:
            f.write(log_buffer.getvalue())
        log_path = out_log
        print(f"\n  ✓  Full log saved : {out_log.name}")

    return {
        "mode": mode,
        "tier": "Tier 2" if mode == "contaminants" else "Tier 3",
        "input_rows": n_input,
        "final_rows": n_final,
        "rows_dropped_by_year_filter": n_dropped_years,
        "rows_dropped_by_mode_filter": n_removed_mode,
        "output_dir": str(out_dir),
        "csv": str(out_csv),
        "parquet": str(out_pq) if wrote_parquet else None,
        "statistics": str(stats_output) if stats_output else None,
        "qa_report": str(qa_output) if qa_output else None,
        "log": str(log_path) if log_path else None,
        "data_quality": {
            "unique_sampling_points": df_sub["Sampling Point"].nunique(),
            "unique_tests": df_sub["Test"].nunique(),
            "unique_analytes": df_sub["Analyte"].nunique() if "Analyte" in df_sub.columns else None,
            "unique_types": df_sub["Type"].nunique(),
            "unique_units": df_sub["Unit"].nunique(),
            "date_range": (str(df_sub["Date"].min()), str(df_sub["Date"].max())),
            "outliers_flagged": int(df_sub["outlier_flag"].sum()) if "outlier_flag" in df_sub.columns else 0,
            "records_with_coordinates": int(df_sub["Latitude"].notna().sum()) if "Latitude" in df_sub.columns else 0,
        },
    }


# ============================================================================
# USAGE  (from a Jupyter notebook in the same folder as the parquet)
# ============================================================================

if __name__ == "__main__":

    # ── SETTINGS (EDIT THESE) ─────────────────────────────────────────
    RAW_DATA_FOLDER = "."                  # <-- folder containing the full parquet
    MODE            = "electrochemistry"   # <-- "electrochemistry" or "contaminants"
    # ──────────────────────────────────────────────────────────────────

    result = hydrostream_tier(
        input_dir          = RAW_DATA_FOLDER,
        mode               = MODE,
        full_parquet       = None,          # auto-detect in input_dir
        years              = range(2000, 2026),
        add_analyte_column = True,
        generate_stats     = True,
        generate_qa_report = True,
        save_log           = True,
    )

    print("\n" + "─" * 60)
    print("QUICK SUMMARY")
    print("─" * 60)
    print(f"  Tier       : {result['tier']}")
    print(f"  Mode       : {result['mode'].upper()}")
    print(f"  Input rows : {result['input_rows']:,}")
    print(f"  Final rows : {result['final_rows']:,}")
    print(f"  Output dir : {result['output_dir']}")
    print(f"\n  Files created:")
    for key in ["csv", "parquet", "statistics", "qa_report", "log"]:
        if result.get(key):
            print(f"    • {Path(result[key]).name}")
    print("─" * 60)
