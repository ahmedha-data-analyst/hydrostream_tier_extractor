<div align="center">
  <img src="logo.png" width="120" alt="HydroStream Logo">

  # HydroStream Tier Extractor
  **Tier 2 & Tier 3 Dataset Builder**

  ![Python](https://img.shields.io/badge/Python-%3E%3D3.9-blue)
  ![Version](https://img.shields.io/badge/version-1.0.0-green)
  ![License](https://img.shields.io/badge/license-CC--BY--4.0-lightgrey)

  *Environment Agency (England) Open Water Quality Archive — Subset Extractor*
</div>

> **Authors:** Domanique Bridglalsingh, Ahmed Abdalla, Jia Hu, Geyong Min, Xiaohong Li, and Siwei Zheng  
> **Website:** [www.hydrostar-eu.com](http://www.hydrostar-eu.com)

---

## Purpose

After running HydroStream in `full` mode to produce the `EA_clean_2000_2025_full.parquet` file, this utility derives specialized dataset subsets directly from that parquet. 

It acts as a fast filter-and-summarise pass. There is no need to re-stream the raw CSV files; all cleaning, coordinate conversion, unit standardisation, and outlier flagging have already been processed upstream.

---

## Dependencies

The script automatically installs missing Python packages. Core libraries include:

<p>
  <img src="https://img.shields.io/badge/pandas-150458?logo=pandas&logoColor=white" alt="pandas">
  <img src="https://img.shields.io/badge/numpy-013243?logo=numpy&logoColor=white" alt="numpy">
  <img src="https://img.shields.io/badge/pyarrow-2C3E50" alt="pyarrow">
  <img src="https://img.shields.io/badge/openpyxl-217346" alt="openpyxl">
</p>

---

## Output Modes

The extractor supports two specialised dataset modes:

* **`contaminants` (Tier 2):** Extracts emerging contaminants (microplastics, PFAS, insecticides, pesticides). Automatically adds a harmonised `Analyte` column that collapses lexical variants (e.g., "- linear", ": Wet Wt") into a single analyte name while preserving chemically distinct isomers.
* **`electrochemistry` (Tier 3):** Extracts a curated set of 59 physicochemical parameters and ionic species highly relevant to electrochemical sensing and water-treatment modelling.

---

## How to Use

**1. Directory Setup** Prepare your working directory. Ensure the full clean parquet from the main HydroStream processor is present:

    Working Directory/
    ├── EA_clean_2000_2025_full.parquet
    └── your_notebook.ipynb

**2. Execution** Create a Jupyter Notebook in the same working directory as the `.parquet` file and run the function:

    from hydrostream_tier import hydrostream_tier

    # For Tier 2 — Emerging Contaminants
    result = hydrostream_tier(
        input_dir=".",
        mode="contaminants"
    )

    # For Tier 3 — Electrochemistry
    result = hydrostream_tier(
        input_dir=".",
        mode="electrochemistry"
    )

**3. Results** After running the function, the derived subset files will be saved automatically in an `EA_processed_output/` folder within the same working directory.

---

## Outputs

All outputs are saved in the `EA_processed_output/` directory and tagged by their respective mode.

| Output Type | Format | Description |
| :--- | :---: | :--- |
| **Subset Dataset** | `.csv`, `.parquet` | The filtered analysis-ready subset. |
| **Statistics** | `.xlsx` | Descriptive statistics specific to the tier. |
| **QA Report** | `.html` | Visual quality assurance summary. |
| **Processing Log**| `.txt` | Detailed execution log. |

---

## Processing Features

Because upstream cleaning is already complete, this utility focuses on:

* Fast temporal filtering by year range.
* Precise parameter and category isolation.
* Lexical harmonisation of complex analyte names (Tier 2).
* Generation of targeted coverage, outlier, and seasonal statistics.

---

## Performance

* **Extremely Fast:** Completes processing in seconds rather than minutes.
* **Efficient:** Leverages compressed `.parquet` columnar formats for rapid loading and filtering.
