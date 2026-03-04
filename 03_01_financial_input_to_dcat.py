#!/usr/bin/env python3
"""
DCAT Metadata Generator for Infocamere Financial Files

Automatically generates DCAT metadata for:

    data/financial/infocamere_YYYY.csv
    data/financial/infocamere_YYYY.xlsx

The metadata file is created only if it does not already exist.
"""

import json
import sys
import re
from pathlib import Path
from datetime import datetime
import pandas as pd


# =========================
# FILE METADATA
# =========================
def get_file_metadata(file_path: Path):

    return {
        "name": file_path.name,
        "size_bytes": file_path.stat().st_size,
        "created": datetime.fromtimestamp(file_path.stat().st_ctime).isoformat(),
        "modified": datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
    }


# =========================
# DATASET INFO
# =========================
def get_dataset_info(file_path: Path):

    if file_path.suffix.lower() == ".csv":

        df = pd.read_csv(
            file_path,
            sep=";",
            decimal=",",
            encoding="utf-8"
        )

    else:

        df = pd.read_excel(file_path)

    return {
        "columns": list(df.columns),
        "column_count": len(df.columns),
        "row_count": len(df),
        "column_types": {col: str(df[col].dtype) for col in df.columns}
    }


# =========================
# DCAT GENERATOR
# =========================
def generate_dcat_json(file_path: Path):

    output_path = file_path.with_suffix(".json")

    # ---- Skip if already exists ----
    if output_path.exists():

        print(f"✓ DCAT already exists: {output_path.name}")
        return

    file_metadata = get_file_metadata(file_path)
    dataset_info = get_dataset_info(file_path)

    base_name = file_path.stem

    dcat_metadata = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/"
        },

        "@type": "dcat:Catalog",

        "dcterms:title": base_name,
        "dcterms:description": "Infocamere financial dataset for FVG companies.",
        "dcterms:issued": datetime.now().isoformat(),
        "dcterms:modified": file_metadata["modified"],

        "dataset": [
            {
                "@type": "dcat:Dataset",

                "dcterms:title": base_name,
                "dcterms:description": f"Financial dataset extracted from {file_metadata['name']}",

                "dcterms:issued": file_metadata["created"],
                "dcterms:modified": file_metadata["modified"],

                "dcat:distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "dcterms:title": file_metadata["name"],
                        "dcat:mediaType": (
                            "text/csv"
                            if file_path.suffix == ".csv"
                            else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        ),
                        "dcat:accessURL": file_metadata["name"]
                    }
                ],

                "metadata": dataset_info
            }
        ],

        "file_info": file_metadata
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(dcat_metadata, f, indent=2, ensure_ascii=False)

    print(f"✓ DCAT created: {output_path.name}")


# =========================
# AUTO DETECT INPUT FILE
# =========================
def detect_latest_file():

    BASE_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = BASE_DIR.parent
    data_path = PROJECT_ROOT / "data" / "financial"

    if not data_path.exists():
        raise FileNotFoundError(f"Directory not found: {data_path}")

    files = list(data_path.glob("infocamere_*.*"))

    valid_files = []

    current_year = datetime.today().year

    for f in files:

        match = re.match(r'infocamere_(\d{4})\.(csv|xlsx)', f.name)

        if match:

            year = int(match.group(1))

            if year <= current_year:
                valid_files.append((year, f))

    if not valid_files:
        raise ValueError("No valid infocamere files found")

    valid_files.sort(reverse=True)

    selected_file = valid_files[0][1]

    print(f"Auto-detected input: {selected_file.name}")

    return selected_file


# =========================
# MAIN
# =========================
def main():

    try:

        if len(sys.argv) > 1:
            file_path = Path(sys.argv[1])
        else:
            file_path = detect_latest_file()

        generate_dcat_json(file_path)

    except Exception as e:

        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()