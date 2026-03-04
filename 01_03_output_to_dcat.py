"""
DCAT Metadata Generator for output CSV files

Scans the data output directory for CSV files produced by the pipeline
and generates DCAT-like JSON metadata describing structure and provenance
for each file. Also produces an aggregated catalog JSON.

Default `data_dir` is the `data/anagrafica` folder relative to current working dir.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
import pandas as pd


def get_file_metadata(path: Path) -> dict:
    stat = path.stat()
    return {
        "filename": path.name,
        "path": str(path),
        "size_bytes": stat.st_size,
        "created": datetime.fromtimestamp(stat.st_ctime).isoformat(),
        "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
    }


def get_csv_structure(path: Path) -> dict:
    # read only header to get columns and dtypes quickly
    try:
        df_head = pd.read_csv(path, sep='|', nrows=0, encoding='utf-8-sig')
    except Exception:
        df_head = pd.read_csv(path, sep='|', nrows=0, encoding='latin1')

    columns = list(df_head.columns)

    # attempt to infer dtypes on a small sample
    try:
        df_sample = pd.read_csv(path, sep='|', nrows=500, encoding='utf-8-sig')
    except Exception:
        df_sample = pd.read_csv(path, sep='|', nrows=500, encoding='latin1')

    dtypes = {col: str(df_sample[col].dtype) for col in df_sample.columns}

    # fast row count (safe fallback if very large)
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            row_count = sum(1 for _ in f) - 1
            if row_count < 0:
                row_count = 0
    except Exception:
        try:
            row_count = int(pd.read_csv(path, sep='|', usecols=[0]).shape[0])
        except Exception:
            row_count = None

    return {
        "columns": columns,
        "column_count": len(columns),
        "column_types_sample": dtypes,
        "row_count": row_count,
    }


def detect_source_excels(data_dir: Path) -> list:
    # look for any input excel matching the naming pattern used by the pipeline
    excels = list(data_dir.glob('imprese_fvg_*.xlsx'))
    return [str(p.name) for p in excels]


def generate_dcat_for_csv(path: Path, provenance: dict = None) -> dict:
    file_meta = get_file_metadata(path)
    structure = get_csv_structure(path)

    base = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/",
            "prov": "http://www.w3.org/ns/prov#"
        },
        "@type": "dcat:Dataset",
        "dcterms:title": path.stem,
        "dcterms:description": f"Dataset exported by the anagrafica pipeline: {path.name}",
        "dcterms:issued": file_meta['created'],
        "dcterms:modified": file_meta['modified'],
        "dcat:distribution": {
            "dcat:accessURL": path.name,
            "dcat:mediaType": "text/csv",
            "bytes": file_meta['size_bytes']
        },
        "structure": structure,
        "provenance": provenance or {},
        "file_info": file_meta,
    }

    return base


def main():
    # default data directory
    BASE_DIR = Path(__file__).resolve().parent      # FAIR/script
    PROJECT_ROOT = BASE_DIR.parent                  # FAIR
    data_path = PROJECT_ROOT / "data" / "anagrafica"


    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else data_path

    catalog_out = None
    if '--catalog' in sys.argv:
        idx = sys.argv.index('--catalog')
        if idx + 1 < len(sys.argv):
            catalog_out = Path(sys.argv[idx + 1])

    if not data_dir.exists():
        print(f"Error: data directory not found: {data_dir}")
        sys.exit(1)

    # detect source excel(s) for provenance
    source_excels = detect_source_excels(data_dir)

    provenance_common = {
        "generated_by": "anagrafica/script/01_02_anagrafica.py",
        "generated_on": datetime.now().isoformat(),
        "source_excels": source_excels,
        "notes": "Derived outputs from the anagrafica pipeline (filtering, cleaning and export)."
    }

    csv_files = list(data_dir.glob('*.csv'))

    if not csv_files:
        print(f"No CSV files found in {data_dir}")
        sys.exit(0)

    catalog = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/",
        },
        "@type": "dcat:Catalog",
        "dcterms:issued": datetime.now().isoformat(),
        "datasets": []
    }

    for csv in csv_files:
        prov = provenance_common.copy()
        prov["source_file_detected"] = csv.name
        metadata = generate_dcat_for_csv(csv, provenance=prov)

        # write per-file DCAT JSON besides the CSV (same stem + .dcat.json)
        outpath = csv.with_suffix('.dcat.json')
        with outpath.open('w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"Wrote: {outpath}")

        catalog['datasets'].append({
            "title": metadata.get('dcterms:title'),
            "file": metadata['file_info']['filename'],
            "dcat_json": outpath.name,
            "row_count": metadata['structure'].get('row_count'),
            "column_count": metadata['structure'].get('column_count')
        })

    # write aggregated catalog if requested or to data_dir/DCAT_anagrafica.json
    if catalog_out is None:
        catalog_out = data_dir / 'DCAT_anagrafica.json'

    with Path(catalog_out).open('w', encoding='utf-8') as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    print(f"Aggregated catalog written to: {catalog_out}")


if __name__ == '__main__':
    main()
