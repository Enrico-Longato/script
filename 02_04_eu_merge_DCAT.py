"""
DCAT Metadata Generator for merged EU Projects CSV files

This script scans the `data/eu_projects/merge` folder for CSV files produced
by the merging pipeline (`02_eu_projects_merging.py`) and generates
DCAT-like JSON metadata for each file. It also outputs an aggregated
catalog JSON describing all merged datasets.

Usage:
    python 02_eu_merge_DCAT.py [merge_dir] [--catalog catalog.json]

Default `merge_dir` is the `data/eu_projects/merge` folder relative to the
repository root.
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
    try:
        df_head = pd.read_csv(path, sep='|', nrows=0, encoding='utf-8-sig')
    except Exception:
        df_head = pd.read_csv(path, sep='|', nrows=0, encoding='latin1')

    columns = list(df_head.columns)

    try:
        df_sample = pd.read_csv(path, sep='|', nrows=500, encoding='utf-8-sig')
    except Exception:
        df_sample = pd.read_csv(path, sep='|', nrows=500, encoding='latin1')

    dtypes = {col: str(df_sample[col].dtype) for col in df_sample.columns}

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
        "dcterms:description": f"Merged dataset produced by 02_eu_projects_merging.py: {path.name}",
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
    BASE_DIR = Path(__file__).resolve().parent
    PROJECT_ROOT = BASE_DIR.parent
    default_merge = PROJECT_ROOT / "data" / "eu_projects" / "merge"

    merge_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else default_merge

    catalog_out = None
    if '--catalog' in sys.argv:
        idx = sys.argv.index('--catalog')
        if idx + 1 < len(sys.argv):
            catalog_out = Path(sys.argv[idx + 1])

    if not merge_dir.exists():
        print(f"Error: merge directory not found: {merge_dir}")
        sys.exit(1)

    provenance_common = {
        "generated_by": "anagrafica/script/02_eu_projects_merging.py",
        "generated_on": datetime.now().isoformat(),
        "notes": "Datasets produced by concatenating H2020 and Horizon Europe files."
    }

    csv_files = list(merge_dir.glob('*.csv'))
    if not csv_files:
        print(f"No CSV files found in {merge_dir}")
        sys.exit(0)

    # optionally build an aggregated catalog if requested
    catalog = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dcterms": "http://purl.org/dc/terms/",
        },
        "@type": "dcat:Catalog",
        "dcterms:issued": datetime.now().isoformat(),
        "datasets": []
    } if catalog_out is not None else None

    for csv in csv_files:
        prov = provenance_common.copy()
        prov["source_file_detected"] = csv.name
        metadata = generate_dcat_for_csv(csv, provenance=prov)

        outpath = csv.with_suffix('.dcat.json')
        with outpath.open('w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"Wrote: {outpath}")

        if catalog is not None:
            catalog['datasets'].append({
                "title": metadata.get('dcterms:title'),
                "file": metadata['file_info']['filename'],
                "dcat_json": outpath.name,
                "row_count": metadata['structure'].get('row_count'),
                "column_count": metadata['structure'].get('column_count')
            })

    # write aggregated catalog only if user explicitly requested it
    if catalog_out is not None and catalog is not None:
        with Path(catalog_out).open('w', encoding='utf-8') as f:
            json.dump(catalog, f, indent=2, ensure_ascii=False)
        print(f"Aggregated catalog written to: {catalog_out}")
    else:
        print("No aggregated catalog created (use --catalog to generate one)")


if __name__ == '__main__':
    main()
