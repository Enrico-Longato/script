"""
DCAT Metadata Generator for EU Projects Downloads

Automatically creates DCAT-compliant JSON metadata describing downloaded 
EU Projects datasets (Horizon 2020 and Horizon Europe) from CORDIS.

DCAT (Data Catalog Vocabulary) is a W3C standard for describing data catalogs.

Usage:
    python 02_eu_input_DCAT.py
    
This script:
1. Scans downloaded EU projects datasets from the data folder
2. Extracts metadata for each file (size, modification date, hash)
3. Generates DCAT JSON descriptions with provenance information
4. Saves a DCAT JSON file for each downloaded file
"""

import json
import sys
import os
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, Any


# Dataset information mapping
DATASETS_INFO = {
    "h2020": {
        "name": "Horizon 2020 Projects",
        "description": "Horizon 2020 research and innovation programs projects data from CORDIS",
        "url": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
        "publisher": "European Commission - CORDIS",
        "version": "2020"
    },
    "horizon_europe": {
        "name": "Horizon Europe Projects",
        "description": "Horizon Europe research and innovation programs projects data from CORDIS",
        "url": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip",
        "publisher": "European Commission - CORDIS",
        "version": "2021-2027"
    }
}


def calculate_file_hash(file_path: Path, algorithm: str = "sha256") -> str:
    """
    Calculate the hash of a file.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm ('md5' or 'sha256')
    
    Returns:
        Hexadecimal hash string
    """
    hash_obj = hashlib.new(algorithm)
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def get_file_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract metadata from a file.
    
    Args:
        file_path: Path to the file
    
    Returns:
        Dictionary containing file metadata
    """
    stat_info = file_path.stat()
    
    metadata = {
        "name": file_path.name,
        "size_bytes": stat_info.st_size,
        "created": datetime.fromtimestamp(stat_info.st_ctime).isoformat(),
        "modified": datetime.fromtimestamp(stat_info.st_mtime).isoformat(),
        "sha256": calculate_file_hash(file_path, "sha256"),
        "md5": calculate_file_hash(file_path, "md5")
    }
    
    return metadata


def generate_dcat_json(file_path: Path, dataset_type: str, output_path: Path = None) -> dict:
    """
    Generate DCAT-compliant JSON metadata for a downloaded EU project file.
    
    Args:
        file_path: Path to the data file
        dataset_type: Type of dataset ("h2020" or "horizon_europe")
        output_path: Optional path to save the JSON (default: same name with .dcat.json)
    
    Returns:
        Dictionary containing DCAT metadata
    """
    
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_metadata = get_file_metadata(file_path)
    dataset_info = DATASETS_INFO.get(dataset_type, {})
    base_name = file_path.stem
    
    # Create DCAT Catalog structure
    dcat_metadata = {
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dctermss": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "prov": "http://www.w3.org/ns/prov#"
        },
        "@type": "dcat:Catalog",
        "dctermss:title": f"{dataset_info.get('name', dataset_type)} - Input Dataset",
        "dctermss:description": f"Input data file from {dataset_info.get('description', '')}",
        "dctermss:issued": datetime.now().isoformat(),
        "dctermss:modified": file_metadata['modified'],
        "foaf:homepage": "https://github.com/Enrico-Longato/script",
        "datasets": [
            {
                "@type": "dcat:Dataset",
                "dctermss:identifier": f"{dataset_type}_{base_name}",
                "dctermss:title": f"{dataset_info.get('name', dataset_type)} - {file_metadata['name']}",
                "dctermss:description": f"Input file: {file_metadata['name']} from {dataset_info.get('description', '')}",
                "dctermss:issued": file_metadata['created'],
                "dctermss:modified": file_metadata['modified'],
                "dcat:distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "dctermss:title": f"{base_name} - CSV Distribution",
                        "dcat:mediaType": "text/csv" if file_metadata['name'].endswith('.csv') else "application/octet-stream",
                        "dcat:accessURL": file_metadata['name'],
                        "dcat:byteSize": file_metadata['size_bytes']
                    }
                ],
                "dcat:keyword": [dataset_type, "EU", "CORDIS", "research", "innovation", "Horizon"],
                "dcat:theme": ["http://publications.europa.eu/resource/authority/data-theme/SOCI"],
                "dctermss:publisher": {
                    "@type": "foaf:Organization",
                    "foaf:name": dataset_info.get('publisher', 'Unknown')
                },
                "metadata": {
                    "dataset_type": dataset_type,
                    "dataset_version": dataset_info.get('version', 'Unknown'),
                    "source_url": dataset_info.get('url', '')
                }
            }
        ],
        "file_info": {
            "filename": file_metadata['name'],
            "size_bytes": file_metadata['size_bytes'],
            "created": file_metadata['created'],
            "modified": file_metadata['modified'],
        },
        "provenance": {
            "source_url": dataset_info.get('url', ''),
            "dataset_type": dataset_type,
            "dataset_version": dataset_info.get('version', 'Unknown'),
            "file_hash": {
                "sha256": file_metadata['sha256'],
                "md5": file_metadata['md5']
            },
            "source_publisher": dataset_info.get('publisher', 'Unknown')
        }
    }
    
    # Save to JSON file if output path is specified
    if output_path is None:
        output_path = file_path.with_suffix('.dcat.json')
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(dcat_metadata, f, indent=2, ensure_ascii=False)
    
    return dcat_metadata


def main():
    """Main function to scan downloaded files and generate DCAT descriptions."""
    
    # Set up paths and find data directory
    base_path = Path(__file__).resolve().parent.parent
    dest_folder = base_path / "data" / "eu_projects"

    print(f"PROJECT ROOT: {base_path}")
    print(f"DATA FOLDER: {dest_folder}")

    if not dest_folder.exists():
        print(f"✗ Error: Destination folder not found at {dest_folder}")
        sys.exit(1)
    
    # List files in the directory to verify structure
    try:
        listafile = os.listdir(dest_folder)
    except Exception as e:
        print(f"✗ Error listing directory: {e}")
        sys.exit(1)
    
    print("=" * 80)
    print("DCAT Generation Script for EU Projects Downloads")
    print("=" * 80)
    print(f"\nScanning directory: {dest_folder}\n")
    print(f"Found {len(listafile)} items in directory")
    
    dcat_files_created = 0
    dcat_files_info = []
    
    # Iterate through dataset folders
    for dataset_type in DATASETS_INFO.keys():
        dataset_folder = dest_folder / dataset_type
        
        if not dataset_folder.exists():
            print(f"⚠️  Folder not found: {dataset_folder}")
            continue
        
        print(f"\nProcessing [{dataset_type}] folder...")
        print("-" * 80)
        
        # Get all CSV files in the dataset folder
        for file_path in sorted(dataset_folder.glob("*.csv")):
            try:
                # Generate DCAT description
                dcat_description = generate_dcat_json(file_path, dataset_type)
                
                # Get output path
                dcat_output_path = file_path.with_suffix('.dcat.json')
                
                dcat_files_created += 1
                dcat_files_info.append({
                    "file": file_path.name,
                    "dataset_type": dataset_type,
                    "size_bytes": dcat_description['file_info']['size_bytes'],
                    "dcat_file": dcat_output_path.name
                })
                
                print(f"✓ {file_path.name}")
                print(f"  → {dcat_output_path.name}")
                
            except Exception as e:
                print(f"✗ Error processing {file_path.name}: {str(e)}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("DCAT Generation Summary")
    print("=" * 80)
    print(f"Total DCAT files created: {dcat_files_created}")
    print(dcat_output_path)
    
    if dcat_files_info:
        print("\nFiles processed:")
        for info in dcat_files_info:
            print(f"  • {info['file']} ({info['dataset_type']})")
            print(f"    Size: {info['size_bytes']:,} bytes")
            print(f"    DCAT: {info['dcat_file']}")
    
    print("=" * 80)


if __name__ == "__main__":
    main()
