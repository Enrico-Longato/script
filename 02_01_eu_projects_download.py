"""
EU Projects Download and Extract Script

Automatically downloads EU projects datasets (Horizon 2020 and Horizon Europe) 
from CORDIS and extracts specific files to organized directories.

Authors: Enrico Longato @ Area Science Park

Description: 
This script downloads research project data from CORDIS for both Horizon 2020 
and Horizon Europe programs, extracts relevant files (project data, organization data, 
and EuroSciVoc classifications), and organizes them into separate folders.

Usage:
    python 02_eu_projects.py

The script will:
1. Create the data/eu_projects directory if it doesn't exist
2. Download ZIP files from CORDIS for both programs
3. Extract specific CSV files to organized folders
4. Clean up temporary ZIP files
"""

# Import necessary libraries
# requests: for downloading files from URLs
# zipfile: for extracting ZIP archives
# Path: for file path operations
# datetime: for timestamp operations
# os: for operating system utilities

import requests
import zipfile
import os
from pathlib import Path
from datetime import datetime


def setup_directories() -> Path:
    """
    Set up paths and find data directory using same methodology as 01_anagrafica.py
    
    Returns:
        Path: The destination folder path
    """
    # Set up paths and find data directory
    base_path = Path(__file__).resolve().parent.parent
    DEST_FOLDER = base_path / "data" / "eu_projects"
    
    # Verify and list files
    if DEST_FOLDER.exists():
        print(f"✓ Data folder found: {DEST_FOLDER}")
        listafile = os.listdir(DEST_FOLDER)
        if listafile:
            print(f"  Current contents: {len(listafile)} items")
        else:
            print(f"  Folder is empty")
    else:
        print(f"✗ Error: Data folder not found at {DEST_FOLDER}")
    
    return DEST_FOLDER


def download_and_extract(name: str, url: str, dest_folder: Path) -> None:
    """
    Download a ZIP file from a URL and extract specific files from it.
    
    Args:
        name: Name of the dataset (h2020 or horizon_europe)
        url: URL to download the ZIP file from
        dest_folder: Destination folder path
    """
    # Create a folder for the program dataset
    program_folder = dest_folder / name
    program_folder.mkdir(exist_ok=True)

    # Define the path where the ZIP file will be temporarily stored
    zip_path = program_folder / f"{name}.zip"

    print(f"\n{'='*80}")
    print(f"Downloading {name} dataset...")
    print(f"{'='*80}")
    
    # Download the file from the URL with streaming enabled for large files
    try:
        response = requests.get(url, stream=True, timeout=30)
    except requests.exceptions.RequestException as e:
        print(f"✗ Error downloading {url}: {e}")
        return

    # Check if the download was successful
    if response.status_code != 200:
        # Exit the function if download failed
        print(f"✗ Download failed with status code {response.status_code}")
        return

    print(f"✓ Download started...")
    
    # Write the downloaded content to the ZIP file in chunks
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)
    
    print(f"✓ File downloaded: {zip_path.name}")

    # Extract specific files from the ZIP archive
    # We're interested in files containing "project", "organization", or "euroscivoc" in their names
    print(f"✓ Extracting files...")
    
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            extracted_count = 0
            for file in z.namelist():

                # Filter files based on specific keywords
                if any(x in file.lower() for x in ["project", "organization", "euroscivoc"]):
                    # Extract the file to the program folder
                    z.extract(file, program_folder)
                    # Print confirmation message with file name
                    extracted_count += 1
                    print(f"✅ {file.split('/')[-1]}")
            
            if extracted_count == 0:
                print("⚠️ No matching files found in archive")
    
    except zipfile.BadZipFile:
        print(f"✗ Error: {zip_path.name} is not a valid ZIP file")
        return
    
    except Exception as e:
        print(f"✗ Error extracting files: {e}")
        return

    # Delete the ZIP file after extraction
    zip_path.unlink()
    print(f"✓ Temporary ZIP file removed")


def main():
    """Main function to orchestrate the download and extraction process."""
    
    print("=" * 80)
    print("EU Projects Download Script")
    print("Horizon 2020 & Horizon Europe from CORDIS")
    print("=" * 80)
    
    # Setup directories
    dest_folder = setup_directories()
    
    # Define datasets dictionary with program names and their download URLs from CORDIS
    # h2020: Horizon 2020 project data
    # horizon_europe: Horizon Europe project data
    DATASETS = {
        "h2020": "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip",
        "horizon_europe": "https://cordis.europa.eu/data/cordis-HORIZONprojects-csv.zip"
    }
    
    # Download and extract each dataset
    for name, url in DATASETS.items():
        download_and_extract(name, url, dest_folder)
    
    print(f"\n{'='*80}")
    print("Download and extraction completed!")
    print(f"Data saved to: {dest_folder}")
    print("=" * 80)


if __name__ == "__main__":
    main()
