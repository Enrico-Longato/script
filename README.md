# Company Data Processing Scripts

Data cleaning and preparation scripts for regional company.

## Files (Python scripts only)

- `00_main.py` - Launcher that executes all `.py` scripts in this folder in alphabetical order.
- `01_01_excel_to_dcat.py` - Produce DCAT metadata for raw company Excel files.
- `01_02_anagrafica.py` - Data cleaning and preparation for company registry (produces CSV outputs).
- `01_03_outputs_to_dcat.py` - Produce DCAT metadata for processed CSV outputs.
- `02_01_eu_projects_download.py` - Download and extract EU project data from CORDIS.
- `02_02_eu_input_DCAT.py` - Produce DCAT metadata for input EU project files.
- `02_03_eu_projects_merging.py` - Merge and clean EU project datasets into `data/eu_projects/merge`.
- `02_04_eu_merge_DCAT.py` - Produce per-file DCAT metadata for files in `data/eu_projects/merge` (aggregated catalog optional).


## `cols_dict.xlsx`

Location: `script/cols_dict.xlsx`

This Excel workbook contains sheets used by the cleaning scripts to map and standardize column names coming from source files. Common sheets:

- `anagrafica`: maps original column names from the `FRIULI Anagrafica` sheet to the standardized names used in `01_02_anagrafica.py`.
- `codici`: maps original column names from the `FRIULI codice attività` sheet to standardized names.

Scripts read the appropriate sheet and build a dictionary pairing `nomi_colonne_originali` -> `nomi_colonne_corretti`, then rename DataFrame columns accordingly. Keep `cols_dict.xlsx` next to the scripts to ensure automatic discovery.

## Launcher (`main.py`)

Run the launcher from inside the `script` directory (or from the project root) to execute all `.py` scripts in filename order:

```powershell
python main.py
```

To exclude specific scripts, pass a comma-separated list of basenames (without `.py`):

```powershell
python main.py --exclude 01_02_anagrafica,02_01_eu_projects_download
```

## DCAT merge script behavior

`02_04_eu_merge_DCAT.py` writes per-file `.dcat.json` metadata next to each CSV by default. An aggregated catalog JSON is created only when you pass the `--catalog <output.json>` option.

## License

CC-BY-4.0




