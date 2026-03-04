#!/usr/bin/env python3
"""
Infocamere Financial Data Validation Script

Author: Enrico Longato @ Area Science Park

Description:
Python script to validate and process Infocamere financial statements.

The script:
- Auto-detects latest available infocamere_YYYY.csv
- Checks expected column number (21)
- Converts values according to predefined type mapping
- Exports valid records and parsing errors into separate CSV files

Notes:
- Leading zeros in fiscal codes are preserved
- Only parsing (type conversion) errors generate rejected rows
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import re


# =========================
# FUNCTION: LOAD FILE
# =========================
def load_file(file_path):

    extension = file_path.suffix.lower()

    if extension in [".xlsx", ".xls"]:
        print("📘 Excel detected")
        return pd.read_excel(
            file_path,
            dtype={"c fiscale": str, "cia": str, "rea": str}
        )

    elif extension == ".csv":
        print("📄 CSV detected")
        return pd.read_csv(
            file_path,
            sep=";",
            decimal=",",
            encoding="utf-8",
            dtype={"c fiscale": str, "cia": str, "rea": str}
        )

    else:
        raise Exception(f"Unsupported file format: {extension}")


# =========================
# PATH SETUP
# =========================
base_path = Path.cwd()
data_path = base_path / "data" / "financial"

files = [f.name for f in data_path.iterdir() if "infocamere_" in f.name]

if not files:
    raise ValueError(f"No files found in {data_path}")


# =========================
# AUTO DETECT YEAR
# =========================
current_year = datetime.today().year
available_years = []

for fname in files:
    match = re.match(r'infocamere_(\d{4})\.csv', fname)
    if match:
        year = int(match.group(1))
        if year <= current_year:
            available_years.append(year)

if not available_years:
    raise ValueError("No valid infocamere_YYYY.csv files found")

available_years.sort(reverse=True)
selected_year = available_years[0]

print(f"Auto-detected year: {selected_year}")
print(f"Current year: {current_year}")

file_path = data_path / f"infocamere_{selected_year}.csv"
print(f"Upload data from: {file_path}")


# =========================
# LOAD FILE
# =========================
df = load_file(file_path)
print(f"Dataset loaded: {df.shape[0]} rows, {df.shape[1]} columns")

EXPECTED_COLUMNS = 21
print(f"Column check OK: {EXPECTED_COLUMNS} columns detected")

if len(df.columns) != EXPECTED_COLUMNS:
    raise Exception(
        f"Numero colonne errato. Attese {EXPECTED_COLUMNS}, trovate {len(df.columns)}"
    )


# =========================
# SCHEMA
# =========================
SCHEMA = [
    {"name": "c fiscale", "type": "string"},
    {"name": "cia", "type": "string"},
    {"name": "rea", "type": "int"},
    {"name": "anno", "type": "int"},
    {"name": "Totale attivo", "type": "float"},
    {"name": "Totale Immobilizzazioni immateriali", "type": "float"},
    {"name": "Crediti esigibili entro l'esercizio successivo", "type": "float"},
    {"name": "Totale patrimonio netto", "type": "float"},
    {"name": "Debiti esigibili entro l'esercizio successivo", "type": "float"},
    {"name": "Totale valore della produzione", "type": "float"},
    {"name": "Ricavi delle vendite", "type": "float"},
    {"name": "Totale Costi del Personale", "type": "float"},
    {"name": "Differenza tra valore e costi della produzione", "type": "float"},
    {"name": "Ammortamento Immobilizzazione Immateriali", "type": "float"},
    {"name": "Utile/perdita esercizio ultimi", "type": "float"},
    {"name": "valore aggiunto", "type": "float"},
    {"name": "tot.aam.acc.svalutazioni", "type": "float"},
    {"name": "(ron) reddito operativo netto", "type": "float"},
    {"name": "Immobilizzazioni materiali", "type": "float"},
    {"name": "Immobilizzazioni finanziarie", "type": "float"},
    {"name": "Attivo Circolante", "type": "float"},
]

df.columns = [col["name"] for col in SCHEMA]


# =========================
# VALIDATION
# =========================
valid_rows = []
error_rows = []
print("Starting row validation...")
for index, row in df.iterrows():

    try:
        validated_row = {}

        for col in SCHEMA:

            value = row[col["name"]]

            if col["type"] == "string":
                value = str(value)

            elif col["type"] == "int":
                value = int(value)

            elif col["type"] == "float":
                value = float(value)

            validated_row[col["name"]] = value

        valid_rows.append(validated_row)

    except Exception as e:

        error_rows.append({
            "riga": index + 2,
            "errore": str(e)
        })


print("Valid rows:", len(valid_rows))
print("Rows with errors:", len(error_rows))
print(f"Validation completed: {len(valid_rows) + len(error_rows)} rows processed")


# =========================
# EXPORT
# =========================
df_valid = pd.DataFrame(valid_rows)
df_errors = pd.DataFrame(error_rows)

valid_output = data_path / f"i2fvg_bilanci_{selected_year}.csv"
error_output = data_path / f"i2fvg_bilanci_errori_{selected_year}.csv"

if not df_valid.empty:
    df_valid.to_csv(valid_output, index=False, sep="|", decimal=",")
    print(f"✅ File with data saved: {valid_output}")

if not df_errors.empty:
    df_errors.to_csv(error_output, index=False, sep="|", decimal=",")
    print(f"⚠️ File errors saved: {error_output}")
else:
    print("✅ no errors found")