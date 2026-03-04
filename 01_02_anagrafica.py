#!/usr/bin/env python3
"""
Notebook 01: basic data preparation (XLSX >>> cleaning >>> CSV)

Authors: Fabio Morea, Leyla Vesnic, Enrico Longato @ Area Science Park
 
Description: python scripts to clean and prepare data on regional companies.
This script imports data from .xlsx and produces clean data in the form of .csv files.


"""

# Setup
import sys
import os
from pathlib import Path
import datetime
import pandas as pd
import numpy as np
import re
import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
pd.options.display.max_columns = None


# Set up paths and find input file
current_path = Path(os.getcwd())
data_subdir = "data/anagrafica"
data_path = current_path / data_subdir
data_dir = str(data_path)
listafile = os.listdir(data_path)
listafile = list(filter(lambda x: 'imprese_fvg_' in x, listafile))  
assert len(listafile) >= 1, f"Error: can't find any file in {data_dir}"

# Auto-detect file_da_elaborare based on current month/year
import re
from datetime import datetime

# Get current date
today = datetime.today()
current_mm = f"{today.month:02d}"
current_yyyy = str(today.year)
current_date = (today.year, today.month)

# Find all available files matching the pattern
available_files = []
for fname in listafile:
    # Pattern: imprese_fvg_MM_YYYY.xlsx
    match = re.match(r'imprese_fvg_(\d{2})_(\d{4})\.xlsx', fname)
    if match:
        mm, yyyy = match.groups()
        file_date = (int(yyyy), int(mm))
        # Only include files not from the future
        if file_date <= current_date:
            available_files.append((file_date, f"{mm}_{yyyy}"))

if not available_files:
    # Fallback to latest available (even if future)
    for fname in listafile:
        match = re.match(r'imprese_fvg_(\d{2})_(\d{4})\.xlsx', fname)
        if match:
            mm, yyyy = match.groups()
            file_date = (int(yyyy), int(mm))
            available_files.append((file_date, f"{mm}_{yyyy}"))

# Sort by date descending and pick the most recent
available_files.sort(reverse=True)

if available_files:
    file_da_elaborare = available_files[0][1]
    print(f"Auto-detected: file_da_elaborare = '{file_da_elaborare}'")
    print(f"Current date: {current_mm}_{current_yyyy}")
    if available_files[0][0] == current_date:
        print(f"✓ File for current month available")
else:
    raise ValueError("No valid input files found matching pattern imprese_fvg_MM_YYYY.xlsx")

excel_file = data_dir + '\\imprese_fvg_' + file_da_elaborare + '.xlsx'
print(f'upload data in {excel_file}')

# Load Excel file and check sheet names
xl = pd.ExcelFile(  excel_file, engine="openpyxl")
xl_sheets = xl.sheet_names  # see all sheet names
assert xl_sheets == ['FRIULI Anagrafica', 'FRIULI codice attività']

# ============================================================================
# First sheet: FRIULI Anagrafica
# ============================================================================
print("Processing sheet: FRIULI Anagrafica")
df_anagrafica = xl.parse(   'FRIULI Anagrafica', 
                            header = 0, 
                            dtype=str,
                            keep_default_na=False)

# create a dictionary with the original and corrected column names
print("Renaming columns in the anagrafica sheet")
cols_path = current_path / "script" / "cols_dict.xlsx"
cols_df = pd.read_excel(cols_path, sheet_name='anagrafica') 
l1 = cols_df['nomi_colonne_originali']
l2 = cols_df['nomi_colonne_corretti']
cols_dic = dict(zip(l1,l2))
df_anagrafica.rename(columns=cols_dic, inplace=True)

#add colunms: source, mm_aaaa, type of location
print("Adding source, reference month/year and location type columns")
df_anagrafica['fonte'] = 'I'
df_anagrafica['mm_aaaa'] = file_da_elaborare
df_anagrafica['n_sede'] = df_anagrafica['sede_ul'].str[3:] 

#convert 'E' in 'n_sede' to '0'
df_anagrafica.loc[ df_anagrafica['n_sede'] == 'E', 'n_sede'] = '0'
df_anagrafica['n_sede'].tolist()

# info about the dataset
dim = df_anagrafica.shape 
print(f'dimension of the dataset (row,columns) = {dim}\n')

#strip special characters
print("Stripping special characters from text fields")
#chars_to_strip = '\\n\\t\\r|#'
pattern = r'[\n\t\r"\'|#\-*]|_x000D_'

cols_to_strip = ['denominazione', 'descrizione_attivita', 'indirizzo']

for col in cols_to_strip:
    df_anagrafica[col] = (
        df_anagrafica[col]
        .astype(str)
        .str.replace(pattern, ' ', regex=True)
        .str.strip()
    )
    print(col, "ok")

#keep only the active companies
print("Filtering to keep only active companies (data_cess_att is empty)")
df_anagrafica = df_anagrafica.loc[df_anagrafica['data_cess_att'].isin([''])].copy()

# The dates in the original file have some issues, need to correct them with a function that: 
#   (1) subtract 3000 from the year, 
#   (2) properly handle errors and "nan", 
#   (3) remove dates earlier than 1800 or later than 2099

def anno_corretto(dstring: str) -> str:
    num = 0
    dstring = str(dstring)[:10]
    if len(dstring) < 4:
        result = "NaT"
    else:
        try:
            num = int(dstring[0:2])
            if num >= 48 and num <= 51: #years between 1800 and 2100
                num = num - 30
                result = str(num) + dstring[2:]
            else:
                result = "NaT"
        except:
            result = "NaT"
    return result   

#test to verify the function anno_corretto
assert anno_corretto('x')   == 'NaT'
assert anno_corretto('1799-03-01') == 'NaT'
assert anno_corretto('2100-03-01') == 'NaT'
assert anno_corretto('4987-03-01') == '1987-03-01'
assert anno_corretto('5021-03-01') == '2021-03-01'
pass

# Normalize all date columns by applying anno_corretto() and converting them to datetime64.

cols_date = [d for d in df_anagrafica.columns if d.startswith('data')]

for col in cols_date:
    datestring3000 = df_anagrafica[col].tolist()
    datestring = [anno_corretto(item) for item in datestring3000]
    df_anagrafica[col] = pd.to_datetime(datestring)

# ============================================================================
# id localiz e id impresa
# ============================================================================
print("Creating id_localiz and id_impresa fields")
# Create the key_cfl field to link id_localiz with the codes dataset,
# combining the tax code (cf) and the branch number (n_sede)

df_anagrafica['key_cfl'] = df_anagrafica['cf'] + '_'  + df_anagrafica['n_sede'].apply(str)


# Create a temporary "years" column to compute and sort records by age:
# - calculate the earliest date across all date columns
# - compute the time difference from today
# - convert it into years
# - drop intermediate helper columns

df_anagrafica['min_date']   = pd.to_datetime( df_anagrafica[cols_date].min(axis=1) )
df_anagrafica['today']      = pd.to_datetime( pd.Timestamp.today().strftime('%Y-%m-%d') ) 
df_anagrafica['anni_timedelta'] =  (df_anagrafica['today']  - df_anagrafica['min_date'])
df_anagrafica['anni'] = df_anagrafica['anni_timedelta'].dt.total_seconds() / (60 * 60 * 24 * 365)
df_anagrafica.drop(columns='today')
df_anagrafica.drop(columns='anni_timedelta')

# Sort the dataframe by the computed age in years and the composite key (key_cfl), in descending order
df_anagrafica.sort_values(by = ['anni', 'key_cfl'], ascending=False, inplace = True)

# Create id_localiz from the dataframe index:
# - reset the index to expose it as a column
# - reset again to generate a sequential numeric id
# - assign id_localiz as index + 1
# - restore the original ordering

df_anagrafica.reset_index(inplace=True)
df_anagrafica.reset_index(inplace=True)
df_anagrafica['id_localiz'] = df_anagrafica['index'] + 1

df_anagrafica.sort_values(by = 'index', inplace = True)

# Create a unique company identifier linked to the tax code (cf):
print("Creating id_impresa field based on unique tax codes (cf)")
# - select tax code and company name
# - drop duplicate tax codes
# - reset the index to generate a sequential id
# - assign id_impresa starting from 1

df_cf_univoco = df_anagrafica[['cf', 'denominazione']].copy()
df_cf_univoco = df_cf_univoco.drop_duplicates(subset='cf')
df_cf_univoco.reset_index(inplace = True)
df_cf_univoco.reset_index(inplace = True)
df_cf_univoco['id_impresa'] = df_cf_univoco['level_0'] +1
df_cf_univoco.columns

# Drop intermediate index columns and reorder the dataframe to show id_impresa and cf
print("Cleaning up the id_impresa mapping table")   
df_cf_univoco = df_cf_univoco.drop(columns=['level_0', 'index'])
cols_order = ['id_impresa', 'cf' ]
df_cf_univoco = df_cf_univoco[cols_order]

df_anagrafica.shape[0] #conta righe

# Join the main anagraphic dataset with the deduplicated company table using the tax code (cf)
print("Merging id_impresa back into the main anagrafica dataset")   

df_anagrafica = df_anagrafica.merge(df_cf_univoco, on = 'cf', how = 'inner') 

# ============================================================================
# Checks on local units without reference headquarters – duplicate detection – date validation
# ============================================================================
print("Performing checks on local units without reference headquarters, duplicate records and date validation")
# Function to create a binary flag column based on whether the unit is a headquarters or a local unit

def f(row):
    if row['sede_ul'] == 'SEDE':
        val = 's'
    else:
        val = 'u'
    return val

# new column 
df_anagrafica['SEDE_UL_num'] = df_anagrafica.apply(f, axis = 1)

# Build a pivot table to count headquarters vs local units for each tax code (cf)

pivot_test = df_anagrafica.pivot_table(index = "cf",
                                values = "sede_ul",
                                columns='SEDE_UL_num',
                                aggfunc = lambda x : x.count(),
                                margins = True,
                                fill_value=0)
pivot_test

## Extract the list cf with no headquarters (s == 0)

df_test = pivot_test
cf_no_sede = df_test[ df_test['s']==0 ]
cf_no_sede = cf_no_sede.reset_index()
cf_no_sede = cf_no_sede['cf'].to_list()

# Remove records whose CF has no headquarters

df_anagrafica = df_anagrafica[~df_anagrafica['cf'].isin(cf_no_sede)].copy()

# ============================================================================
# Duplicate management
# ============================================================================

# Identify duplicate records based on CF and sede_ul
df_dup = df_anagrafica[df_anagrafica.duplicated(subset=['cf','sede_ul'], keep=False)]

# Keep only duplicates where the province differs from the Chamber of Commerce province,
# then inspect records for a specific CF
df_dup = df_dup[df_dup['prov'] != df_dup['prov_camera_commercio']]
df_dup[df_dup['cf'] == '07381630966']

# Drop duplicated records identified for removal using their row indices
id_df_dup = df_dup.index.values.tolist()
print(f"Duplicate rows removed: {len(id_df_dup)}")
df_anagrafica.drop(id_df_dup, axis=0, inplace=True)

# Recheck for remaining duplicates based on CF and sede_ul after cleanup
df_dup_test_final = df_anagrafica[df_anagrafica.duplicated(subset=['cf','sede_ul'], keep=False)]
df_dup_test_final

# Check whether any remaining duplicates refer to the same local unit in the same province
df_dup_test_final_2 = df_dup_test_final[df_dup_test_final.duplicated(subset=['cf','sede_ul','prov'], keep=False)]
df_dup_test_final_2

# ============================================================================
# Second sheet: FRIULI activity codes
# ============================================================================

# Load the "FRIULI codice attività" sheet into a dataframe, forcing all columns to string
print("Processing sheet: FRIULI codice attività")   
df_codici = xl.parse('FRIULI codice attività',  
                    header = 0,
                    dtype=str,
                    keep_default_na=False) 

# Create a dictionary mapping original column names to corrected ones,
# then rename the dataframe columns accordingly

cols_df = pd.read_excel(cols_path, sheet_name='codici') 
l1 = cols_df['nomi_colonne_originali']

l2 = cols_df['nomi_colonne_corretti']

cols_dic = dict(zip(l1,l2))



df_codici.rename(columns=cols_dic, inplace=True)

df_codici

# Add source identifier and reference month/year fields
df_codici['fonte'] = 'I'
df_codici['mm_aaaa'] = file_da_elaborare

# Create key_cfl in df_codici to link to id_localiz,
# then extract the mapping table from df_anagrafica

df_codici['key_cfl'] = df_codici['cf'] + '_'  + df_codici['loc_n']
df_temp = df_anagrafica[['key_cfl',  'id_impresa', 'id_localiz']]

# Add id_localiz to df_codici by joining on key_cfl
df_codici = df_codici.merge(df_temp, on = 'key_cfl', how = 'inner') 

# Filter the codes sheet as well by removing CF values with no headquarters
df_codici = df_codici[~df_codici['cf'].isin(cf_no_sede)]

# Identify duplicate records in df_codici based on CF and location/activity fields
df_cod_dup = df_codici[df_codici.duplicated(subset=['cf','prov', 'rea', 'loc_n', 'ateco_tipo','ateco','ateco_desc'], keep=False)]

df_codici.shape[0]

# ============================================================================
# Write output CSV files for multiple targets
# ============================================================================
print("Exporting cleaned data to CSV files for multiple targets (repository and innovation intelligence)")  
# File .csv per Repository
# Since the "tipo_sedeul_n" fields need to be merged, I create a copy of the dataframe so that the merge can be performed on the copy and saved in the "Repository" version.

# Create a copy of the dataframe for the repository version,
# merge the tipo_sedeul_* fields into a single column,
# and drop the original component columns
df_anagrafica_repo = df_anagrafica.copy()
df_anagrafica_repo['tipo_sedeul'] =  df_anagrafica_repo.tipo_sedeul_1+df_anagrafica_repo.tipo_sedeul_2+df_anagrafica_repo.tipo_sedeul_3+df_anagrafica_repo.tipo_sedeul_4+df_anagrafica_repo.tipo_sedeul_5
df_anagrafica_repo.drop(columns=['tipo_sedeul_1', 'tipo_sedeul_2', 'tipo_sedeul_3', 'tipo_sedeul_4',
       'tipo_sedeul_5'], inplace=True)

# Save the ANAGRAFICA file in the "repository" version:
# - load the export column mapping and order
# - sort columns according to the repository layout
# - export the selected fields to a pipe-delimited CSV file
file_risultati = data_dir + '\\' + 'imprese_anagrafica.csv'
cols_df = pd.read_excel(cols_path, sheet_name='anagrafica',
            usecols = ['nomi_colonne_export','ordine_repo']).dropna() 
cols_df.sort_values('ordine_repo', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_export'])
df_anagrafica_repo[cols_to_use].to_csv(  file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned anagrafica data to {file_risultati}")

# Save the CODICI file in the "repository" version:
# - load the export column mapping and order
# - sort columns according to the repository layout
# - export the selected fields to a pipe-delimited CSV file
file_risultati = data_dir + '\\' + 'imprese_codici.csv'
cols_df = pd.read_excel(cols_path, sheet_name='codici',
            usecols = ['nomi_colonne_corretti','ordine_repo']).dropna() 
cols_df.sort_values('ordine_repo', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_corretti'])
df_codici[cols_to_use].to_csv(      file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned codici data to {file_risultati}") 
# ============================================================================
# CSV file for Innovation Intelligence (local units including those outside FVG)
# ============================================================================
print("Exporting cleaned data to CSV files for multiple targets (repository and innovation intelligence)")  
# Save the ANAGRAFICA file in the "Innovation Intelligence" version:
# - load the export column mapping and order
# - sort columns according to the Innovation Intelligence layout
# - export the selected fields to a pipe-delimited CSV file
file_risultati = data_dir + '\\' + 'i2fvg_anagrafica.csv'
cols_df = pd.read_excel(cols_path, sheet_name='anagrafica',
            usecols = ['nomi_colonne_corretti','ordine_i2fvg']).dropna() 
cols_df.sort_values('ordine_i2fvg', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_corretti'])
df_anagrafica[cols_to_use].to_csv(  file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned anagrafica data to {file_risultati}") 
# Save the CODICI file in the "Innovation Intelligence" version:
# - load the export column mapping and order
# - sort columns according to the Innovation Intelligence layout
# - export the selected fields to a pipe-delimited CSV file
file_risultati = data_dir + '\\' + 'i2fvg_codici.csv'
cols_df = pd.read_excel(cols_path, sheet_name='codici',
            usecols = ['nomi_colonne_corretti','ordine_i2fvg']).dropna() 
cols_df.sort_values('ordine_i2fvg', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_corretti'])
df_codici[cols_to_use].to_csv(      file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned codici data to {file_risultati}")     
# ============================================================================
# File .csv per Innovation Intelligence (solamente unità locali FVG)
# ============================================================================
print("Exporting cleaned data to CSV files for multiple targets (repository and innovation intelligence)")  
# Filter out local units outside the FVG provinces from the ANAGRAFICA dataset:
# - keep headquarters
# - remove non-FVG local units
local_sede = ["SEDE"]
prov_FVG = ["GO", "PN", "UD", "TS"]
unità_locali_extraFVG_filter = ~df_anagrafica["sede_ul"].isin(local_sede) & ~df_anagrafica["prov"].isin(prov_FVG)
df_anagrafica = df_anagrafica[~unità_locali_extraFVG_filter]
print(f"After filtering, anagrafica dataset has {df_anagrafica.shape[0]} rows") 
# Save the ANAGRAFICA file in the "Innovation Intelligence" version
# after removing local units outside the FVG region
file_risultati = data_dir + '\\' + 'i2fvg_anagrafica_filtrato.csv'
cols_df = pd.read_excel(cols_path, sheet_name='anagrafica',
            usecols = ['nomi_colonne_corretti','ordine_i2fvg']).dropna() 
cols_df.sort_values('ordine_i2fvg', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_corretti'])
df_anagrafica[cols_to_use].to_csv(  file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned anagrafica data to {file_risultati}") 
# Filter out local units outside the FVG provinces from the CODICI dataset:
# - keep headquarters (loc_n == "0")
# - remove non-FVG local units
local_sede = ["0"]
prov_FVG = ["GO", "PN", "UD", "TS"]
unità_locali_extraFVG_filter = ~df_codici["loc_n"].isin(local_sede) & ~df_codici["prov"].isin(prov_FVG)
df_codici = df_codici[~unità_locali_extraFVG_filter]
print(f"After filtering, codici dataset has {df_codici.shape[0]} rows") 
# Save the CODICI file in the "Innovation Intelligence" version
# after removing local units outside the FVG region
file_risultati = data_dir + '\\' + 'i2fvg_codici_filtrato.csv'
cols_df = pd.read_excel(cols_path, sheet_name='codici',
            usecols = ['nomi_colonne_corretti','ordine_i2fvg']).dropna() 
cols_df.sort_values('ordine_i2fvg', inplace = True)
cols_to_use = list(cols_df['nomi_colonne_corretti'])
df_codici[cols_to_use].to_csv(      file_risultati, 
                                    sep ='|',   
                                    encoding='utf-8-sig', 
                                    index=False)
print(f"Saved cleaned codici data to {file_risultati}") 
