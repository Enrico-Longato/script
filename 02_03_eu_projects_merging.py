#!/usr/bin/env python3
"""
EU Projects Data Processing Script

Authors: Andrea Bincoletto & Enrico Longato @ Area Science Park

Description: Python script to filter and concatenate data on European projects for FVG companies.

Files downloaded from: https://cordis.europa.eu/projects

The Framework Programs to download are:
- Horizon 2020 (folder "cordis-h2020projects-csv") 
  link: https://data.europa.eu/data/datasets/cordish2020projects?locale=en
- Horizon Europe (folder "cordis-HORIZONprojects-csv") 
  link: https://data.europa.eu/data/datasets/cordis-eu-research-projects-under-horizon-europe-2021-2027?locale=en

Each folder contains the following .csv files:
- euroSciVoc
- organization
- project
- legalBasis
- topics
- webItem
- webLink
"""

# ============================================================================
# SETUP
# ============================================================================

import sys
import os
from pathlib import Path
import datetime
import pandas as pd
import numpy as np
import io
import sharepy
import getpass
import openpyxl
import csv

pd.options.display.max_columns = None


# ============================================================================
# COMPANY REGISTRY - Load the list of FVG companies
# ============================================================================

base_path = Path.cwd()  
file_anagrafica = base_path / "data" / "anagrafica" / "iifvg_anagrafica_filtrato.csv"

print("Using this file:", file_anagrafica)

df_anagrafica = pd.read_csv(file_anagrafica, sep="|", engine="python", dtype=str)

# keep only active companies (only active companies are shown in I2FVG)
# df_anagrafica = df_anagrafica[df_anagrafica['stato_impresa'].isin(['ATTIVA'])]

filter_i2fvg = df_anagrafica[['piva','cf']]
filter_i2fvg = filter_i2fvg.drop_duplicates('piva')

print(f"The dataframe contains {filter_i2fvg.shape[0]} unique VAT numbers.")


# ============================================================================
# PROCESSING FRAMEWORK PROGRAM CSV FILES
# ============================================================================
"""
Processing steps:

1 - Access the files through Sharepoint authentication.

2 - The first step is to open the CSV file of the I2FVG registry (source Insiel), 
    updated monthly and contained in the Data Repository. Save the VAT numbers 
    and corresponding fiscal codes in a dataframe.

3 - Open the Cordis CSV files (organizations, projects and euroscivoc) contained 
    in the folder and filter the data for Italian cases, keeping information for 
    companies and projects throughout Italy (initially insert the downloaded folders 
    in the "input" folder).

4 - Finally, concatenate the dataframes of Horizon2020 and HorizonEurope.

NOTE: Use VAT numbers and not fiscal codes because Cordis uses those for companies! 
Corresponding fiscal codes are also inserted because PowerBI uses the fiscal code 
as a key between queries.
"""


# ============================================================================
# H2020 - HORIZON 2020
# ============================================================================

# ---- H2020 Organization ----

file = base_path / "data" / "eu_projects" / "h2020" / "organization.csv"
df_h2020_organization = pd.read_csv(file, sep=";", encoding='utf-8-sig', dtype=str)
# Use the code below if you get the following error:
# "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xbf in position 176070: invalid start byte"
# df_h2020_organization = pd.read_csv(file, sep=";", encoding='unicode_escape', dtype=str)

print(f"The dataframe contains {df_h2020_organization.shape[0]} total (non-unique) companies that participated in H2020 European projects.")


# Dataframe with a column containing the coordinator reference by projectID (to be inserted in the project csv)
df_h2020_coordinator = df_h2020_organization.copy()

df_h2020_coordinator = df_h2020_coordinator[['projectID', 'name', 'role']]

df_h2020_coordinator = df_h2020_coordinator.loc[df_h2020_coordinator['role'].isin(['coordinator'])]

df_h2020_coordinator = df_h2020_coordinator.groupby('projectID')['name'].apply(';'.join).reset_index()
df_h2020_coordinator = df_h2020_coordinator.rename(columns={
    'name':'coordinator',
    'projectID':'id'
})


# Dataframe with a column containing the partner reference per projectID (to be inserted in the project csv)
df_h2020_partners = df_h2020_organization.copy()

df_h2020_partners = df_h2020_partners[['projectID', 'name', 'role']]

# df_h2020_partners = df_h2020_partners.loc[~df_h2020_partners['role'].isin(['coordinator'])]
# Consider also 'thirdParty', 'InternationalPartner' and 'partner'
df_h2020_partners = df_h2020_partners.loc[df_h2020_partners['role'].isin(['participant'])]

df_h2020_partners = df_h2020_partners.groupby('projectID')['name'].apply(';'.join).reset_index()
df_h2020_partners = df_h2020_partners.rename(columns={
    'name':'participants',
    'projectID':'id'
})


# Keep only Italian companies ("country" = IT)
df_h2020_organization = df_h2020_organization[df_h2020_organization['country'].isin(['IT'])]
print(f"The dataframe contains {df_h2020_organization.shape[0]} Italian companies that participated in European projects.")


# Filter out rows with invalid VAT numbers
df_h2020_organization = (
    df_h2020_organization
    .loc[~df_h2020_organization['vatNumber'].isin(
        ['MISSING', 'NOTAPPLICABLE', 'VATEXEMPTION']
    )]
    .copy()
)

# Replace empty strings with NaN
df_h2020_organization['vatNumber'] = (
    df_h2020_organization['vatNumber'].replace('', np.nan)
)

# Remove NaN values
df_h2020_organization = df_h2020_organization.dropna(subset=['vatNumber'])


# Strip "IT" in vatNumber column
df_h2020_organization['vatNumber'] = df_h2020_organization['vatNumber'].map(lambda vatNumber: str(vatNumber)[2:])


# Drop rows of VAT numbers that do not have 11 digits
df_h2020_organization['vatnumber_lenght'] = df_h2020_organization['vatNumber'].apply(lambda number: len(number))
print("The VAT numbers in the dataframe have the following lengths: ")
print(df_h2020_organization['vatnumber_lenght'].value_counts().head(10))

df_h2020_organization = df_h2020_organization.loc[df_h2020_organization['vatnumber_lenght'].isin([11])]

print('--------')
print(f"After cleaning, the dataframe contains {df_h2020_organization.shape[0]} Italian companies that participated in European projects.")


# Replace dot with comma in "ecContribution" and "netEcContribution"
list_column = ['ecContribution','netEcContribution']
for i in list_column:
    df_h2020_organization[i] = df_h2020_organization[i].str.replace('.',',')
    print("Done!")


# Merge with dataframes with tax codes of i2fvg companies
df_h2020_organization = pd.merge(df_h2020_organization, filter_i2fvg, how='left', left_on='vatNumber', right_on='piva')


# Drop vatnumber_lenght column
df_h2020_organization = df_h2020_organization.drop('vatnumber_lenght', axis=1)
df_h2020_organization = df_h2020_organization.drop('piva', axis=1)


# Filter VAT numbers by filter_i2fvg (not to be used): check for FVG
df_h2020_organization_i2fvg = df_h2020_organization[df_h2020_organization['vatNumber'].isin(filter_i2fvg['piva'])]
print(f"The total number of FVG companies with matches in European projects is {df_h2020_organization_i2fvg.shape[0]}.")
print(f"The unique FVG companies in I2FVG that participated in H2020 European projects are {df_h2020_organization_i2fvg['vatNumber'].nunique()}.")


# ---- H2020 Project ----

file = base_path / "data" / "eu_projects" / "h2020" / "project.csv"

df_h2020_project = pd.read_csv(file, sep=";", encoding="utf-8-sig", dtype=str, quotechar='"', on_bad_lines="skip")
print(f"The dataframe contains {df_h2020_project.shape[0]} European projects.")


# Insert columns with reference to the coordinator and project partner
df_h2020_project = pd.merge(left=df_h2020_project, right=df_h2020_coordinator, on='id', how='left')
df_h2020_project = pd.merge(left=df_h2020_project, right=df_h2020_partners, on='id', how='left')


# Filter European projects of Italian companies only
filter_IT_h2020_projects = df_h2020_organization['projectID']
filter_IT_h2020_projects = filter_IT_h2020_projects.drop_duplicates()

df_h2020_project = df_h2020_project[df_h2020_project['id'].isin(filter_IT_h2020_projects)]
print(f"The total number of European projects is {df_h2020_project.shape[0]}.")


# Filter FVG projects (not to be used)
filter_i2fvg_h2020_project = df_h2020_organization_i2fvg['projectID']
filter_i2fvg_h2020_project = filter_i2fvg_h2020_project.drop_duplicates()
print(f"The total number of European projects from I2FVG companies is {filter_i2fvg_h2020_project.shape[0]}.")


# ---- H2020 euroSciVoc ----

file = base_path / "data" / "eu_projects" / "h2020" / "euroSciVoc.csv"

df_h2020_euroSciVoc = pd.read_csv(file, sep=";", encoding="utf-8-sig", dtype=str, quotechar='"', on_bad_lines="skip")
print(f"The dataframe contains {df_h2020_euroSciVoc.shape[0]} euroSciVoc entries.")


print(f"The total number of unique euroSciVocTitle entries is {df_h2020_euroSciVoc['euroSciVocTitle'].drop_duplicates().count()}.")
print(f"The main euroSciVocTitle entries are: \n{df_h2020_euroSciVoc['euroSciVocTitle'].value_counts().head(5)}")


# Filter euroSciVoc with filter_IT_h2020_projects
df_h2020_euroscivoc = df_h2020_euroSciVoc[df_h2020_euroSciVoc['projectID'].isin(filter_IT_h2020_projects)]
df_h2020_euroscivoc.shape


# Create a column with the first level of the euroscivoc path (6 in total)
df_h2020_euroscivoc = df_h2020_euroscivoc.copy()

df_h2020_euroscivoc['livello_1'] = (
    df_h2020_euroscivoc['euroSciVocPath']
        .str.split('/')
        .str[1]
)

df_h2020_euroscivoc['livello_1'].value_counts().head(6)


# Create a column with the second level of the euroscivoc path (if missing, the first level is inserted)
df_h2020_euroscivoc = df_h2020_euroscivoc.copy()

df_h2020_euroscivoc['livello_2'] = (
    df_h2020_euroscivoc['euroSciVocPath']
        .str.split('/')
        .str[2]                     # extract level 2 directly
        .fillna(df_h2020_euroscivoc['livello_1'])   # fallback to level 1
)

print(df_h2020_euroscivoc['livello_2'].value_counts().head(5))


# Filter the euroSciVocTitle and livello_1 for I2FVG companies (not to be used)
filter_i2fvg_h2020_euroscivoc = df_h2020_euroscivoc[df_h2020_euroscivoc['projectID'].isin(filter_i2fvg_h2020_project)]
print(f"The main euroSciVocTitle entries for I2FVG are: \n{filter_i2fvg_h2020_euroscivoc['euroSciVocTitle'].value_counts().head(5)}")
print("----------------------------------------")
print(f"The livello_1 entries for I2FVG are: \n{filter_i2fvg_h2020_euroscivoc['livello_1'].value_counts()}")
print("----------------------------------------")
print(f"The main livello_2 entries for I2FVG are: \n{filter_i2fvg_h2020_euroscivoc['livello_2'].value_counts().head(5)}")


# ============================================================================
# HORIZON EUROPE
# ============================================================================

# ---- Horizon Europe Organization ----

file = base_path / "data" / "eu_projects" / "horizon_europe" / "organization.csv"
df_he_organization = pd.read_csv(file, sep=";", encoding='utf-8-sig', dtype=str)
print(f"The dataframe contains {df_he_organization.shape[0]} total (non-unique) companies that participated in Horizon Europe European projects.")


# Dataframe with a column containing the coordinator reference by projectID (to be inserted in the project csv)
df_he_coordinator = df_he_organization.copy()

df_he_coordinator = df_he_coordinator[['projectID', 'name', 'role']]

df_he_coordinator = df_he_coordinator.loc[df_he_coordinator['role'].isin(['coordinator'])]

df_he_coordinator = df_he_coordinator.groupby('projectID')['name'].apply(';'.join).reset_index()
df_he_coordinator = df_he_coordinator.rename(columns={
    'name':'coordinator',
    'projectID':'id'
})


# Dataframe with a column containing the partner reference per projectID (to be inserted in the project csv)
df_he_partners = df_he_organization.copy()

df_he_partners = df_he_partners[['projectID', 'name', 'role']]

# df_he_partners = df_he_partners.loc[~df_he_partners['role'].isin(['coordinator'])]
# Consider also 'thirdParty', 'InternationalPartner' and 'partner'
df_he_partners = df_he_partners.loc[df_he_partners['role'].isin(['participant'])]

df_he_partners = df_he_partners.groupby(['projectID'], as_index=False).agg({'name': lambda x: ';'.join(x.astype(str))})
df_he_partners = df_he_partners.rename(columns={
    'name':'participants',
    'projectID':'id'
})


# Keep only Italian companies ("country" = IT)
df_he_organization = df_he_organization[df_he_organization['country'].isin(['IT'])]
print(f"The dataframe now contains {df_he_organization.shape[0]} Italian companies that participated in European projects.")


# Remove missing VAT numbers from the vatNumber column:
# - MISSING
# - VATEXEMPTION
# - NOTAPPLICABLE
df_he_organization = df_he_organization.loc[~df_he_organization['vatNumber'].isin(['MISSING','NOTAPPLICABLE','VATEXEMPTION'])]

# Drop blank rows in vatNumber columns
df_he_organization['vatNumber'] = (df_he_organization['vatNumber'].replace('', np.nan))
df_he_organization.dropna(subset=['vatNumber'], inplace=True)


# Strip "IT" in vatNumber column
df_he_organization['vatNumber'] = df_he_organization['vatNumber'].map(lambda vatNumber: str(vatNumber)[2:])


# Drop rows of VAT numbers that do not have 11 digits
df_he_organization['vatnumber_lenght'] = df_he_organization['vatNumber'].apply(lambda number: len(number))
print("The VAT numbers in the dataframe have the following lengths: ")
print(df_he_organization['vatnumber_lenght'].value_counts().head(10))

df_he_organization = df_he_organization.loc[df_he_organization['vatnumber_lenght'].isin([11])]

print('--------')
print(f"After cleaning, the dataframe has {df_he_organization.shape[0]} rows.")


# Replace dot with comma in "ecContribution" and "netEcContribution"
list_column = ['ecContribution','netEcContribution']
for i in list_column:
    df_he_organization[i] = df_he_organization[i].str.replace('.',',')
    print("Done!")


# Merge with dataframes with tax codes of i2fvg companies
df_he_organization = pd.merge(df_he_organization, filter_i2fvg, how='left', left_on='vatNumber', right_on='piva')


# Drop vatnumber_lenght column
df_he_organization = df_he_organization.drop('vatnumber_lenght', axis=1)
df_he_organization = df_he_organization.drop('piva', axis=1)


# Filter VAT numbers by filter_i2fvg (not to be used)
df_he_organization_i2fvg = df_he_organization[df_he_organization['vatNumber'].isin(filter_i2fvg['piva'])]
print(f"The total number of FVG companies with matches in European projects is {df_he_organization_i2fvg.shape[0]}.")
print(f"The unique FVG companies in I2FVG that participated in Horizon Europe projects are {df_he_organization_i2fvg['vatNumber'].nunique()}.")


# ---- Horizon Europe Project ----

file = base_path / "data" / "eu_projects" / "horizon_europe" / "project.csv"

df_he_project = pd.read_csv(file, sep=";", encoding="utf-8-sig", dtype=str, quotechar='"', on_bad_lines="skip")

print(f"The dataframe contains {df_he_project.shape[0]} European projects.")


# Insert columns with reference to the coordinator and project partner 
df_he_project = pd.merge(left=df_he_project, right=df_he_coordinator, on='id', how='left')
df_he_project = pd.merge(left=df_he_project, right=df_he_partners, on='id', how='left')


# Filter European projects of Italian companies only
filter_IT_he_projects = df_he_organization['projectID']
filter_IT_he_projects = filter_IT_he_projects.drop_duplicates()

df_he_project = df_he_project[df_he_project['id'].isin(filter_IT_he_projects)]
print(f"The total number of European projects is {df_he_project.shape[0]}.")


# Filter FVG projects (not to be used)
filter_i2fvg_he_project = df_he_organization_i2fvg['projectID']
filter_i2fvg_he_project = filter_i2fvg_he_project.drop_duplicates()
print(f"The total number of European projects from I2FVG companies is {filter_i2fvg_he_project.shape[0]}.")


# ---- Horizon Europe EuroSciVoc ----

file = base_path / "data" / "eu_projects" / "horizon_europe" / "euroSciVoc.csv"

df_he_euroSciVoc = pd.read_csv(file, sep=";", encoding="utf-8-sig", dtype=str, quotechar='"', on_bad_lines="skip")
print(f"The dataframe contains {df_he_euroSciVoc.shape[0]} euroSciVoc entries.")


print(f"The total number of unique euroSciVocTitle entries is {df_he_euroSciVoc['euroSciVocTitle'].drop_duplicates().count()}.")
print(f"The main euroSciVocTitle entries are: \n{df_he_euroSciVoc['euroSciVocTitle'].value_counts().head(5)}")


# Filter euroSciVoc with filter_IT_projects
df_he_euroscivoc = df_he_euroSciVoc[df_he_euroSciVoc['projectID'].isin(filter_IT_he_projects)]
df_he_euroscivoc.shape


# If df_he_euroscivoc comes from a filter → ensure it is a copy
df_he_euroscivoc = df_he_euroscivoc.copy()

# Create column with the first level of the path
df_he_euroscivoc.loc[:, 'livello_1'] = (
    df_he_euroscivoc['euroSciVocPath']
        .astype(str)
        .str.split('/')
        .str[1]
)

# Count
df_he_euroscivoc['livello_1'].value_counts().head(6)


# Create a column with the second level of the euroscivoc path (if missing, the first level is inserted)
df_he_euroscivoc['livello_2'] = df_he_euroscivoc['euroSciVocPath'].str.split('/').str[2:3].str.join('/')

df_he_euroscivoc['livello_2'] = np.where(df_he_euroscivoc['livello_2'] == '', df_he_euroscivoc['livello_1'], df_he_euroscivoc['livello_2'])

print(df_he_euroscivoc["livello_2"].value_counts().head(5))


# Filter the euroSciVocTitle and livello_1 for I2FVG companies (not to be used)
filter_i2fvg_he_euroscivoc = df_he_euroscivoc[df_he_euroscivoc['projectID'].isin(filter_i2fvg_he_project)]
print(f"The main euroSciVocTitle entries for I2FVG are: \n{filter_i2fvg_he_euroscivoc['euroSciVocTitle'].value_counts().head(5)}")
print("----------------------------------------")
print(f"The livello_1 entries for I2FVG are: \n{filter_i2fvg_he_euroscivoc['livello_1'].value_counts()}")
print("----------------------------------------")
print(f"The main livello_2 entries for I2FVG are: \n{filter_i2fvg_he_euroscivoc['livello_2'].value_counts().head(5)}")


# ============================================================================
# CONCATENATE DATAFRAMES
# ============================================================================

df_organization_final = pd.concat(
    [df_h2020_organization, df_he_organization],
    ignore_index=True
)

df_project_final = pd.concat(
    [df_h2020_project, df_he_project],
    ignore_index=True
)

df_euroscivoc_final = pd.concat(
    [df_h2020_euroscivoc, df_he_euroscivoc],
    ignore_index=True
)


# ============================================================================
# FINAL PREPARATION OF DATAFRAMES
# ============================================================================

# Rename "id" column to "projectID"
df_project_final = df_project_final.rename(columns={
    'id':'projectID',
})


# Replace "HORIZON" with "HORIZON EUROPE" in the 'frameworkProgramme' column
df_project_final['frameworkProgramme'] = df_project_final['frameworkProgramme'].str.replace('HORIZON', 'HORIZON EUROPE')


# Delete hours/minutes/seconds from datetime columns + change type object--->datetime64
# Using errors='coerce'. It will replace all non-numeric values with NaN
df_organization_final["contentUpdateDate"] = pd.to_datetime(df_organization_final["contentUpdateDate"], errors="coerce", dayfirst=True).dt.date
df_organization_final["contentUpdateDate"] = pd.to_datetime(df_organization_final["contentUpdateDate"])


# Delete hours/minutes/seconds from datetime columns + change type object--->datetime64
# Using errors='coerce'. It will replace all non-numeric values with NaN
list_date = ['startDate', 'endDate', 'ecSignatureDate', 'contentUpdateDate']

for col in list_date:
    df_project_final[col] = pd.to_datetime(df_project_final[col], errors='coerce')
    print(f"Column [{col}] fixed!")


# ============================================================================
# SAVE TO OUTPUT FOLDER
# ============================================================================

# Organization
csv_data = df_organization_final.to_csv(sep='|', index=False).encode('utf-8-sig')
path = base_path / "data" / "eu_projects" / "merge" / "organization.csv"

with open(path, 'wb') as file:
    file.write(csv_data)

print(f"Organization file saved to: {path}")


# Project
csv_data = df_project_final.to_csv(sep='|', index=False).encode('utf-8-sig')
path = base_path / "data" / "eu_projects" / "merge" / "project.csv"

with open(path, 'wb') as file:
    file.write(csv_data)

print(f"Project file saved to: {path}")


# EuroSciVoc
csv_data = df_euroscivoc_final.to_csv(sep='|', index=False).encode('utf-8-sig')
path = base_path / "data" / "eu_projects" / "merge" / "euroscivoc.csv"

with open(path, 'wb') as file:
    file.write(csv_data)

print(f"EuroSciVoc file saved to: {path}")

print("\nData processing completed successfully!")
