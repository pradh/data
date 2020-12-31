# NPI Registry Import

## About the Dataset

[US
CMS](https://en.wikipedia.org/wiki/Centers_for_Medicare_%26_Medicaid_Services)
assigns a [National Provider
Identifier](https://en.wikipedia.org/wiki/National_Provider_Identifier) (NPI)
for all individual and organization health-care providers in the US. The NPI
Registry dataset includes records with information on all registered
health-care providers in the US, identified by the NPI ID.

The NPI dataset is available in a large CSV at
<https://download.cms.gov/nppes/NPI_Files.html>; referred to as the "Full
Replacement Monthly NPI File". The list of columns is described in a README
present in the downloaded ZIP file, but
[here](https://www.google.com/url?q=https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand/Downloads/Data_Dissemination_File-Readme.pdf&sa=D&ust=1609378657505000&usg=AOvVaw22zC0EYs80pc-gyRLdoVEi)
is a stale version online.

## Importing the data

The `preprocess.py` script has logic to download the full ZIP file, clean the
CSV and produce associated TMCF file.

To download and generate CSV and TMCF:

```
python3 preprocess.py --zip_url=https://download.cms.gov/nppes/NPPES_Data_Dissemination_December_2020.zip
```

This would download the ZIP file to `npi_registry.zip` and produce
`npi_cleaned.csv` and `npi.tmcf` that are ready for import.

NOTE: This import relies on Health Care Provider Taxonomy Codes that is part of
a separate import.
