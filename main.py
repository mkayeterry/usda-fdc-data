import argparse
import os
import re
import json
import pandas as pd
import numpy as np

from preprocessing._utils import get_usda_urls, dict_to_json, postprocess_stacked_df, fillna_and_set_dtypes

from preprocessing.process_foundation import process_foundation
from preprocessing.process_srlegacy import process_srlegacy
from preprocessing.process_branded import process_branded

import warnings
warnings.simplefilter("ignore")

# ---------------------------------------------------------------------
# ---- Parse command-line arguments ----
# ---------------------------------------------------------------------

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description='download and process USDA Food Data Central datasets.')
parser.add_argument('--output_dir', default='fdc_data',
                    help='specify output directory path (default: fdc_data)')
parser.add_argument('--filename', default='usda_food_nutrition_data.csv',
                    help='specify output filename (default: usda_food_nutrition_data.csv')
parser.add_argument('--keep_files', action='store_true',
                    help='keep raw/indv files post-processing (default: delete raw/indv files)')
args = parser.parse_args()

# Check filename and extension
filename = args.filename
file_ext = filename.split('.')[-1] if '.' in filename else None
filename = filename if file_ext else f"{filename}.csv"

# Check keep_files flag
keep_files = args.keep_files
if keep_files:
    print(f"\n'keep_files' flag specified:\n> Raw and individual files will be kept after processing.")

# ---------------------------------------------------------------------
# ---- Setup raw and output directories ----
# ---------------------------------------------------------------------

# Define directories and ensure they exist (raw_dir existence will be checked in individual processing files)
OUTPUT_DIR = args.output_dir
RAW_DIR = os.path.join(OUTPUT_DIR, 'FoodData_Central_raw')
print(
    f'\nInitializing processing of USDA FDC data. Output directory set to:\n> {OUTPUT_DIR}\n')

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f'Directory created:\n> {OUTPUT_DIR}\n')

# ---------------------------------------------------------------------
# ---- Process USDA datasets ----
# ---------------------------------------------------------------------

# Gather urls and send them out to appropriate processing scripts
usda_urls = get_usda_urls()

foundation_urls = [
    url for url in usda_urls if 'foundation' in url or 'FoodData_Central_csv' in url]
srlegacy_url = [url for url in usda_urls if 'sr_legacy' in url][0]
branded_url = [url for url in usda_urls if 'branded' in url][0]

process_foundation(foundation_urls, OUTPUT_DIR, RAW_DIR, keep_files)
process_srlegacy(srlegacy_url, OUTPUT_DIR, RAW_DIR, keep_files)
process_branded(branded_url, OUTPUT_DIR, RAW_DIR, keep_files)

# ---------------------------------------------------------------------
# ---- Postprocess processed USDA datasets ----
# ---------------------------------------------------------------------

print(f'Initializing stacking of individually processed data:')
for root, dirs, files in os.walk(OUTPUT_DIR):
    for file in files:
        if '.parquet' in file:
            print(f'> {file}')

# Stack processed data
stacked_data = pd.concat([
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_foundation.parquet')),
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_srlegacy.parquet')),
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_branded.parquet'))
])

# Apply some post-processing
print(f'\nInitializing postprocessing of {filename}.\n')
stacked_data = postprocess_stacked_df(stacked_data)

# ---------------------------------------------------------------------
# ---- Save final output data and cleanup directories ----
# ---------------------------------------------------------------------

stacked_data.to_csv(os.path.join(OUTPUT_DIR, filename), index=False)

# Delete raw dir/individual processed files if keep_files flag is not specified
for root, dirs, files in os.walk(OUTPUT_DIR):
    for file in files:

        if not keep_files:

            if 'foundation' in file:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            if 'srlegacy' in file:
                file_path = os.path.join(root, file)
                os.remove(file_path)
            if 'branded' in file:
                file_path = os.path.join(root, file)
                os.remove(file_path)


print(
    f"Processing of USDA FDC data is complete. The processed data file ('{filename}') is now available in:\n> {OUTPUT_DIR}\n")
