import argparse
import os
import pandas as pd
import warnings

from preprocessing._utils import get_usda_urls
from preprocessing._utils import fillna_and_define_dtype

from preprocessing.process_foundation import process_foundation
from preprocessing.process_srlegacy import process_srlegacy
from preprocessing.process_branded import process_branded


# Parse command-line arguments
parser = argparse.ArgumentParser(description='download and process USDA Food Data Central datasets.')
parser.add_argument('--output_dir', default='fdc_data', help='specify output directory path (default: fdc_data)')
parser.add_argument('--filename', default='usda_food_nutrition_data.csv', help='specify output filename and extension (default: usda_food_nutrition_data.csv)')
parser.add_argument('--keep_files', action='store_true', help='keep raw/indv files post-processing (default: delete raw/indv files)')
args = parser.parse_args()

# Check filename and extension, default to csv if not in supported format
filename = args.filename
file_ext = filename.split('.')[-1]
save_exts = ['csv', 'xlsx', 'xls', 'hdf', 'json', 'parquet', 'feather', 'sql']
if file_ext not in save_exts:
    print(f"\nUserWarning: The file extension '{file_ext}' is not in the list of supported extensions: {', '.join(save_exts)}\n> File will be saved to 'csv' as default.")
    filename = filename.split('.')[0] + '.csv'


keep_files = args.keep_files
if keep_files:
    print(f"\n'keep_files' flag specified:\n> Raw and individual files will be kept after processing.")


# Define directories and ensure they exist (raw_dir existence will be checked in individual processing files)
OUTPUT_DIR = args.output_dir
RAW_DIR = os.path.join(OUTPUT_DIR, 'FoodData_Central_raw')
print(f'\nInitializing processing of USDA FDC data. Output directory set to:\n> {OUTPUT_DIR}\n')


if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f'Directory created:\n> {OUTPUT_DIR}\n')


# Gather urls and send them out to appropriate processing scripts
usda_urls = get_usda_urls()

foundation_urls = [url for url in usda_urls if 'foundation' in url or 'FoodData_Central_csv' in url]
srlegacy_url = [url for url in usda_urls if 'sr_legacy' in url][0]
branded_url = [url for url in usda_urls if 'branded' in url][0]

process_foundation(foundation_urls, OUTPUT_DIR, RAW_DIR, keep_files)
process_srlegacy(srlegacy_url, OUTPUT_DIR, RAW_DIR, keep_files)
process_branded(branded_url, OUTPUT_DIR, RAW_DIR, keep_files)

print(f'Initializing stacking of individually processed data:')
for root, dirs, files in os.walk(OUTPUT_DIR):
    for file in files:
        if '.parquet' in file:
            print(f'> {file}')


# Stack processed data, reorder columns, and save csv
stacked_data = pd.concat([
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_foundation.parquet')),
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_srlegacy.parquet')),
    pd.read_parquet(os.path.join(OUTPUT_DIR, 'processed_branded.parquet'))
])
stacked_data.reset_index(drop=True, inplace=True)

stacked_data = stacked_data[[
                        'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 'brand_name', 'food_description', 'ingredients', 
                        'portion_amount', 'portion_unit', 'portion_modifier', 'std_portion_amount', 'std_portion_unit', 'portion_gram_weight', 
                        'portion_energy', 'energy', 'carbohydrate_by_difference', 'protein', 'total_lipid_fat', 'fiber_total_dietary', 'sugars_total', 
                        'calcium_ca', 'iron_fe', 'vitamin_c_total_ascorbic_acid', 'vitamin_a_rae', 'vitamin_e_alphatocopherol', 
                        'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 
                        'fatty_acids_total_polyunsaturated', 'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 
                        'vitamin_b12', 'vitamin_d3_cholecalciferol', 'vitamin_d2_ergocalciferol', 'pantothenic_acid', 'phosphorus_p', 'magnesium_mg', 
                        'potassium_k', 'zinc_zn', 'copper_cu', 'manganese_mn', 'selenium_se', 'carotene_beta', 'retinol', 'vitamin_k_dihydrophylloquinone', 
                        'vitamin_k_menaquinone4', 'tryptophan', 'threonine', 'methionine', 'phenylalanine', 'tyrosine', 'valine', 'arginine', 'histidine', 
                        'isoleucine', 'leucine', 'lysine', 'cystine', 'alanine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'sucrose', 'glucose', 
                        'maltose', 'fructose', 'lactose', 'galactose', 'choline_total', 'betaine'
                    ]]

for col in stacked_data.columns.tolist():
    fillna_and_define_dtype(stacked_data, col)


# Determine the appropriate save method based on file extension
save_method = getattr(stacked_data, f"to_{filename.split('.')[-1]}")
save_method(os.path.join(OUTPUT_DIR, filename), index=False)


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


print(f"\nProcessing of USDA FDC data is complete. The processed data file ('usda_food_nutrition_data.parquet') is now available in:\n> {OUTPUT_DIR}\n")

