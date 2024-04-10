import argparse
import os
import pandas as pd

from preprocessing._utils import get_usda_urls
from preprocessing._utils import fillna_and_define_dtype

from preprocessing.process_foundation import process_foundation
from preprocessing.process_srlegacy import process_srlegacy
from preprocessing.process_branded import process_branded


# Parse command-line arguments
parser = argparse.ArgumentParser(description='download and process USDA Food Data Central datasets.')
parser.add_argument('--base_dir', default='fdc_data', help='specify base directory (default: fdc_data)')
parser.add_argument('--keep_files', action='store_true', help='keep raw/indv files, as well as processed data')
args = parser.parse_args()


keep_files = args.keep_files
if keep_files:
    print(f'\nkeep_files flag specified:\n> Raw and individual files will be kept after processing.\n')


# Define directories and ensure they exist (raw_dir existence will be checked in individual processing files)
BASE_DIR = args.base_dir
RAW_DIR = os.path.join(BASE_DIR, 'FoodData_Central_raw')
print(f'Base directory set to:\n> {BASE_DIR}\n')

if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)
    print(f'Directory created:\n> {BASE_DIR}\n')


# Gather urls and send them out to appropriate processing files
usda_urls = get_usda_urls()

foundation_urls = [url for url in usda_urls if 'foundation' in url or 'FoodData_Central_csv' in url]
srlegacy_url = [url for url in usda_urls if 'sr_legacy' in url][0]
branded_url = [url for url in usda_urls if 'branded' in url][0]

process_foundation(foundation_urls, BASE_DIR, RAW_DIR, keep_files)
process_srlegacy(srlegacy_url, BASE_DIR, RAW_DIR, keep_files)
process_branded(branded_url, BASE_DIR, RAW_DIR, keep_files)

print(f'Initializing stacking of individually processed data:')
for root, dirs, files in os.walk(BASE_DIR):
    for file in files:
        print(f'> {file}')
# Stack processed data, reorder columns, and save csv
stacked_data = pd.concat([
    pd.read_parquet(os.path.join(BASE_DIR, 'processed_foundation.parquet')),
    pd.read_parquet(os.path.join(BASE_DIR, 'processed_srlegacy.parquet')),
    pd.read_parquet(os.path.join(BASE_DIR, 'processed_branded.parquet'))
])
stacked_data.reset_index(drop=True, inplace=True)

stacked_data = stacked_data[[
                        'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 'brand_name', 'food_description', 'ingredients', 
                        'portion_amount', 'portion_unit', 'portion_modifier', 'standardized_quantity', 'standardized_portion', 'portion_gram_weight', 
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

stacked_data.to_csv(os.path.join(BASE_DIR, 'processed_usda_data.csv'), index=False)


# Delete raw dir/individual processed files if keep_files flag is not specified
for root, dirs, files in os.walk(BASE_DIR):
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


    print(f'\nProcessing of USDA FDC data is complete. The processed data file ({file}) is now available in:\n> {root}\n')
