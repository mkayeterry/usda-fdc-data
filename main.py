import argparse
import os
import pandas as pd

from preprocessing._utils import get_usda_urls

from preprocessing.process_foundation import process_foundation
from preprocessing.process_srlegacy import process_srlegacy
from preprocessing.process_branded import process_branded



DEFAULT_BASE_DIR = 'fdc_data'


# Parse command-line arguments
parser = argparse.ArgumentParser(description='download and process USDA Food Data Central datasets.')
parser.add_argument('--base_dir', default=DEFAULT_BASE_DIR, help='specify base directory (default: fdc_data)')
parser.add_argument('--delete_files', default=True, help='delete raw/indv files, leaving only processed data (default: True)')
args = parser.parse_args()


# Define delete_files flag
delete_files = args.delete_files
print(f'\nDelete files flag set to:')

if delete_files == True:
    print(f'> {delete_files}. Raw and individual files will be deleted after processing.\n')

if not delete_files == False:
    print(f'> {delete_files}. Raw and individual files will remain after processing.\n')


# Define directories
BASE_DIR = args.base_dir
RAW_DIR = os.path.join(BASE_DIR, 'FoodData_Central_raw')
print(f'Base directory set to:\n> {BASE_DIR}\n')


# Ensure base directory exists
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)
    print(f'Directory created:\n> {BASE_DIR}\n')




usda_urls = get_usda_urls()

foundation_urls = [url for url in usda_urls if 'foundation' in url or 'FoodData_Central_csv' in url]
srlegacy_url = [url for url in usda_urls if 'sr_legacy' in url][0]
branded_url = [url for url in usda_urls if 'branded' in url][0]

process_foundation(foundation_urls, BASE_DIR, RAW_DIR, delete_files)
process_srlegacy(srlegacy_url, BASE_DIR, RAW_DIR, delete_files)
process_branded(branded_url, BASE_DIR, RAW_DIR, delete_files)











# Define specific data type paths
for path in os.listdir(RAW_DIR):

    if 'foundation' in path:
        FOUNDATION_FOOD_DIR = os.path.join(RAW_DIR, path)

    if 'sr_legacy' in path:
        SR_LEGACY_FOOD_DIR = os.path.join(RAW_DIR, path)

    if 'branded' in path:
        BRANDED_FOOD_DIR = os.path.join(RAW_DIR, path)

    if 'FoodData_Central_csv' in path:
        FDC_ALL_DIR = os.path.join(RAW_DIR, path)

FF_FOOD_NUTRIENT = os.path.join(FOUNDATION_FOOD_DIR, 'food_nutrient.csv')
FF_FOOD = os.path.join(FOUNDATION_FOOD_DIR, 'food.csv')
FF_NUTRIENT = os.path.join(FOUNDATION_FOOD_DIR, 'nutrient.csv') 
FF_CATEGORY = os.path.join(FDC_ALL_DIR, 'food_category.csv')
FF_PORTION = os.path.join(FOUNDATION_FOOD_DIR, 'food_portion.csv')
FF_MEASURE_UNIT = os.path.join(FOUNDATION_FOOD_DIR, 'measure_unit.csv')

SR_FOOD_NUTRIENT = os.path.join(SR_LEGACY_FOOD_DIR, 'food_nutrient.csv')
SR_FOOD = os.path.join(SR_LEGACY_FOOD_DIR, 'food.csv')
SR_NUTRIENT = os.path.join(SR_LEGACY_FOOD_DIR, 'nutrient.csv')
SR_CATEGORY = os.path.join(SR_LEGACY_FOOD_DIR, 'food_category.csv')
SR_PORTION = os.path.join(SR_LEGACY_FOOD_DIR, 'food_portion.csv')
SR_MEASURE_UNIT = os.path.join(SR_LEGACY_FOOD_DIR, 'measure_unit.csv') 

BF_BRANDED_FOOD = os.path.join(BRANDED_FOOD_DIR, 'branded_food.csv')
BF_FOOD_NUTRIENT = os.path.join(BRANDED_FOOD_DIR, 'food_nutrient.csv')
BF_FOOD = os.path.join(BRANDED_FOOD_DIR, 'food.csv')
BF_NUTRIENT = os.path.join(BRANDED_FOOD_DIR, 'nutrient.csv')


# Process data
try:
    print(f'Initializing processing for:\n> {FOUNDATION_FOOD_DIR}\n')
    process_foundation_and_sr_legacy(FF_FOOD_NUTRIENT, FF_FOOD, FF_NUTRIENT, FF_CATEGORY, FF_PORTION, FF_MEASURE_UNIT)
except Exception as e:
    print(f'Error occurred while processing files in {FOUNDATION_FOOD_DIR}\n> {e}\n')


try:
    print(f'Initializing processing for:\n> {SR_LEGACY_FOOD_DIR}\n')
    process_foundation_and_sr_legacy(SR_FOOD_NUTRIENT, SR_FOOD, SR_NUTRIENT, SR_CATEGORY, SR_PORTION, SR_MEASURE_UNIT)
except Exception as e:
    print(f'Error occurred while processing files in {SR_LEGACY_FOOD_DIR}\n> {e}\n')


try:
    print(f'Initializing processing for:\n> {BRANDED_FOOD_DIR}\n')
    process_branded(BF_BRANDED_FOOD, BF_FOOD_NUTRIENT, BF_FOOD, BF_NUTRIENT)
except Exception as e:
    print(f'Error occurred while processing files in {BRANDED_FOOD_DIR}\n> {e}\n')


# Stack processed data, reorder columns, and save csv
stacked_data = pd.concat([processed_foundation_food, processed_legacy_food, processed_branded_food])
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
    fill_na_and_define_dtype(stacked_data, col)

stacked_data.to_parquet(os.path.join(BASE_DIR, 'processed_usda_data.parquet'), index=False)


# if delete_files == True:
    
#     # Remove raw data files
#     for root, dirs, files in os.walk(RAW_DIR):
#         for file in files:
#             file_path = os.path.join(root, file)
#             os.remove(file_path)

#     # Remove all directories (in reverse order)
#     for root, dirs, files in os.walk(RAW_DIR, topdown=False):
#         for dir in dirs:
#             dir_path = os.path.join(root, dir)
#             os.rmdir(dir_path)

#     os.rmdir(RAW_DIR)


#     # Remove individual processed data files and folder
#     for root, dirs, files in os.walk(OUTPUT_DIR):
#         for file in files:
#             file_path = os.path.join(root, file)
#             os.remove(file_path)

#     os.rmdir(OUTPUT_DIR)


print(f'Processing of USDA FDC data is complete. The processed data file ("processed_usda_data.parquet") is now available in:\n> {BASE_DIR}\n')
