import argparse
import os

from download_datasets import download_usda_data
from preprocessing.process_foundation_and_sr_legacy import process_foundation_and_sr_legacy
from preprocessing.process_branded import process_branded



DEFAULT_BASE_DIR = 'fdc_data'


# Parse command-line arguments
parser = argparse.ArgumentParser(description='Download and process USDA Food Data Central datasets.')
parser.add_argument('--base_dir', default=DEFAULT_BASE_DIR, help='base directory (default: fdc_data)')
args = parser.parse_args()


# Define directories
BASE_DIR = args.base_dir
RAW_DIR = os.path.join(BASE_DIR, 'FoodData_Central_raw')
OUTPUT_DIR = os.path.join(BASE_DIR, 'FoodData_Central_processed')


# Ensure directories exist
if not os.path.exists(BASE_DIR):
    os.makedirs(BASE_DIR)

if not os.path.exists(RAW_DIR):
    os.makedirs(RAW_DIR)
    download_usda_data(RAW_DIR) # Download USDA datasets using download_usda_data function

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


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
processed_foundation_food = process_foundation_and_sr_legacy(FF_FOOD_NUTRIENT, FF_FOOD, FF_NUTRIENT, FF_CATEGORY, FF_PORTION, FF_MEASURE_UNIT)
processed_foundation_food.to_csv(os.path.join(OUTPUT_DIR, 'processed_foundation_food.csv'))

processed_legacy_food = process_foundation_and_sr_legacy(SR_FOOD_NUTRIENT, SR_FOOD, SR_NUTRIENT, SR_CATEGORY, SR_PORTION, SR_MEASURE_UNIT)
processed_legacy_food.to_csv(os.path.join(OUTPUT_DIR, 'processed_legacy_food.csv'))

processed_branded_food = process_branded(BF_BRANDED_FOOD, BF_FOOD_NUTRIENT, BF_FOOD, BF_NUTRIENT)
processed_branded_food.to_csv(os.path.join(OUTPUT_DIR, 'processed_branded_food.csv'))

# Stack the datasets
stacked_data = pd.concat([processed_foundation_food, processed_legacy_food, processed_branded_food])
stacked_data.reset_index(drop=True, inplace=True)

# Fill numeric columns with NaN values with default float
numeric_columns = [
    'fiber_total_dietary', 'sugars_total', 
    'calcium_ca', 'iron_fe', 'vitamin_c_total_ascorbic_acid', 'vitamin_a_rae', 'vitamin_e_alphatocopherol', 
    'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 
    'fatty_acids_total_polyunsaturated', 'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 
    'folate_total', 'vitamin_b12', 'vitamin_d3_cholecalciferol', 'vitamin_d2_ergocalciferol', 'pantothenic_acid', 
    'phosphorus_p', 'magnesium_mg', 'potassium_k', 'zinc_zn', 'copper_cu', 'manganese_mn', 'selenium_se', 
    'carotene_beta', 'retinol', 'vitamin_k_dihydrophylloquinone', 'vitamin_k_menaquinone4', 'tryptophan', 
    'threonine', 'methionine', 'phenylalanine', 'tyrosine', 'valine', 'arginine', 'histidine', 'isoleucine', 
    'leucine', 'lysine', 'cystine', 'alanine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'sucrose', 
    'glucose', 'maltose', 'fructose', 'lactose', 'galactose', 'choline_total', 'betaine'
]

for col in numeric_columns:
    stacked_data[col] = stacked_data[col].fillna(0).astype(float)
    

# Save stacked datasets
stacked_data.to_csv(os.path.join(OUTPUT_DIR, 'processed_usda_data.csv'), index=False)