import argparse
import os

from download_datasets import download_usda_data
from preprocessing.process_foundation_and_sr_legacy import process_foundation_and_sr_legacy
from preprocessing.process_branded import process_branded



DEFAULT_BASE_DIR = 'fdc_data'


# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process some directories.')
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
processed_foundation_food = process_foundation_and_sr_legacy(Config.FF_FOOD_NUTRIENT, Config.FF_FOOD, Config.FF_NUTRIENT, Config.FF_CATEGORY, Config.FF_PORTION, Config.FF_MEASURE_UNIT)
processed_foundation_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_foundation_food.csv'))

processed_legacy_food = process_foundation_and_sr_legacy(Config.SR_FOOD_NUTRIENT, Config.SR_FOOD, Config.SR_NUTRIENT, Config.SR_CATEGORY, Config.SR_PORTION, Config.SR_MEASURE_UNIT)
processed_legacy_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_legacy_food.csv'))

processed_branded_food = process_branded(Config.BF_BRANDED_FOOD, Config.BF_FOOD_NUTRIENT, Config.BF_FOOD, Config.BF_NUTRIENT)
processed_branded_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_branded_food.csv'))

