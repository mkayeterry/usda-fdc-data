from config import Config
import os
from preprocessing.process_usda_data import process_usda_data

processed_foundation_food = process_usda_data(Config.FF_FOOD_NUTRIENT, Config.FF_FOOD, Config.FF_NUTRIENT, Config.FF_CATEGORY, Config.FF_PORTION, Config.FF_MEASURE_UNIT)
processed_foundation_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_foundation_food.csv'))

processed_legacy_food = process_usda_data(Config.LF_FOOD_NUTRIENT, Config.LF_FOOD, Config.LF_NUTRIENT, Config.LF_CATEGORY, Config.LF_PORTION, Config.LF_MEASURE_UNIT)
processed_legacy_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_legacy_food.csv'))

# processed_branded_food = process_branded_food(Config.BRANDED_FOOD_PATHS)
# processed_branded_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_branded_food.csv'))

data_paths = Config.SR_LEGACY_FOOD_PATHS