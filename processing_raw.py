from config import Config
import os
from preprocessing.process_datasets import process_datasets

processed_foundation_food = process_datasets(Config.FF_FOOD_NUTRIENT, Config.FF_FOOD, Config.FF_NUTRIENT, Config.FF_CATEGORY, Config.FF_PORTION, Config.FF_MEASURE_UNIT)
processed_foundation_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_foundation_food.csv'))

processed_legacy_food = process_datasets(Config.SR_FOOD_NUTRIENT, Config.SR_FOOD, Config.SR_NUTRIENT, Config.SR_CATEGORY, Config.SR_PORTION, Config.SR_MEASURE_UNIT)
processed_legacy_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_legacy_food.csv'))

processed_branded_food = process_branded_food(Config.BF_FOOD_NUTRIENT, Config.BF_FOOD, Config.BF_NUTRIENT, Config.BF_CATEGORY, Config.BF_PORTION, Config.BF_MEASURE_UNIT)
processed_branded_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_branded_food.csv'))
