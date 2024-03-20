from config import Config
import os
from preprocessing.sr_legacy_food import process_legacy_food
from preprocessing.foundation_food import process_foundation_food
from preprocessing.branded_food import process_branded_food


processed_legacy_food = process_legacy_food(Config.SR_LEGACY_FOOD_PATHS)
processed_legacy_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_legacy_food.csv'))

processed_foundation_food = process_foundation_food(Config.FOUNDATION_FOOD_PATHS)
processed_foundation_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_foundation_food.csv'))

processed_branded_food = process_branded_food(Config.BRANDED_FOOD_PATHS)
processed_branded_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_branded_food.csv'))

# data_paths = Config.SR_LEGACY_FOOD_PATHS