from config import Config
import os
from preprocessing.sr_legacy_food import process_legacy_foods


processed_legacy_food = process_legacy_foods(Config.SR_LEGACY_FOOD_PATHS)
processed_legacy_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_legacy_food.csv'))

processed_foundation_food = process_legacy_foods(Config.SR_LEGACY_FOOD_PATHS)
processed_foundation_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_foundation_food.csv'))