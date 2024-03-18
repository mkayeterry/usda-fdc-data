from config import Config
import os
from preprocessing.sr_legacy_food import process_legacy_foods
from preprocessing.foundation_food import process_foundation_foods
from preprocessing.survey_food import process_survey_foods


processed_legacy_food = process_legacy_foods(Config.SR_LEGACY_FOOD_PATHS)
processed_legacy_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_legacy_food.csv'))

processed_foundation_food = process_legacy_foods(Config.SR_LEGACY_FOOD_PATHS)
processed_foundation_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_foundation_food.csv'))

processed_survey_food = process_legacy_foods(Config.SURVEY_FOOD_PATHS)
processed_survey_food.to_csv(os.path.join(Config.PROCESSED_DIR, 'processed_survey_food.csv'))

data_paths = Config.SURVEY_FOOD_PATHS