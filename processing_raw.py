from config import Config
import os
from preprocessing.process_ff_sr import process_ff_sr
from preprocessing.process_bf import process_bf

processed_foundation_food = process_ff_sr(Config.FF_FOOD_NUTRIENT, Config.FF_FOOD, Config.FF_NUTRIENT, Config.FF_CATEGORY, Config.FF_PORTION, Config.FF_MEASURE_UNIT)
processed_foundation_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_foundation_food.csv'))

processed_legacy_food = process_ff_sr(Config.SR_FOOD_NUTRIENT, Config.SR_FOOD, Config.SR_NUTRIENT, Config.SR_CATEGORY, Config.SR_PORTION, Config.SR_MEASURE_UNIT)
processed_legacy_food.to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_legacy_food.csv'))

processed_branded_food = process_bf(Config.BF_BRANDED_FOOD, Config.BF_FOOD_NUTRIENT, Config.BF_FOOD, Config.BF_NUTRIENT)
processed_branded_food.head(1000).to_csv(os.path.join(Config.OUTPUT_DIR, 'processed_branded_food.csv'))
