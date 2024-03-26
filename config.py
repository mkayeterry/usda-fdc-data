import os
# from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.dirname(__name__))
# load_dotenv(os.path.join(BASE_DIR, '.env'))

class Config:
    # Base directory and path configurations
    # BASE_DIR_NAME = 'fdc_data'
    # BASE_DIR_PATH = os.getenv('BASE_DIR_PATH')

    # Define paths for raw and output directories
    BASE_DIR = 'fdc_data'
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)

    OUTPUT_DIR = os.path.join(BASE_DIR, 'fdc_output')
    if not os.path.exists(OUTPUT_DIR):
         os.makedirs(OUTPUT_DIR)

    for path in os.listdir(BASE_DIR):

        if 'foundation' in path:
            print(path)
            FOUNDATION_FOOD_DIR = os.path.join(BASE_DIR, path)
        if 'sr_legacy' in path:
            print(path)
            SR_LEGACY_FOOD_DIR = os.path.join(BASE_DIR, path)
        if 'branded' in path:
            print(path)
            BRANDED_FOOD_DIR = os.path.join(BASE_DIR, path)
        else:
            FDC_ALL_DIR = os.path.join(BASE_DIR, path)


    FOUNDATION_FOOD_PATHS = {
                "ff_food_nutrient": os.path.join(FOUNDATION_FOOD_DIR, 'food_nutrient.csv'), 
                "ff_food": os.path.join(FOUNDATION_FOOD_DIR, 'food.csv'), 
                "ff_nutrient": os.path.join(FOUNDATION_FOOD_DIR, 'nutrient.csv'), 
                "ff_category": os.path.join(FDC_ALL_DIR, 'food_category.csv'), 
                "ff_portion": os.path.join(FOUNDATION_FOOD_DIR, 'food_portion.csv'), 
                "ff_measure_unit": os.path.join(FOUNDATION_FOOD_DIR, 'measure_unit.csv')
                }


    SR_LEGACY_FOOD_PATHS = {
                "lf_food_nutrient": os.path.join(SR_LEGACY_FOOD_DIR, 'food_nutrient.csv'), 
                "lf_food": os.path.join(SR_LEGACY_FOOD_DIR, 'food.csv'), 
                "lf_nutrient": os.path.join(SR_LEGACY_FOOD_DIR, 'nutrient.csv'), 
                "lf_category": os.path.join(SR_LEGACY_FOOD_DIR, 'food_category.csv'), 
                "lf_portion": os.path.join(SR_LEGACY_FOOD_DIR, 'food_portion.csv'), 
                "lf_measure_unit": os.path.join(SR_LEGACY_FOOD_DIR, 'measure_unit.csv')
                }


    # BRANDED_FOOD_PATHS = {
    #             "bf_food_nutrient": os.path.join(BRANDED_FOOD_DIR, 'food_nutrient.csv'), 
    #             "bf_food": os.path.join(BRANDED_FOOD_DIR, 'food.csv'), 
    #             "bf_nutrient": os.path.join(BRANDED_FOOD_DIR, 'nutrient.csv'), 
    #             "bf_category": os.path.join(FDC_ALL_DIR, 'food_category.csv'), 
    #             "bf_portion": os.path.join(FDC_ALL_DIR, 'food_portion.csv')
    #             }


