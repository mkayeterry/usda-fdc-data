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

    OUTPUT_DIR = os.path.join(BASE_DIR, 'FoodData_Central_output')
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


 
    FF_FOOD_NUTRIENT = os.path.join(FOUNDATION_FOOD_DIR, 'food_nutrient.csv')
    FF_FOOD = os.path.join(FOUNDATION_FOOD_DIR, 'food.csv')
    FF_NUTRIENT = os.path.join(FOUNDATION_FOOD_DIR, 'nutrient.csv')
    FF_CATEGORY = os.path.join(FDC_ALL_DIR, 'food_category.csv')
    FF_PORTION = os.path.join(FOUNDATION_FOOD_DIR, 'food_portion.csv')
    FF_MEASURE_UNIT = os.path.join(FOUNDATION_FOOD_DIR, 'measure_unit.csv')


    LF_FOOD_NUTRIENT = os.path.join(SR_LEGACY_FOOD_DIR, 'food_nutrient.csv')
    LF_FOOD = os.path.join(SR_LEGACY_FOOD_DIR, 'food.csv')
    LF_NUTRIENT = os.path.join(SR_LEGACY_FOOD_DIR, 'nutrient.csv')
    LF_CATEGORY = os.path.join(SR_LEGACY_FOOD_DIR, 'food_category.csv')
    LF_PORTION = os.path.join(SR_LEGACY_FOOD_DIR, 'food_portion.csv')
    LF_MEASURE_UNIT = os.path.join(SR_LEGACY_FOOD_DIR, 'measure_unit.csv')


    BF_FOOD_NUTRIENT = os.path.join(BRANDED_FOOD_DIR, 'food_nutrient.csv')
    BF_FOOD = os.path.join(BRANDED_FOOD_DIR, 'food.csv')
    BF_NUTRIENT = os.path.join(BRANDED_FOOD_DIR, 'nutrient.csv')
    BF_CATEGORY = os.path.join(FDC_ALL_DIR, 'food_category.csv')
    BF_PORTION = os.path.join(FDC_ALL_DIR, 'food_portion.csv')


