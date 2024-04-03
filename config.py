import os

BASE_DIR = os.path.abspath(os.path.dirname(__name__))

class Config:

    BASE_DIR = 'fdc_data'

    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)   


    OUTPUT_DIR = os.path.join(BASE_DIR, 'FoodData_Central_output')



    if not os.path.exists(OUTPUT_DIR):
         os.makedirs(OUTPUT_DIR)

    for path in os.listdir(BASE_DIR):

        if 'foundation' in path:
            FOUNDATION_FOOD_DIR = os.path.join(BASE_DIR, path)

        if 'sr_legacy' in path:
            SR_LEGACY_FOOD_DIR = os.path.join(BASE_DIR, path)

        if 'branded' in path:
            BRANDED_FOOD_DIR = os.path.join(BASE_DIR, path)

        if 'FoodData_Central_csv' in path:
            FDC_ALL_DIR = os.path.join(BASE_DIR, path)


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
