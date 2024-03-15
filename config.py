import os
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.dirname(__name__))
load_dotenv(os.path.join(BASE_DIR, '.env'))

class Config:

    # Base directory and path configurations
    BASE_DIR_NAME = 'fdc_data'
    BASE_DIR_PATH = os.getenv('BASE_DIR_PATH')

    # Define paths for raw and output directories
    BASE_DIR = os.path.join(BASE_DIR_PATH, BASE_DIR_NAME)
    FOUNDATION_FOOD_DIR = os.path.join(BASE_DIR, 'foundation_food_indiv')
    SR_LEGACY_FOOD_DIR = os.path.join(BASE_DIR, 'sr_legacy_food_indiv')
    SURVEY_FOOD_DIR = os.path.join(BASE_DIR, 'survey_food_indiv')
    OUTPUT_DIR = os.path.join(BASE_DIR, 'fdc_output')
    PROCESSED_DIR = os.path.join(OUTPUT_DIR, 'indiv_processed')

    # Create a list of directories to check and create
    directories = [BASE_DIR, FOUNDATION_FOOD_DIR, SR_LEGACY_FOOD_DIR, SURVEY_FOOD_DIR, OUTPUT_DIR, PROCESSED_DIR]

    # Loop through the list of directories and create them if they don't exist
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)

            # Print directories, if created
            print(f"Directory created: {directory}")


    SR_LEGACY_FOOD_PATHS = {
                "lf_food_nutrient": os.path.join(SR_LEGACY_FOOD_DIR, 'food_nutrient.csv'), 
                "lf_food": os.path.join(SR_LEGACY_FOOD_DIR, 'food.csv'), 
                "lf_nutrient": os.path.join(SR_LEGACY_FOOD_DIR, 'nutrient.csv'), 
                "lf_category": os.path.join(SR_LEGACY_FOOD_DIR, 'food_category.csv'), 
                "lf_portion": os.path.join(SR_LEGACY_FOOD_DIR, 'food_portion.csv')
                }


    FOUNDATION_FOOD_PATHS = {
                "lf_food_nutrient": os.path.join(FOUNDATION_FOOD_DIR, 'food_nutrient.csv'), 
                "lf_food": os.path.join(FOUNDATION_FOOD_DIR, 'food.csv'), 
                "lf_nutrient": os.path.join(FOUNDATION_FOOD_DIR, 'nutrient.csv'), 
                "lf_category": os.path.join(SR_LEGACY_FOOD_DIR, 'food_category.csv'), 
                "lf_portion": os.path.join(FOUNDATION_FOOD_DIR, 'food_portion.csv')
                }


