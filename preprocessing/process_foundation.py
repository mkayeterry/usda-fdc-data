import os
import pandas as pd
import gc
from preprocessing._utils import *

def process_foundation(
        urls = None, 
        base_dir = None, 
        raw_dir = None, 
        keep_files = False, 
    ):

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)

    for url in urls:
        download_usda_csv(url, raw_dir)

    # Find the directory containing the downloaded files to define foundation_dir
    for path in os.listdir(raw_dir):
        if 'foundation' in path:
            foundation_dir = os.path.join(raw_dir, path)
            source = define_source(foundation_dir)[0]
        if 'FoodData_Central_csv' in path:
            all_dir = os.path.join(raw_dir, path)

    # Delete unnecessary files if keep_files flag is not specified
    if not keep_files:
        files_to_keep = ['food_nutrient.csv', 'food.csv', 'nutrient.csv', 'food_portion.csv', 'measure_unit.csv']
        delete_unnecessary_files(foundation_dir, files_to_keep)

        files_to_keep = ['food_category.csv']
        delete_unnecessary_files(all_dir, files_to_keep)

    print(f'Initializing processing for:\n> {source}\n')

    # Load datasets

    food_nutrients = pd.read_csv(os.path.join(foundation_dir, 'food_nutrient.csv'), 
                                usecols    = ['fdc_id', 'nutrient_id', 'amount'], 
                                dtype      = {'fdc_id': 'int32', 'nutrient_id': 'int32', 'amount': 'float32'}, 
                                low_memory = False)

    foods = pd.read_csv(os.path.join(foundation_dir, 'food.csv'),
                                usecols    = ['fdc_id', 'description', 'food_category_id'], 
                                dtype      = {'fdc_id': 'int32', 'description': 'str', 'food_category_id': 'float32'}, 
                                low_memory = False)

    nutrients = pd.read_csv(os.path.join(foundation_dir, 'nutrient.csv'),
                                usecols    = ['id', 'name', 'unit_name'], 
                                dtype      = {'id': 'int32', 'name': 'str', 'unit_name': 'str'}, 
                                low_memory = False)

    categories = pd.read_csv(os.path.join(all_dir, 'food_category.csv'),
                                usecols    = ['id', 'description'], 
                                dtype      = {'id': 'int32', 'description': 'str'}, 
                                low_memory = False)

    portions = pd.read_csv(os.path.join(foundation_dir, 'food_portion.csv'),
                                usecols    = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight'], 
                                dtype      = {'id': 'int32', 'fdc_id': 'int32', 'amount': 'float32', 'measure_unit_id': 'int32', 'modifier': 'str', 'gram_weight': 'float32'}, 
                                low_memory = False)

    measure_units = pd.read_csv(os.path.join(foundation_dir, 'measure_unit.csv'),
                                usecols    = ['id', 'name'], 
                                dtype      = {'id': 'int32', 'name': 'str'}, 
                                low_memory=False)

    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
    portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

    gc.collect()

    # Set data types for all columns, and fill NA values using fillna_and_define_dtype function
    for df in [food_nutrients, foods, nutrients, categories, portions, measure_units]:
        for col in df.columns.tolist():
            fillna_and_define_dtype(df, col)

    # Join datasets
    foods = pd.merge(foods, categories, on='category_id', how='left')
    foods.drop(['category_id'], axis=1, inplace=True)

    nutrients = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients.drop(['nutrient_id'], axis=1, inplace=True)

    # If True, portion_units are non-applicable
    if (portions['measure_unit_id'] == 9999).all():
        portions['portion_unit'] = 'no_value'
    else:
        portions = pd.merge(portions, measure_units, on='measure_unit_id', how='left')

    portions.drop(['measure_unit_id'], axis=1, inplace=True)
    portions.drop(['portion_id'], axis=1, inplace=True)

    full_foods = pd.merge(foods, nutrients, on='fdc_id', how='left')
    full_foods = pd.merge(full_foods, portions, on='fdc_id', how='inner')

    gc.collect()

    # Filter for rows with relevant_nutrients using filter_relevent_nutrients function
    filter_relevent_nutrients(full_foods)

    # Add new column for per gram amount using add_per_gram_amt function
    add_per_gram_amt(full_foods)

    gc.collect()

    # Aggregate rows with equal values and pivot data
    full_foods = full_foods.groupby(['food_description', 'category', 'nutrient_name', 'nutrient_unit', 'portion_modifier', 'portion_unit']).mean(numeric_only=True).reset_index()

    full_foods = pd.pivot_table(full_foods,
                                    index=['fdc_id', 
                                        'food_description', 
                                        'category', 
                                        'portion_amount', 
                                        'portion_unit', 
                                        'portion_modifier', 
                                        'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_gram_amt').reset_index()

    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_gram_weight']

    # Add source columns
    full_foods['usda_data_source'] = define_source(foundation_dir)[0]
    full_foods['data_type'] = define_source(foundation_dir)[1]

    # # Add columns applying ingredient_slicer function
    # full_foods['standardized_quantity'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    # full_foods['standardized_portion'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])

    # Format the column names using the format_col_names function
    lst_col_names = full_foods.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the food_description and category values using the format_col_values function
    for col in ['food_description', 'category']:
        full_foods[col] = full_foods[col].str.replace(',', '').replace('(', '').replace(')', '').str.lower()


    # Save intermediary dataframe
    full_foods.to_parquet(os.path.join(base_dir, f'processed_foundation.parquet'))

    # Delete remaining files if keep_files flag is not specified
    if not keep_files:
        import shutil

        for root, dirs, files in os.walk(all_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        try:
            os.removedirs(all_dir)
        except OSError:
            shutil.rmtree(all_dir)

        for root, dirs, files in os.walk(foundation_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        try:
            os.removedirs(foundation_dir)
        except OSError:
            shutil.rmtree(foundation_dir)
