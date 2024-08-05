
import pandas as pd
import gc
from preprocessing._utils import *


def process_branded(
    url=None,
    output_dir=None,
    raw_dir=None,
    keep_files=False,
):

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)

    download_usda_csv(url, raw_dir)

    # Find the directory containing the downloaded files to define branded_dir
    for path in os.listdir(raw_dir):
        if 'branded' in path:
            print(f'Found branded data in: {path}\n')
            branded_dir = os.path.join(raw_dir, path)
            source = define_source(branded_dir)[0]

    # Delete unnecessary files if keep_files flag is not specified
    if not keep_files:
        files_to_keep = ['branded_food.csv',
                         'food_nutrient.csv', 'food.csv', 'nutrient.csv']
        delete_unnecessary_files(branded_dir, files_to_keep)

    print(f'Initializing processing for:\n> {source}\n')

    branded_foods = pd.read_csv(os.path.join(branded_dir, 'branded_food.csv'),
                                usecols=['fdc_id', 'brand_owner', 'brand_name', 'ingredients', 'serving_size',
                                         'serving_size_unit', 'household_serving_fulltext', 'branded_food_category'],
                                dtype={'fdc_id': 'int32', 'brand_owner': 'str', 'brand_name': 'str', 'ingredients': 'str',
                                       'serving_size': 'float32', 'serving_size_unit': 'str', 'household_serving_fulltext': 'str',
                                       'branded_food_category': 'str'},
                                low_memory=False)

    food_nutrients = pd.read_csv(os.path.join(branded_dir, 'food_nutrient.csv'),
                                 usecols=['fdc_id', 'nutrient_id', 'amount'],
                                 dtype={
                                     'fdc_id': 'int32', 'nutrient_id': 'int16', 'amount': 'float32'},
                                 low_memory=False)

    foods = pd.read_csv(os.path.join(branded_dir, 'food.csv'),
                        usecols=['fdc_id', 'description'],
                        dtype={'fdc_id': 'int32', 'description': 'str'},
                        low_memory=False)

    nutrients = pd.read_csv(os.path.join(branded_dir, 'nutrient.csv'),
                            usecols=['id', 'name', 'unit_name'],
                            dtype={'id': 'int16', 'name': 'str',
                                   'unit_name': 'str'},
                            low_memory=False)

    branded_foods.rename(columns={'serving_size': 'portion_amount',
                                  'serving_size_unit': 'portion_unit',
                                  'household_serving_fulltext': 'portion_modifier',
                                  'branded_food_category': 'category'}, inplace=True)

    # Rename columns to be consistent across datasets
    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name',
                     'unit_name': 'nutrient_unit'}, inplace=True)

    gc.collect()

    # Set data types for all columns, and fill NA values using fillna_and_set_dtypes function
    branded_foods = fillna_and_set_dtypes(branded_foods)
    food_nutrients = fillna_and_set_dtypes(food_nutrients)
    foods = fillna_and_set_dtypes(foods)

    # Join datasets
    full_foods = pd.merge(food_nutrients, nutrients,
                          on='nutrient_id', how='left')
    full_foods.drop(['nutrient_id'], axis=1, inplace=True)

    full_foods = pd.merge(foods, full_foods, on='fdc_id', how='left')
    full_foods = pd.merge(full_foods, branded_foods, on='fdc_id', how='left')

    gc.collect()

    # Filter for rows with relevant_nutrients using filter_relevent_nutrients function
    filter_relevent_nutrients(full_foods)

    # Add new column for per gram amount using add_per_gram_amt function
    add_per_gram_amt(full_foods)
    full_foods.drop(['nutrient_unit'], axis=1, inplace=True)
    full_foods.drop(['nutrient_amount'], axis=1, inplace=True)

    gc.collect()

    # Stash the first row of each group to join back with the pivoted nutrition data later
    stashed_food_info = full_foods.groupby(
        ['fdc_id', 'food_description', 'category']).first().reset_index()

    stashed_food_info['brand_name'].fillna('no_value', inplace=True)
    stashed_food_info['portion_modifier'].fillna('no_value', inplace=True)

    full_foods = full_foods.pivot_table(
        index=['fdc_id'],
        columns='nutrient_name',
        values='per_gram_amt').reset_index()

    full_foods = pd.merge(stashed_food_info, full_foods,
                          on='fdc_id', how='left')

    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * \
        full_foods['portion_amount']

    # Add source columns
    full_foods['usda_data_source'] = define_source(branded_dir)[0]
    full_foods['data_type'] = define_source(branded_dir)[1]

    # Add columns applying ingredient_slicer function
    full_foods['portion_combined'] = full_foods.loc[:, 'portion_amount'].astype(
        str) + ' ' + full_foods.loc[:, 'portion_unit'] + ' ' + full_foods.loc[:, 'portion_modifier']

    # Extract ingredients from portion_combined column
    full_foods['parsed_ingredient'] = full_foods['portion_combined'].apply(
        lambda x: ingredient_slicer.IngredientSlicer(x).parsed_ingredient())
    full_foods['std_portion_amount'] = full_foods['parsed_ingredient'].apply(
        lambda x: x["quantity"] if x["quantity"] else "no_value")
    full_foods['std_portion_unit'] = full_foods['parsed_ingredient'].apply(
        lambda x: x["standardized_unit"] if x["standardized_unit"] else "no_value")
    full_foods.drop(['portion_combined', 'parsed_ingredient'],
                    axis=1, inplace=True)

    gc.collect()

    # Format the column names using the format_col_names function
    lst_col_names = full_foods.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the category, brand_name, and brand_owner values
    for col in ['category', 'brand_name', 'brand_owner']:
        full_foods[col] = full_foods[col].str.replace(
            ',', '').replace('(', '').replace(')', '').str.lower()

    # Format the food_description values
    for col in ['food_description']:
        full_foods[col] = full_foods[col].str.replace(
            '(', '').replace(')', '').str.lower()

    # Format ingredient values using the format_ingredients function
    full_foods['ingredients'] = full_foods['ingredients'].apply(
        lambda x: format_ingredients(x))

    # Save intermediary dataframe
    full_foods.to_parquet(os.path.join(
        output_dir, f'processed_branded.parquet'))

    # Delete ramining files if keep_files flag is not specified
    if not keep_files:
        import shutil

        for root, dirs, files in os.walk(branded_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        try:
            os.removedirs(branded_dir)
        except OSError:
            shutil.rmtree(branded_dir)
