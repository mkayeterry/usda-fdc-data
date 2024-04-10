import os
import pandas as pd
import gc
from preprocessing._utils import *

def process_srlegacy(
        url = None, 
        base_dir = None, 
        raw_dir = None, 
        delete_files = True, 
    ):

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)

    download_usda_csv(url, raw_dir)

    # Find the directory containing the downloaded files to define srlegacy_dir
    for path in os.listdir(raw_dir):
        if 'sr_legacy' in path:
            srlegacy_dir = os.path.join(raw_dir, path)

    if delete_files == 'true':
        files_to_keep = ['food_nutrient.csv', 'food.csv', 'nutrient.csv', 'food_category.csv', 'food_portion.csv', 'measure_unit.csv']
        delete_unnecessary_files(srlegacy_dir, files_to_keep)

    print(f'Initializing processing for:\n> {os.path.basename(url)[:-4]}\n')

    # Load datasets
    food_nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'food_nutrient.csv'), low_memory=False)
    foods = pd.read_csv(os.path.join(srlegacy_dir, 'food.csv'), low_memory=False)
    nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'nutrient.csv') , low_memory=False)
    categories = pd.read_csv(os.path.join(srlegacy_dir, 'food_category.csv'), low_memory=False)
    portions = pd.read_csv(os.path.join(srlegacy_dir, 'food_portion.csv'), low_memory=False)
    measure_units = pd.read_csv(os.path.join(srlegacy_dir, 'measure_unit.csv'), low_memory=False)

    # Drop unnecessary columns and rename
    food_nutrients.drop(columns=food_nutrients.columns.difference(['fdc_id', 'nutrient_id', 'amount']), inplace=True)
    foods.drop(columns=foods.columns.difference(['fdc_id', 'description', 'food_category_id']), inplace=True)
    nutrients.drop(columns=nutrients.columns.difference(['id', 'name', 'unit_name']), inplace=True)
    categories.drop(columns=categories.columns.difference(['id', 'description']), inplace=True)
    portions.drop(columns=portions.columns.difference(['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']), inplace=True)
    measure_units.drop(columns=measure_units.columns.difference(['id', 'name']), inplace=True)

    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
    portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

    gc.collect()

    # Set data types for all columns, and fill NA values using fillna_and_define_dtype function
    df_lst = [food_nutrients, foods, nutrients, categories, portions, measure_units]

    for df in df_lst:
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

    # Filter for rows with relevant_nutrients that consumers care about
    relevant_nutrients = ['Energy', 'Protein', 'Carbohydrate, by difference', 'Total lipid (fat)', 
                        'Iron, Fe', 'Sodium, Na', 'Cholesterol', 'Fatty acids, total trans', 'Fatty acids, total saturated', 
                        'Fiber, total dietary', 'Sugars, Total','Vitamin A, RAE', 'Vitamin C, total ascorbic acid', 
                        'Calcium, Ca', 'Retinol', 'Folate, total', 'Fatty acids, total monounsaturated', 'Fatty acids, total polyunsaturated', 
                        'Riboflavin', 'Vitamin B-12', 'Vitamin K (Dihydrophylloquinone)', 'Vitamin K (phylloquinone)', 
                        'Tryptophan', 'Threonine', 'Methionine', 'Phenylalanine', 'Carotene, beta', 'Thiamin', 
                        'Starch', 'Fructose', 'Lactose', 'Galactose', 'Magnesium, Mg', 'Phosphorus, P', 'Copper, Cu',
                        'Manganese, Mn', 'Tyrosine', 'Alanine', 'Glutamic acid', 'Glycine', 'Proline', 'Valine',
                        'Arginine', 'Histidine', 'Aspartic acid', 'Serine', 'Sucrose', 'Glucose', 'Maltose',
                        'Potassium, K', 'Zinc, Zn', 'Selenium, Se', 'Vitamin E (alpha-tocopherol)', 'Niacin', 'Pantothenic acid', 
                        'Vitamin B-6', 'Isoleucine', 'Leucine', 'Lysine', 'Cystine', 
                        'Choline, total', 'Betaine', 'Vitamin K (Menaquinone-4)', 
                        'Vitamin D3 (cholecalciferol)', 'Vitamin D2 (ergocalciferol)']

    full_foods = full_foods[full_foods['nutrient_name'].isin(relevant_nutrients)]
    full_foods = full_foods[full_foods['nutrient_unit'] != 'kJ'] # Drop energy when in kJ (only kcal needed)

    # Add new column for per gram amount for various nutrients
    full_foods['multiplier'] = 0
    full_foods.loc[full_foods['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)
    full_foods['per_gram_amt'] = round(full_foods.nutrient_amount * full_foods.multiplier, 10)
    full_foods.drop(['multiplier'], axis=1, inplace=True)

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
    full_foods['usda_data_source'] = define_source(srlegacy_dir)[0]
    full_foods['data_type'] = define_source(srlegacy_dir)[1]

    # Add columns applying ingredient_slicer function
    full_foods['standardized_quantity'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods['standardized_portion'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])

    # Format the column names using the format_col_names function
    lst_col_names = full_foods.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the food_description and category values using the format_col_values function
    full_foods['food_description'] = full_foods['food_description'].apply(lambda x: format_col_values(x))
    full_foods['category'] = full_foods['category'].apply(lambda x: format_col_values(x))

    # Save intermediary dataframe
    full_foods.to_parquet(os.path.join(base_dir, f'processed_srlegacy.parquet'))

    # Delete raw downloads if delete_files flag is set to True
    if delete_files == 'true':
        import shutil

        for root, dirs, files in os.walk(srlegacy_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        try:
            os.removedirs(srlegacy_dir)
        except OSError:
            shutil.rmtree(srlegacy_dir)




