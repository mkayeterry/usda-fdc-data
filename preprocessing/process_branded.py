import pandas as pd
import gc
import timeit
from preprocessing._utils import *

def process_branded(
        branded_food_path = None, 
        food_nutrient_path = None,
        food_path = None,
        nutrient_path = None
    ):

    print(f'\n> loading files...')
    start = timeit.default_timer()
    branded_foods = pd.read_csv(branded_food_path, low_memory=False)
    food_nutrients = pd.read_csv(food_nutrient_path, low_memory=False)
    foods = pd.read_csv(food_path, low_memory=False)
    nutrients = pd.read_csv(nutrient_path, low_memory=False)
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    # Drop unnecessary columns
    branded_foods.drop(columns=branded_foods.columns.difference(['fdc_id', 'brand_owner', 'brand_name', 'ingredients', 'serving_size', 'serving_size_unit', 'household_serving_fulltext', 'branded_food_category']), inplace=True)
    food_nutrients.drop(columns=food_nutrients.columns.difference(['fdc_id', 'nutrient_id', 'amount']), inplace=True)
    foods.drop(columns=foods.columns.difference(['fdc_id', 'description']), inplace=True)
    nutrients.drop(columns=nutrients.columns.difference(['id', 'name', 'unit_name']), inplace=True)

    # Perform inplace renaming of columns
    branded_foods.rename(columns={'serving_size': 'portion_amount', 'serving_size_unit': 'portion_unit', 'household_serving_fulltext': 'portion_modifier', 'branded_food_category': 'category'}, inplace=True)
    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)

    print(f'> garbage collection...\n')
    # Release memory
    del branded_food_path, food_nutrient_path, nutrient_path
    gc.collect()

    # Set data types for all columns, and fill NA values
    branded_foods['fdc_id'].fillna(0, inplace=True)
    branded_foods['brand_owner'].fillna('no_value', inplace=True)
    branded_foods['brand_name'].fillna('no_value', inplace=True)
    branded_foods['ingredients'].fillna('no_value', inplace=True)
    branded_foods['portion_amount'].fillna(0.0, inplace=True)
    branded_foods['portion_unit'].fillna('no_value', inplace=True)
    branded_foods['portion_modifier'].fillna('no_value', inplace=True)
    branded_foods['category'].fillna('no_value', inplace=True)

    food_nutrients['fdc_id'].fillna(0, inplace=True)
    food_nutrients['nutrient_id'].fillna(0, inplace=True)
    food_nutrients['nutrient_amount'].fillna(0.0, inplace=True)

    foods['fdc_id'].fillna(0, inplace=True)
    foods['food_description'].fillna('no_value', inplace=True)

    nutrients['nutrient_id'].fillna(0, inplace=True)
    nutrients['nutrient_name'].fillna('no_value', inplace=True)
    nutrients['nutrient_unit'].fillna('no_value', inplace=True)

    # Merge datasets 
    print('> merging datasets...') 
    start = timeit.default_timer()  
    nutrients_merged = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients_merged.drop(['nutrient_id'], axis=1, inplace=True)

    foods_and_nutrients_merged = pd.merge(foods, nutrients_merged, on='fdc_id', how='left')

    full_foods = pd.merge(foods_and_nutrients_merged, branded_foods, on='fdc_id', how='left')
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    print(f'> garbage collection...\n')
    # Release memory
    gc.collect()

    # List of nutrients consumers care about
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

    # Add condition to filter for rows with relevant_nutrients
    full_foods = full_foods[full_foods['nutrient_name'].isin(relevant_nutrients)]
    full_foods = full_foods[full_foods['nutrient_unit'] != 'kJ']

    # Add new column for per gram amount for various nutrients
    full_foods['multiplier'] = 0
    full_foods.loc[full_foods['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    full_foods.loc[full_foods['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)
    full_foods['per_gram_amt'] = round(full_foods.nutrient_amount * full_foods.multiplier, 10)
    
    full_foods.drop(['multiplier'], axis=1, inplace=True)
    full_foods.drop(['nutrient_unit'], axis=1, inplace=True)
    full_foods.drop(['nutrient_amount'], axis=1, inplace=True)

    # Release memory
    gc.collect()

    print('> grouping data...')
    start = timeit.default_timer()
    full_foods = full_foods.groupby(['food_description', 'nutrient_name', 'portion_amount', 'portion_unit']).first()
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    full_foods['brand_name'].fillna('no_value', inplace=True)
    full_foods['portion_modifier'].fillna('no_value', inplace=True)

    print('> pivoting table...')
    start = timeit.default_timer()
    full_foods = full_foods.pivot_table(
                                index=['fdc_id', 
                                    'food_description', 
                                    'category', 
                                    'brand_owner', 
                                    'brand_name', 
                                    'ingredients', 
                                    'portion_amount', 
                                    'portion_unit',
                                    'portion_modifier'],
                                columns='nutrient_name', 
                                values='per_gram_amt').reset_index()
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    print(f'> garbage collection...\n')
    # Release memory
    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_amount']

    # Add source columns
    full_foods['usda_data_source'] = define_source(food_path)[0]
    full_foods['data_type'] = define_source(food_path)[1]

    print('> applying ingredient-slicer...')
    start = timeit.default_timer()
    # Add columns applying ingredient_slicer function
    full_foods['standardized_quantity'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods['standardized_portion'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    # # Add missing columns to stack on other data type dfs
    # cols_to_add = ['portion_gram_weight', 'fructose', 'serine', 'glucose', 'copper_cu', 'vitamin_a_rae', 'sugars_total', 'galactose', 'betaine', 'threonine', 'glycine', 'vitamin_d3_cholecalciferol', 'cystine', 'leucine', 'vitamin_k_menaquinone4', 'valine', 'choline_total', 'lysine', 'vitamin_k_dihydrophylloquinone', 'tyrosine', 'vitamin_e_alphatocopherol', 'proline', 'tryptophan', 'phenylalanine', 'vitamin_k_phylloquinone', 'methionine', 'isoleucine', 'maltose', 'histidine', 'alanine', 'vitamin_d2_ergocalciferol', 'carotene_beta', 'glutamic_acid', 'arginine', 'retinol', 'sucrose', 'lactose']
    # for i in cols_to_add:
    #     full_foods[i] = 0.0

    print('> formatting column names and values...')
    start = timeit.default_timer()
    # Format the column names using the format_col_names function
    lst_col_names = full_foods.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the food_description, category, brand_name, and brand_owner values using the format_col_values function
    full_foods['food_description'] = full_foods['food_description'].apply(lambda x: format_col_values(x))
    full_foods['category'] = full_foods['category'].apply(lambda x: format_col_values(x))
    full_foods['brand_name'] = full_foods['brand_name'].apply(lambda x: format_col_values(x))
    full_foods['brand_owner'] = full_foods['brand_owner'].apply(lambda x: format_col_values(x))

    # Format ingredient values using the format_ingredients function
    full_foods['ingredients'] = full_foods['ingredients'].apply(lambda x: format_ingredients(x))
    stop = timeit.default_timer()
    print(f'> processing time: {stop - start}\n')

    # full_foods = full_foods[[
    #                         'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 
    #                         'brand_name', 'food_description', 'ingredients', 'portion_amount', 
    #                         'portion_unit', 'portion_modifier', 'standardized_quantity', 'standardized_portion', 
    #                         'portion_gram_weight', 'portion_energy', 'energy', 'carbohydrate_by_difference', 
    #                         'protein', 'total_lipid_fat', 'fiber_total_dietary', 'calcium_ca', 'iron_fe', 
    #                         'vitamin_c_total_ascorbic_acid',  'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 
    #                         'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 'fatty_acids_total_polyunsaturated', 
    #                         'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 'vitamin_b12', 'pantothenic_acid', 
    #                         'phosphorus_p', 'magnesium_mg', 'potassium_k', 'zinc_zn', 'manganese_mn', 'selenium_se'
    #                     ]]

    # Release memory
    gc.collect()

    return full_foods

