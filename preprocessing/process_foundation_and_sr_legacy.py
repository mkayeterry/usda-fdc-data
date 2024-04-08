import pandas as pd
import gc
from preprocessing._utils import *

def process_foundation_and_sr_legacy(
        food_nutrient_path = None,
        food_path = None,
        nutrient_path = None, 
        category_path = None, 
        portion_path = None, 
        measure_unit_path = None
    ):

    # Load datasets
    food_nutrients = pd.read_csv(food_nutrient_path, low_memory=False)
    foods = pd.read_csv(food_path, low_memory=False)
    nutrients = pd.read_csv(nutrient_path, low_memory=False)
    categories = pd.read_csv(category_path, low_memory=False)
    portions = pd.read_csv(portion_path, low_memory=False)
    measure_units = pd.read_csv(measure_unit_path, low_memory=False)

    # Drop unnecessary columns
    food_nutrients.drop(columns=food_nutrients.columns.difference(['fdc_id', 'nutrient_id', 'amount']), inplace=True)
    foods.drop(columns=foods.columns.difference(['fdc_id', 'description', 'food_category_id']), inplace=True)
    nutrients.drop(columns=nutrients.columns.difference(['id', 'name', 'unit_name']), inplace=True)
    categories.drop(columns=categories.columns.difference(['id', 'description']), inplace=True)
    portions.drop(columns=portions.columns.difference(['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']), inplace=True)
    measure_units.drop(columns=measure_units.columns.difference(['id', 'name']), inplace=True)

    # Rename columns
    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
    portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

    # Release memory
    del food_nutrient_path, nutrient_path, category_path, portion_path, measure_unit_path
    gc.collect()

    # Set data types for all columns, and fill NA values
    food_nutrients['fdc_id'].fillna(0, inplace=True)
    food_nutrients['nutrient_id'].fillna(0, inplace=True)
    food_nutrients['nutrient_amount'].fillna(0.0, inplace=True)

    foods['fdc_id'].fillna(0, inplace=True)
    foods['food_description'].fillna('no_value', inplace=True)
    foods['category_id'].fillna(0, inplace=True)

    nutrients['nutrient_id'].fillna(0, inplace=True)
    nutrients['nutrient_name'].fillna('no_value', inplace=True)
    nutrients['nutrient_unit'].fillna('no_value', inplace=True)

    categories['category_id'].fillna(0, inplace=True)
    categories['category'].fillna('no_value', inplace=True)

    portions['portion_id'].fillna(0, inplace=True)
    portions['fdc_id'].fillna(0, inplace=True)
    portions['portion_amount'].fillna(0.0, inplace=True)
    portions['measure_unit_id'].fillna(0, inplace=True)
    portions['portion_modifier'].fillna('no_value', inplace=True)
    portions['portion_gram_weight'].fillna(0.0, inplace=True)

    measure_units['measure_unit_id'].fillna(0, inplace=True)
    measure_units['portion_unit'].fillna('no_value', inplace=True)

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

    # Release memory
    gc.collect()

    # Aggregate rows with equal values
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

    # Release memory
    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_gram_weight']

    # Add source columns
    full_foods['usda_data_source'] = define_source(food_path)[0]
    full_foods['data_type'] = define_source(food_path)[1]

    # Add columns applying ingredient_slicer function
    full_foods['standardized_quantity'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods['standardized_portion'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])

    # # Add missing columns to stack on other data type dfs
    # full_foods['brand_owner'] = 'no_value'
    # full_foods['brand_name'] = 'no_value'
    # full_foods['ingredients'] = 'no_value'

    # Format the column names using the format_col_names function
    lst_col_names = full_foods.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the food_description and category values using the format_col_values function
    full_foods['food_description'] = full_foods['food_description'].apply(lambda x: format_col_values(x))
    full_foods['category'] = full_foods['category'].apply(lambda x: format_col_values(x))

    # full_foods = full_foods[[
    #                         'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 'brand_name', 'food_description', 'ingredients', 
    #                         'portion_amount', 'portion_unit', 'portion_modifier', 'standardized_quantity', 'standardized_portion', 'portion_gram_weight', 
    #                         'portion_energy', 'energy', 'carbohydrate_by_difference', 'protein', 'total_lipid_fat', 'fiber_total_dietary', 'sugars_total', 
    #                         'calcium_ca', 'iron_fe', 'vitamin_c_total_ascorbic_acid', 'vitamin_a_rae', 'vitamin_e_alphatocopherol', 
    #                         'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 
    #                         'fatty_acids_total_polyunsaturated', 'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 
    #                         'vitamin_b12', 'vitamin_d3_cholecalciferol', 'vitamin_d2_ergocalciferol', 'pantothenic_acid', 'phosphorus_p', 'magnesium_mg', 
    #                         'potassium_k', 'zinc_zn', 'copper_cu', 'manganese_mn', 'selenium_se', 'carotene_beta', 'retinol', 'vitamin_k_dihydrophylloquinone', 
    #                         'vitamin_k_menaquinone4', 'tryptophan', 'threonine', 'methionine', 'phenylalanine', 'tyrosine', 'valine', 'arginine', 'histidine', 
    #                         'isoleucine', 'leucine', 'lysine', 'cystine', 'alanine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'sucrose', 'glucose', 
    #                         'maltose', 'fructose', 'lactose', 'galactose', 'choline_total', 'betaine'
    #                     ]]


    # Release memory
    gc.collect()

    return full_foods


