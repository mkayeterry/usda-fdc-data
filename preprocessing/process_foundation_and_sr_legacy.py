import pandas as pd
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

    # Specify columns to keep for each dataframe
    food_nutrient_cols = ['fdc_id', 'nutrient_id', 'amount']
    food_cols = ['fdc_id', 'description', 'food_category_id']
    nutrient_cols = ['id', 'name', 'unit_name']
    category_cols = ['id', 'description']
    portion_cols = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']
    measure_unit_cols = ['id', 'name']

    # Execute column selection
    food_nutrients = food_nutrients[food_nutrient_cols]
    foods = foods[food_cols]
    nutrients = nutrients[nutrient_cols]
    categories = categories[category_cols]
    portions = portions[portion_cols]
    measure_units = measure_units[measure_unit_cols]

    # Rename columns
    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
    portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

    # Set data types for all columns, and fill NA values
    food_nutrients['fdc_id'] = food_nutrients['fdc_id'].fillna(0).astype(int)
    food_nutrients['nutrient_id'] = food_nutrients['nutrient_id'].fillna(0).astype(int)
    food_nutrients['nutrient_amount'] = food_nutrients['nutrient_amount'].fillna(0).astype(float)

    foods['fdc_id'] = foods['fdc_id'].fillna(0).astype(int)
    foods['food_description'] = foods['food_description'].fillna('no_value_given').astype(str)
    foods['category_id'] = foods['category_id'].fillna(0).astype(int)

    nutrients['nutrient_id'] = nutrients['nutrient_id'].fillna(0).astype(int)
    nutrients['nutrient_name'] = nutrients['nutrient_name'].fillna('no_value_given').astype(str)
    nutrients['nutrient_unit'] = nutrients['nutrient_unit'].fillna('no_value_given').astype(str)

    categories['category_id'] = categories['category_id'].fillna(0).astype(int)
    categories['category'] = categories['category'].fillna('no_value_given').astype(str)

    portions['portion_id'] = portions['portion_id'].fillna(0).astype(int)
    portions['fdc_id'] = portions['fdc_id'].fillna(0).astype(int)
    portions['portion_amount'] = portions['portion_amount'].fillna(0).astype(float)
    portions['measure_unit_id'] = portions['measure_unit_id'].fillna(0).astype(int)
    portions['portion_modifier'] = portions['portion_modifier'].fillna('no_value_given').astype(str)
    portions['portion_gram_weight'] = portions['portion_gram_weight'].fillna(0).astype(float)

    measure_units['measure_unit_id'] = measure_units['measure_unit_id'].fillna(0).astype(int)
    measure_units['portion_unit'] = measure_units['portion_unit'].fillna('no_value_given').astype(str)

    foods_merged = pd.merge(foods, categories, on='category_id', how='left')
    foods_merged = foods_merged.drop(['category_id'], axis=1)

    nutrients_merged = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients_merged = nutrients_merged.drop(['nutrient_id'], axis=1)

    # If True, portion_units are non-applicable
    if (portions['measure_unit_id'] == 9999).all():
        portions['portion_unit'] = 'no_value_given'
        portions_merged = portions
    else:
        portions_merged = pd.merge(portions, measure_units, on='measure_unit_id', how='left')

    portions_merged = portions_merged.drop(['measure_unit_id'], axis=1)
    portions_merged = portions_merged.drop(['portion_id'], axis=1)

    foods_and_nutrients_merged = pd.merge(foods_merged, nutrients_merged, on='fdc_id', how='left')

    full_foods_merged = pd.merge(foods_and_nutrients_merged, portions_merged, on='fdc_id', how='inner')

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
    full_foods_filtered = full_foods_merged[full_foods_merged['nutrient_name'].isin(relevant_nutrients)]
    full_foods_filtered = full_foods_filtered[full_foods_filtered['nutrient_unit'] != 'kJ']

    # Add new column for per gram amount for various nutrients
    full_foods_filtered['multiplier'] = 0
    full_foods_filtered.loc[full_foods_filtered['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    full_foods_filtered.loc[full_foods_filtered['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    full_foods_filtered.loc[full_foods_filtered['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    full_foods_filtered.loc[full_foods_filtered['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)
    full_foods_filtered['per_gram_amt'] = round(full_foods_filtered.nutrient_amount * full_foods_filtered.multiplier, 10)
    full_foods_filtered.drop(['multiplier'], axis=1, inplace=True)

    # Aggregate rows with equal values
    full_foods_agg = full_foods_filtered.groupby(['food_description', 'category', 'nutrient_name', 'nutrient_unit', 'portion_modifier', 'portion_unit']).mean(numeric_only=True).reset_index()

    full_foods_pivot = pd.pivot_table(full_foods_agg,
                                    index=['fdc_id', 
                                        'food_description', 
                                        'category', 
                                        'portion_amount', 
                                        'portion_unit', 
                                        'portion_modifier', 
                                        'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_gram_amt').reset_index()

    # Add portion_energy column as calorie estimate
    full_foods_pivot['portion_energy'] = full_foods_pivot['Energy'] * full_foods_pivot['portion_gram_weight']

    # Add source columns
    full_foods_pivot['usda_data_source'] = define_source(food_path)[0]
    full_foods_pivot['data_type'] = define_source(food_path)[1]

    # Add columns applying ingredient_slicer function
    full_foods_pivot['standardized_quantity'] = full_foods_pivot['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods_pivot['standardized_portion'] = full_foods_pivot['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])
    
    # Add missing columns to stack on other data type dfs
    full_foods_pivot['brand_owner'] = 'no_value_given'
    full_foods_pivot['brand_name'] = 'no_value_given'
    full_foods_pivot['ingredients'] = 'no_value_given'

    # Format the column names using the format_col_names function
    lst_col_names = full_foods_pivot.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods_pivot.columns = lst_col_names

    # Format the food_description and category values using the format_col_values function
    full_foods_pivot['food_description'] = full_foods_pivot['food_description'].apply(lambda x: format_col_values(x))
    full_foods_pivot['category'] = full_foods_pivot['category'].apply(lambda x: format_col_values(x))

    full_foods_pivot = full_foods_pivot[[
                            'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 'brand_name', 'food_description', 'ingredients', 
                            'portion_amount', 'portion_unit', 'portion_modifier', 'standardized_quantity', 'standardized_portion', 'portion_gram_weight', 
                            'portion_energy', 'energy', 'carbohydrate_by_difference', 'protein', 'total_lipid_fat', 'fiber_total_dietary', 'sugars_total', 
                            'protein', 'calcium_ca', 'iron_fe', 'vitamin_c_total_ascorbic_acid', 'vitamin_a_rae', 'vitamin_e_alphatocopherol', 
                            'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 
                            'fatty_acids_total_polyunsaturated', 'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 
                            'vitamin_b12', 'vitamin_d3_cholecalciferol', 'vitamin_d2_ergocalciferol', 'pantothenic_acid', 'phosphorus_p', 'magnesium_mg', 
                            'potassium_k', 'zinc_zn', 'copper_cu', 'manganese_mn', 'selenium_se', 'carotene_beta', 'retinol', 'vitamin_k_dihydrophylloquinone', 
                            'vitamin_k_menaquinone4', 'tryptophan', 'threonine', 'methionine', 'phenylalanine', 'tyrosine', 'valine', 'arginine', 'histidine', 
                            'isoleucine', 'leucine', 'lysine', 'cystine', 'alanine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'sucrose', 'glucose', 
                            'maltose', 'fructose', 'lactose', 'galactose', 'choline_total', 'betaine'
                        ]]


    return full_foods_pivot


