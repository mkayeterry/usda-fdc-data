import pandas as pd
from preprocessing._utils import *

def process_branded(
        branded_food_path = None, 
        food_nutrient_path = None,
        food_path = None,
        nutrient_path = None
    ):

    # Load datasets
    branded_foods = pd.read_csv(branded_food_path, low_memory=False).head(10000)
    food_nutrients = pd.read_csv(food_nutrient_path, low_memory=False).head(10000)
    foods = pd.read_csv(food_path, low_memory=False).head(10000)
    nutrients = pd.read_csv(nutrient_path, low_memory=False).head(10000)

    # Specify columns to keep for each dataframe
    branded_food_cols = ['fdc_id', 'brand_owner', 'brand_name', 'ingredients', 'serving_size', 'serving_size_unit', 'household_serving_fulltext', 'branded_food_category']
    food_nutrient_cols = ['fdc_id', 'nutrient_id', 'amount']
    food_cols = ['fdc_id', 'description']
    nutrient_cols = ['id', 'name', 'unit_name']

    # Execute column selection
    branded_foods = branded_foods[branded_food_cols]
    food_nutrients = food_nutrients[food_nutrient_cols]
    foods = foods[food_cols]
    nutrients = nutrients[nutrient_cols]

    # Rename columns
    branded_foods.rename(columns={'serving_size': 'portion_amount', 'serving_size_unit': 'portion_unit', 'household_serving_fulltext': 'portion_modifier', 'branded_food_category': 'category'}, inplace=True)
    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)

    # Set data types for all columns, and fill NA values
    branded_foods['fdc_id'] = branded_foods['fdc_id'].fillna(0).astype(int)
    branded_foods['brand_owner'] = branded_foods['brand_owner'].fillna('no_value_given').astype(str)
    branded_foods['brand_name'] = branded_foods['brand_name'].fillna('no_value_given').astype(str)
    branded_foods['ingredients'] = branded_foods['ingredients'].fillna('no_value_given').astype(str)
    branded_foods['portion_amount'] = branded_foods['portion_amount'].fillna(0).astype(float)
    branded_foods['portion_unit'] = branded_foods['portion_unit'].fillna('no_value_given').astype(str)
    branded_foods['portion_modifier'] = branded_foods['portion_modifier'].fillna('no_value_given').astype(str)
    branded_foods['category'] = branded_foods['category'].fillna('no_value_given').astype(str)

    food_nutrients['fdc_id'] = food_nutrients['fdc_id'].fillna(0).astype(int)
    food_nutrients['nutrient_id'] = food_nutrients['nutrient_id'].fillna(0).astype(int)
    food_nutrients['nutrient_amount'] = food_nutrients['nutrient_amount'].fillna(0).astype(float)

    foods['fdc_id'] = foods['fdc_id'].fillna(0).astype(int)
    foods['food_description'] = foods['food_description'].fillna('no_value_given').astype(str)

    nutrients['nutrient_id'] = nutrients['nutrient_id'].fillna(0).astype(int)
    nutrients['nutrient_name'] = nutrients['nutrient_name'].fillna('no_value_given').astype(str)
    nutrients['nutrient_unit'] = nutrients['nutrient_unit'].fillna('no_value_given').astype(str)

    # Merge datasets    
    nutrients_merged = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients_merged = nutrients_merged.drop(['nutrient_id'], axis=1)

    foods_and_nutrients_merged = pd.merge(foods, nutrients_merged, on='fdc_id', how='left')

    full_foods_merged = pd.merge(foods_and_nutrients_merged, branded_foods, on='fdc_id', how='left')


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
    full_foods_filtered.drop(['nutrient_unit'], axis=1, inplace=True)
    full_foods_filtered.drop(['nutrient_amount'], axis=1, inplace=True)

    full_foods_grouped = full_foods_filtered.groupby(['food_description', 'nutrient_name', 'portion_amount', 'portion_unit']).first()

    full_foods_grouped['brand_name'] = full_foods_grouped['brand_name'].fillna('no_value_given')
    full_foods_grouped['portion_modifier'] = full_foods_grouped['portion_modifier'].fillna('no_value_given')

    full_foods_pivot = full_foods_grouped.pivot_table(
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

    # Add portion_energy column as calorie estimate
    full_foods_pivot['portion_energy'] = full_foods_pivot['Energy'] * full_foods_pivot['portion_amount']

    # Add source columns
    full_foods_pivot['usda_data_source'] = define_source(food_path)[0]
    full_foods_pivot['data_type'] = define_source(food_path)[1]

    # Add columns applying ingredient_slicer function
    full_foods_pivot['standardized_quantity'] = full_foods_pivot['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods_pivot['standardized_portion'] = full_foods_pivot['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])
    
    # Add missing columns to stack on other data type dfs
    full_foods_pivot['portion_gram_weight'] = 'no_value_given'

    # Format the column names using the format_col_names function
    lst_col_names = full_foods_pivot.columns.to_list()
    lst_col_names = format_col_names(lst_col_names)
    full_foods_pivot.columns = lst_col_names

    # Format the food_description, category, brand_name, and brand_owner values using the format_col_values function
    full_foods_pivot['food_description'] = full_foods_pivot['food_description'].apply(lambda x: format_col_values(x))
    full_foods_pivot['category'] = full_foods_pivot['category'].apply(lambda x: format_col_values(x))
    full_foods_pivot['brand_name'] = full_foods_pivot['brand_name'].apply(lambda x: format_col_values(x))
    full_foods_pivot['brand_owner'] = full_foods_pivot['brand_owner'].apply(lambda x: format_col_values(x))

    # Format ingredient values using the format_ingredients function
    full_foods_pivot['ingredients'] = full_foods_pivot['ingredients'].apply(lambda x: format_ingredients(x))

    full_foods_pivot = full_foods_pivot[[
                            'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 
                            'brand_name', 'food_description', 'ingredients', 'portion_amount', 
                            'portion_unit', 'portion_modifier', 'standardized_quantity', 'standardized_portion', 
                            'portion_gram_weight', 'portion_energy', 'energy', 'carbohydrate_by_difference', 
                            'protein', 'total_lipid_fat', 'fiber_total_dietary', 'protein', 'calcium_ca', 'iron_fe', 
                            'vitamin_c_total_ascorbic_acid',  'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 
                            'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 'fatty_acids_total_polyunsaturated', 
                            'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 'vitamin_b12',  'pantothenic_acid', 
                            'phosphorus_p', 'magnesium_mg', 'potassium_k', 'zinc_zn', 'manganese_mn', 'selenium_se'
                        ]]


    return full_foods_pivot