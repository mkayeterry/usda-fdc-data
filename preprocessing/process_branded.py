import pandas as pd
import gc
from preprocessing._utils import *

def process_branded(
        branded_food_path = None, 
        food_nutrient_path = None,
        food_path = None,
        nutrient_path = None
    ):

    branded_foods = pd.read_csv(branded_food_path, low_memory=False, nrows=10000)
    food_nutrients = pd.read_csv(food_nutrient_path, low_memory=False, nrows=10000)
    foods = pd.read_csv(food_path, low_memory=False, nrows=10000)
    nutrients = pd.read_csv(nutrient_path, low_memory=False, nrows=10000)

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

    # Release memory
    del branded_food_path, food_nutrient_path, nutrient_path
    gc.collect()

    # Set data types for all columns, and fill NA values using fill_na_and_define_dtype function
    df_lst = [branded_foods, food_nutrients, foods, nutrients]

    for df in df_lst:
        for col in df.columns.tolist():
            fill_na_and_define_dtype(df, col)

    # Join datasets  
    full_foods = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    full_foods.drop(['nutrient_id'], axis=1, inplace=True)
    
    full_foods = pd.merge(foods, full_foods, on='fdc_id', how='left')
    full_foods = pd.merge(full_foods, branded_foods, on='fdc_id', how='left')

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


    full_foods = full_foods.groupby(['food_description', 'nutrient_name', 'portion_amount', 'portion_unit']).first()

    full_foods['brand_name'].fillna('no_value', inplace=True)
    full_foods['portion_modifier'].fillna('no_value', inplace=True)

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


    # Release memory
    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_amount']

    # Add source columns
    full_foods['usda_data_source'] = define_source(food_path)[0]
    full_foods['data_type'] = define_source(food_path)[1]

    # Add columns applying ingredient_slicer function
    full_foods['standardized_quantity'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    full_foods['standardized_portion'] = full_foods['portion_modifier'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])

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

    # Release memory
    gc.collect()

    return full_foods

