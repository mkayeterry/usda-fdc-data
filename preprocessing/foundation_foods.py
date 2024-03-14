import pandas as pd
from preprocessing._utils import *

# Load datasets
ff_food_nutrient = pd.read_csv('/Users/mkayeterry/Desktop/fdc_data/foundation_food_indiv/food_nutrient.csv', low_memory=False)
ff_food = pd.read_csv('/Users/mkayeterry/Desktop/fdc_data/foundation_food_indiv/food.csv', low_memory=False)
ff_nutrient = pd.read_csv('/Users/mkayeterry/Desktop/fdc_data/foundation_food_indiv/nutrient.csv', low_memory=False)
ff_category = pd.read_csv('/Users/mkayeterry/Desktop/fdc_data/sr_legacy_food_indiv/food_category.csv', low_memory=False)
ff_portion = pd.read_csv('/Users/mkayeterry/Desktop/fdc_data/foundation_food_indiv/food_portion.csv', low_memory=False)

# Specify columns to keep for each dataframe
ff_food_nutrient_cols = ['id', 'fdc_id', 'nutrient_id', 'amount', 'derivation_id']
ff_food_cols = ['fdc_id', 'description', 'food_category_id']
ff_nutrient_cols = ['id', 'name', 'unit_name', 'nutrient_nbr']
ff_category_cols = ['id', 'description']
ff_portion_cols = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']

# Execute column selection
ff_food_nutrient = ff_food_nutrient[ff_food_nutrient_cols]
ff_food = ff_food[ff_food_cols]
ff_nutrient = ff_nutrient[ff_nutrient_cols]
ff_category = ff_category[ff_category_cols]
ff_portion = ff_portion[ff_portion_cols]

# Rename columns before merge
ff_food_nutrient.rename(columns={'id': 'food_nutrient_id', 'amount': 'nutrient_amount'}, inplace=True)
ff_food.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
ff_nutrient.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
ff_category.rename(columns={'id': 'category_id', 'description': 'category_description'}, inplace=True)
ff_portion.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'measure_unit_id': 'portion_unit', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)

# Merge datasets
ff_merged = pd.merge(ff_food_nutrient, ff_food, on='fdc_id', how='left')
ff_merged = pd.merge(ff_merged, ff_nutrient, on='nutrient_id', how='left')
ff_merged = pd.merge(ff_merged, ff_category, on='category_id', how='left')
ff_merged = pd.merge(ff_merged, ff_portion, on='fdc_id', how='left')

# Filter to only relevant columns
filtered_ff_merged = ff_merged[['fdc_id', 'food_description', 'category_description', 'nutrient_name', 'nutrient_amount', 'nutrient_unit', 'portion_amount', 'portion_unit', 'portion_modifier', 'portion_gram_weight']]

# List of nutrients consumers care about
relevant_nutrients = ['Protein', 'Energy', 'Fiber, total dietary', 'Iron, Fe', 'Sodium, Na', 'Cholesterol', 
                    'Fatty acids, total trans', 'Fatty acids, total saturated', 'Total lipid (fat)',
                    'Carbohydrate, by difference', 'Sugars, Total','Vitamin A, RAE', 'Vitamin C, total ascorbic acid', 
                    'Calcium, Ca', 'Retinol', 'Folate, total', 'Fatty acids, total monounsaturated', 'Fatty acids, total polyunsaturated', 
                    'Riboflavin', 'Vitamin B-12', 'Vitamin K (Dihydrophylloquinone)', 'Vitamin K (phylloquinone)', 
                    'Tryptophan', 'Threonine', 'Methionine', 'Phenylalanine', 'Carotene, beta', 'Thiamin', 
                    'Starch', 'Fructose', 'Lactose', 'Galactose', 'Magnesium, Mg', 'Phosphorus, P', 'Copper, Cu',
                    'Manganese, Mn', 'Tyrosine', 'Alanine', 'Glutamic acid', 'Glycine', 'Proline', 'Valine',
                    'Arginine', 'Histidine', 'Aspartic acid', 'Serine', 'Sucrose', 'Glucose', 'Maltose',
                    'Potassium, K', 'Zinc, Zn', 'Selenium, Se', 'Vitamin E (alpha-tocopherol)', 'Niacin', 'Pantothenic acid', 
                    'Vitamin B-6', 'Isoleucine', 'Leucine', 'Lysine', 'Cystine', 
                    'Choline, total', 'Betaine', 'Vitamin K (Menaquinone-4)', 
                    'Vitamin D3 (cholecalciferol)', 'Vitamin D2 (ergocalciferol)'
                ]

# Add condition to filter for rows with relevant_nutrients
filtered_ff_merged = filtered_ff_merged[filtered_ff_merged['nutrient_name'].isin(relevant_nutrients)]

# Alter nutrient_name for Energy to include units
filtered_ff_merged.loc[(filtered_ff_merged['nutrient_name'] == 'Energy') & (filtered_ff_merged['nutrient_unit'] == 'kJ'), 'nutrient_name'] = 'Energy kJ'
filtered_ff_merged.loc[(filtered_ff_merged['nutrient_name'] == 'Energy') & (filtered_ff_merged['nutrient_unit'] == 'KCAL'), 'nutrient_name'] = 'Energy KCAL'

filtered_ff_merged['multiplier'] = 0

filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'kJ', 'multiplier'] = round(1/100, 10)
filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)

# Pivot the table by unique combinations of 'fdc_id', 'food_description', 'food_category_description' on 'nutrient_name' and 'nuntrient_unit
ff_merged_pivot = pd.pivot_table(filtered_ff_merged,
                                 index=['fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_unit', 'portion_modifier', 'portion_gram_weight'],
                                 columns=['nutrient_name'],
                                 values='nutrient_amount').reset_index()

# Fill NaN nutrient values with zeros
for col in ff_merged_pivot.columns:
    if col in relevant_nutrients:
        ff_merged_pivot[col] = ff_merged_pivot[col].fillna(0)

# Convert the column names to a list
lst_col_names = ff_merged_pivot.columns.to_list()

# Format the column names using the format_names function
lst_col_names = format_names(lst_col_names)

# Assign the formatted column names back to the df
ff_merged_pivot.columns = lst_col_names

# Reorder columns & remove energy_kJ
ff_merged_pivot = ff_merged_pivot[[
                        'fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_unit', 'portion_modifier', 
                        'portion_gram_weight', 'energy_kcal', 'energy_kj', 'protein', 'total_lipid_fat', 'carbohydrate_by_difference', 
                        'fiber_total_dietary', 'sugars_total', 'cholesterol', 'sodium_na', 'potassium_k', 'calcium_ca', 
                        'iron_fe', 'magnesium_mg', 'phosphorus_p', 'copper_cu', 'manganese_mn', 'selenium_se', 'zinc_zn', 
                        'retinol',  'vitamin_a_rae', 'vitamin_b12', 'vitamin_b6', 
                        'vitamin_c_total_ascorbic_acid', 'vitamin_d2_ergocalciferol', 'vitamin_d3_cholecalciferol', 
                        'vitamin_e_alphatocopherol', 'vitamin_k_dihydrophylloquinone', 'vitamin_k_menaquinone4', 
                        'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'pantothenic_acid', 
                        'folate_total', 'alanine', 'arginine', 
                        'aspartic_acid', 'betaine', 'carotene_beta', 'choline_total', 'cystine', 'fatty_acids_total_monounsaturated', 
                        'fatty_acids_total_polyunsaturated', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fructose', 
                        'galactose', 'glucose', 'glutamic_acid', 'glycine', 'histidine',  'isoleucine', 'lactose', 'leucine', 
                        'lysine', 'maltose', 'methionine', 'phenylalanine', 'proline', 'serine', 'starch', 'sucrose',
                        'threonine', 'tryptophan', 'tyrosine', 'valine'
                    ]]


ff_merged_pivot.columns
ff_merged_pivot['fdc_id']
ff_merged_pivot['food_description']
ff_merged_pivot['category_description']
ff_merged_pivot['portion_amount']
ff_merged_pivot['portion_unit']
ff_merged_pivot['portion_modifier']
ff_merged_pivot['portion_gram_weight']
ff_merged_pivot['energy_kcal']
ff_merged_pivot['energy_kj']
ff_merged_pivot['protein']
ff_merged_pivot['total_lipid_fat']
ff_merged_pivot['carbohydrate_by_difference']
ff_merged_pivot['fiber_total_dietary']
ff_merged_pivot['sugars_total']
ff_merged_pivot['cholesterol']
ff_merged_pivot['sodium_na']

