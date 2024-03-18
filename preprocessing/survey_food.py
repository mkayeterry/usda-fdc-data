import pandas as pd
from preprocessing._utils import *

def process_survey_foods(data_paths):

    # Load datasets
    sf_food_nutrient = pd.read_csv(data_paths['sf_food_nutrient'], low_memory=False)
    sf_food = pd.read_csv(data_paths['sf_food'], low_memory=False)
    sf_nutrient = pd.read_csv(data_paths['sf_nutrient'], low_memory=False)
    sf_category = pd.read_csv(data_paths['sf_category'], low_memory=False)
    sf_portion = pd.read_csv(data_paths['sf_portion'], low_memory=False)
    sf_fndds_food = pd.read_csv(data_paths['sf_fndds_food'], low_memory=False)

    # Specify columns to keep for each dataframe
    sf_food_nutrient_cols = ['id', 'fdc_id', 'nutrient_id', 'amount']
    sf_food_cols = ['fdc_id', 'description']
    sf_nutrient_cols = ['id', 'name', 'unit_name', 'nutrient_nbr']
    sf_portion_cols = ['id', 'fdc_id', 'portion_description', 'modifier', 'gram_weight']
    sf_category_cols = ['wweia_food_category', 'wweia_food_category_description']
    sf_fndds_food_cols = ['fdc_id', 'wweia_category_number']

    # Execute column selection
    sf_food_nutrient = sf_food_nutrient[sf_food_nutrient_cols]
    sf_food = sf_food[sf_food_cols]
    sf_nutrient = sf_nutrient[sf_nutrient_cols]
    sf_portion = sf_portion[sf_portion_cols]
    sf_category = sf_category[sf_category_cols]
    sf_fndds_food = sf_fndds_food[sf_fndds_food_cols]

    # Rename columns before merge
    sf_food_nutrient.rename(columns={'id': 'food_nutrient_id', 'amount': 'nutrient_amount'}, inplace=True)
    sf_food.rename(columns={'description': 'food_description'}, inplace=True)
    sf_nutrient.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    sf_portion.rename(columns={'id': 'portion_id', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    sf_category.rename(columns={'wweia_food_category': 'category_id', 'wweia_food_category_description': 'category_description'}, inplace=True)
    sf_fndds_food.rename(columns={'wweia_category_number': 'category_id'}, inplace=True)

    # Merge datasets
    sf_merged = pd.merge(sf_food_nutrient, sf_food, on='fdc_id', how='left')
    sf_merged.iloc[10]
    sf_merged = pd.merge(sf_merged, sf_nutrient, on='nutrient_id', how='left')
    sf_merged.iloc[10]
    sf_merged = pd.merge(sf_merged, sf_portion, on='fdc_id', how='left')
    sf_merged.iloc[10]
    sf_category_merged = pd.merge(sf_fndds_food, sf_category, on='category_id', how='left')
    sf_category_merged.iloc[10]
    sf_merged = pd.merge(sf_merged, sf_category_merged, on='fdc_id', how='left')
    sf_merged.iloc[10]

    # Filter to only relevant columns
    filtered_sf_merged = sf_merged[['fdc_id', 'food_description', 'category_description', 'nutrient_name', 'nutrient_amount', 'nutrient_unit', 'portion_description', 'portion_modifier', 'portion_gram_weight']]

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
    filtered_sf_merged = filtered_sf_merged[filtered_sf_merged['nutrient_name'].isin(relevant_nutrients)]

    # Alter nutrient_name for Energy to include units
    filtered_sf_merged.loc[(filtered_sf_merged['nutrient_name'] == 'Energy') & (filtered_sf_merged['nutrient_unit'] == 'kJ'), 'nutrient_name'] = 'Energy kJ'
    filtered_sf_merged.loc[(filtered_sf_merged['nutrient_name'] == 'Energy') & (filtered_sf_merged['nutrient_unit'] == 'KCAL'), 'nutrient_name'] = 'Energy KCAL'

    filtered_sf_merged['multiplier'] = 0

    filtered_sf_merged.loc[filtered_sf_merged['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    filtered_sf_merged.loc[filtered_sf_merged['nutrient_unit'] == 'kJ', 'multiplier'] = round(1/100, 10)
    filtered_sf_merged.loc[filtered_sf_merged['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    filtered_sf_merged.loc[filtered_sf_merged['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    filtered_sf_merged.loc[filtered_sf_merged['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)

    filtered_sf_merged['per_g_amt'] = round(filtered_sf_merged.nutrient_amount * filtered_sf_merged.multiplier, 10)

    # Pivot the table by unique combinations of 'fdc_id', 'food_description', 'food_category_description' on 'nutrient_name' and 'nuntrient_unit
    sf_merged_pivot = pd.pivot_table(filtered_sf_merged,
                                    index=['fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_unit', 'portion_modifier', 'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_g_amt').reset_index()

    # Fill NaN nutrient values with zeros
    for col in lf_merged_pivot.columns:
        if col in relevant_nutrients:
            sf_merged_pivot[col] = sf_merged_pivot[col].fillna(0)

    # Convert the column names to a list
    lst_col_names = sf_merged_pivot.columns.to_list()

    # Format the column names using the format_names function
    lst_col_names = format_names(lst_col_names)

    # Assign the formatted column names back to the df
    sf_merged_pivot.columns = lst_col_names

    # Reorder columns & remove energy_kJ
    sf_merged_pivot = sf_merged_pivot[[
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


    return sf_merged_pivot
