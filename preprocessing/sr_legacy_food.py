import pandas as pd
from preprocessing._utils import *

def process_legacy_foods(data_paths):

    # Load datasets
    lf_food_nutrient = pd.read_csv(data_paths['lf_food_nutrient'], low_memory=False)
    lf_food = pd.read_csv(data_paths['lf_food'], low_memory=False)
    lf_nutrient = pd.read_csv(data_paths['lf_nutrient'], low_memory=False)
    lf_category = pd.read_csv(data_paths['lf_category'], low_memory=False)
    lf_portion = pd.read_csv(data_paths['lf_portion'], low_memory=False)

    # Specify columns to keep
    lf_food_nutrient_cols = ['id', 'fdc_id', 'nutrient_id', 'amount', 'derivation_id']
    lf_food_cols = ['fdc_id', 'description', 'food_category_id']
    lf_nutrient_cols = ['id', 'name', 'unit_name', 'nutrient_nbr']
    lf_category_cols = ['id', 'description']
    lf_portion_cols = ['id', 'fdc_id', 'amount', 'modifier', 'gram_weight']

    # Execute column selection
    lf_food_nutrient = lf_food_nutrient[lf_food_nutrient_cols]
    lf_food = lf_food[lf_food_cols]
    lf_nutrient = lf_nutrient[lf_nutrient_cols]
    lf_category = lf_category[lf_category_cols]
    lf_portion = lf_portion[lf_portion_cols]

    # Rename columns before merge
    lf_food_nutrient.rename(columns={'id': 'food_nutrient_id', 'amount': 'nutrient_amount'}, inplace=True)
    lf_food.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    lf_nutrient.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    lf_category.rename(columns={'id': 'category_id', 'description': 'category_description'}, inplace=True)
    lf_portion.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)

    # Merge datasets
    lf_merged = pd.merge(lf_food_nutrient, lf_food, on='fdc_id', how='left')
    lf_merged = pd.merge(lf_merged, lf_nutrient, on='nutrient_id', how='left')
    lf_merged = pd.merge(lf_merged, lf_category, on='category_id', how='left')
    lf_merged = pd.merge(lf_merged, lf_portion, on='fdc_id', how='left')

    # Filter to only relevant columns
    filtered_lf_merged = lf_merged[['fdc_id', 'food_description', 'category_description', 'nutrient_name', 'nutrient_amount', 'nutrient_unit', 'portion_amount', 'portion_modifier', 'portion_gram_weight']]

    # List of nutrients consumers care about
    relevant_nutrients = ['Protein', 'Energy', 'Fiber, total dietary', 'Iron, Fe', 'Sodium, Na', 'Cholesterol', 
                        'Fatty acids, total trans', 'Fatty acids, total saturated', 'Total lipid (fat)',
                        'Carbohydrate, by difference', 'Sugars, Total','Vitamin A, RAE', 'Vitamin C, total ascorbic acid', 
                        'Calcium, Ca', 'Retinol', 'Folate, total', 'Fatty acids, total monounsaturated', 'Fatty acids, total polyunsaturated', 
                        'Folate, DFE', 'Riboflavin', 'Vitamin B-12', 'Vitamin K (Dihydrophylloquinone)', 'Vitamin K (phylloquinone)', 
                        'Folate, food', 'Tryptophan', 'Threonine', 'Methionine', 'Phenylalanine', 'Carotene, beta', 'Thiamin', 
                        'Starch', 'Fructose', 'Lactose', 'Alcohol, ethyl', 'Galactose', 'Magnesium, Mg', 'Phosphorus, P', 'Copper, Cu',
                        'Manganese, Mn', 'Tyrosine', 'Alanine', 'Glutamic acid', 'Glycine', 'Proline', 'Vitamin B-12, added', 'Valine',
                        'Arginine', 'Histidine', 'Aspartic acid', 'Serine', 'Vitamin E, added', 'Sucrose', 'Glucose', 'Maltose', 'Caffeine', 
                        'Theobromine', 'Potassium, K', 'Zinc, Zn', 'Selenium, Se', 'Vitamin E (alpha-tocopherol)', 'Niacin', 'Pantothenic acid', 
                        'Vitamin B-6', 'Folic acid', 'Isoleucine', 'Leucine', 'Lysine', 'Cystine', 
                        'Choline, total', 'Betaine', 'Vitamin K (Menaquinone-4)', 'Fluoride, F',
                        'Vitamin D3 (cholecalciferol)', 'Vitamin D2 (ergocalciferol)'
                    ]

    # Add condition to filter for rows with relevant_nutrients
    filtered_lf_merged = filtered_lf_merged[filtered_lf_merged['nutrient_name'].isin(relevant_nutrients)]

    # Alter nutrient_name for Energy to include units
    filtered_lf_merged.loc[(filtered_lf_merged['nutrient_name'] == 'Energy') & (filtered_lf_merged['nutrient_unit'] == 'kJ'), 'nutrient_name'] = 'Energy kJ'
    filtered_lf_merged.loc[(filtered_lf_merged['nutrient_name'] == 'Energy') & (filtered_lf_merged['nutrient_unit'] == 'KCAL'), 'nutrient_name'] = 'Energy KCAL'

    filtered_lf_merged['multiplier'] = 0

    filtered_lf_merged.loc[filtered_lf_merged['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    filtered_lf_merged.loc[filtered_lf_merged['nutrient_unit'] == 'kJ', 'multiplier'] = round(1/100, 10)
    filtered_lf_merged.loc[filtered_lf_merged['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    filtered_lf_merged.loc[filtered_lf_merged['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    filtered_lf_merged.loc[filtered_lf_merged['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)

    filtered_lf_merged['per_g_amt'] = round(filtered_lf_merged.nutrient_amount * filtered_lf_merged.multiplier, 10)

    # Pivot the table by unique combinations of 'fdc_id', 'food_description', 'food_category_description' on 'nutrient_name' and 'nuntrient_unit
    lf_merged_pivot = pd.pivot_table(filtered_lf_merged,
                                    index=['fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_modifier', 'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_g_amt').reset_index()

    # Fill NaN nutrient values with zeros
    for col in lf_merged_pivot.columns:
        if col in relevant_nutrients:
            lf_merged_pivot[col] = lf_merged_pivot[col].fillna(0)

    # Convert the column names to a list
    lst_col_names = lf_merged_pivot.columns.to_list()

    # Format the column names using the format_names function
    lst_col_names = format_names(lst_col_names)

    # Assign the formatted column names back to the df
    lf_merged_pivot.columns = lst_col_names

    # Reorder columns & remove energy_kJ
    lf_merged_pivot = lf_merged_pivot[[
                            'fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_modifier', 
                            'portion_gram_weight', 'energy_kcal', 'energy_kj', 'protein', 'total_lipid_fat', 'carbohydrate_by_difference', 
                            'fiber_total_dietary', 'sugars_total', 'cholesterol', 'sodium_na', 'potassium_k', 'calcium_ca', 
                            'iron_fe', 'magnesium_mg', 'phosphorus_p', 'copper_cu', 'manganese_mn', 'selenium_se', 'zinc_zn', 
                            'fluoride_f', 'retinol',  'vitamin_a_rae', 'vitamin_b12', 'vitamin_b12_added', 'vitamin_b6', 
                            'vitamin_c_total_ascorbic_acid', 'vitamin_d2_ergocalciferol', 'vitamin_d3_cholecalciferol', 
                            'vitamin_e_alphatocopherol', 'vitamin_e_added', 'vitamin_k_dihydrophylloquinone', 'vitamin_k_menaquinone4', 
                            'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'pantothenic_acid', 'vitamin_b12_added', 
                            'folate_dfe', 'folate_food', 'folate_total', 'folic_acid', 'alanine', 'alcohol_ethyl', 'arginine', 
                            'aspartic_acid', 'betaine', 'caffeine', 'carotene_beta', 'choline_total', 'cystine', 'fatty_acids_total_monounsaturated', 
                            'fatty_acids_total_polyunsaturated', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fructose', 
                            'galactose', 'glucose', 'glutamic_acid', 'glycine', 'histidine',  'isoleucine', 'lactose', 'leucine', 
                            'lysine', 'maltose', 'methionine', 'phenylalanine', 'proline', 'serine', 'starch', 'sucrose', 'theobromine', 
                            'threonine', 'tryptophan', 'tyrosine', 'valine'
                        ]]

    return lf_merged_pivot
