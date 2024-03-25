import pandas as pd
from preprocessing._utils import *

def process_foundation_food(data_paths):

    # Load datasets
    ff_food_nutrient = pd.read_csv(data_paths['ff_food_nutrient'], low_memory=False)
    ff_food = pd.read_csv(data_paths['ff_food'], low_memory=False)
    ff_nutrient = pd.read_csv(data_paths['ff_nutrient'], low_memory=False)
    ff_category = pd.read_csv(data_paths['ff_category'], low_memory=False)
    ff_portion = pd.read_csv(data_paths['ff_portion'], low_memory=False)
    ff_measure_unit = pd.read_csv(data_paths['ff_measure_unit'], low_memory=False)

    # Specify columns to keep for each dataframe
    ff_food_nutrient_cols = ['id', 'fdc_id', 'nutrient_id', 'amount', 'derivation_id']
    ff_food_cols = ['fdc_id', 'description', 'food_category_id']
    ff_nutrient_cols = ['id', 'name', 'unit_name', 'nutrient_nbr']
    ff_category_cols = ['id', 'description']
    ff_portion_cols = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']
    ff_measure_unit_cols = ['id', 'name']

    # Execute column selection
    ff_food_nutrient = ff_food_nutrient[ff_food_nutrient_cols]
    ff_food = ff_food[ff_food_cols]
    ff_nutrient = ff_nutrient[ff_nutrient_cols]
    ff_category = ff_category[ff_category_cols]
    ff_portion = ff_portion[ff_portion_cols]
    ff_measure_unit = ff_measure_unit[ff_measure_unit_cols]

    # Rename columns before merge
    ff_food_nutrient.rename(columns={'id': 'food_nutrient_id', 'amount': 'nutrient_amount'}, inplace=True)
    ff_food.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    ff_nutrient.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    ff_category.rename(columns={'id': 'category_id', 'description': 'category_description'}, inplace=True)
    ff_portion.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    ff_measure_unit.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

    # Set data types for all columns, and fill NA values
    ff_food_nutrient['food_nutrient_id'] = ff_food_nutrient['food_nutrient_id'].fillna(0).astype(int)
    ff_food_nutrient['fdc_id'] = ff_food_nutrient['fdc_id'].fillna(0).astype(int)
    ff_food_nutrient['nutrient_id'] = ff_food_nutrient['nutrient_id'].fillna(0).astype(int)
    ff_food_nutrient['nutrient_amount'] = ff_food_nutrient['nutrient_amount'].fillna(0).astype(float)
    ff_food_nutrient['derivation_id'] = ff_food_nutrient['derivation_id'].fillna(0).astype(int)

    ff_food['fdc_id'] = ff_food['fdc_id'].fillna(0).astype(int)
    ff_food['food_description'] = ff_food['food_description'].fillna('NA').astype(str)
    ff_food['category_id'] = ff_food['category_id'].fillna(0).astype(int)

    ff_nutrient['nutrient_id'] = ff_nutrient['nutrient_id'].fillna(0).astype(int)
    ff_nutrient['nutrient_name'] = ff_nutrient['nutrient_name'].fillna('NA').astype(str)
    ff_nutrient['nutrient_unit'] = ff_nutrient['nutrient_unit'].fillna('NA').astype(str)
    ff_nutrient['nutrient_nbr'] = ff_nutrient['nutrient_nbr'].fillna(0).astype(int)

    ff_category['category_id'] = ff_category['category_id'].fillna(0).astype(int)
    ff_category['category_description'] = ff_category['category_description'].fillna('NA').astype(str)

    ff_portion['portion_id'] = ff_portion['portion_id'].fillna(0).astype(int)
    ff_portion['fdc_id'] = ff_portion['fdc_id'].fillna(0).astype(int)
    ff_portion['portion_amount'] = ff_portion['portion_amount'].fillna(0).astype(float)
    ff_portion['measure_unit_id'] = ff_portion['measure_unit_id'].fillna(0).astype(int)
    ff_portion['portion_modifier'] = ff_portion['portion_modifier'].fillna('NA').astype(str)
    ff_portion['portion_gram_weight'] = ff_portion['portion_gram_weight'].fillna(0).astype(float)

    ff_measure_unit['measure_unit_id'] = ff_measure_unit['measure_unit_id'].fillna(0).astype(int)
    ff_measure_unit['portion_unit'] = ff_measure_unit['portion_unit'].fillna('NA').astype(str)

    # Merge datasets
    ff_merged = pd.merge(ff_food, ff_food_nutrient, on='fdc_id', how='left')
    ff_merged = pd.merge(ff_merged, ff_nutrient, on='nutrient_id', how='left')
    ff_merged = pd.merge(ff_merged, ff_category, on='category_id', how='left')
    ff_merged = pd.merge(ff_merged, ff_portion, on='fdc_id', how='left')
    ff_merged = pd.merge(ff_merged, ff_measure_unit, on='measure_unit_id', how='left')

    # Filter to only relevant columns
    filtered_ff_merged = ff_merged[['fdc_id', 'food_description', 'category_description', 'nutrient_name', 'nutrient_amount', 'nutrient_unit', 'portion_amount', 'portion_unit', 'portion_modifier', 'portion_gram_weight']]
# filtered_ff_merged.to_csv(os.path.join(Config.PROCESSED_DIR, 'pre_hummus_filter.csv'))
    # TODO hummus filtering
    filtered_ff_merged = ff_merged[ff_merged['food_description'].str.contains('hummus', case=False)]
# filtered_ff_merged.to_csv(os.path.join(Config.PROCESSED_DIR, 'post_hummus_filter.csv'))

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
# filtered_ff_merged.to_csv(os.path.join(Config.PROCESSED_DIR, 'pre_rel_nut_filter.csv'))
    # Add condition to filter for rows with relevant_nutrients
    filtered_ff_merged = filtered_ff_merged[filtered_ff_merged['nutrient_name'].isin(relevant_nutrients) | filtered_ff_merged['nutrient_name'].str.contains('NA') | filtered_ff_merged['nutrient_name'].isna()]
# filtered_ff_merged.to_csv(os.path.join(Config.PROCESSED_DIR, 'post_rel_nut_filter.csv'))
    # Alter nutrient_name for Energy to include units
    filtered_ff_merged.loc[(filtered_ff_merged['nutrient_name'] == 'Energy') & (filtered_ff_merged['nutrient_unit'] == 'kJ'), 'nutrient_name'] = 'Energy kJ'
    filtered_ff_merged.loc[(filtered_ff_merged['nutrient_name'] == 'Energy') & (filtered_ff_merged['nutrient_unit'] == 'KCAL'), 'nutrient_name'] = 'Energy KCAL'

    filtered_ff_merged['multiplier'] = 0

    filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'kJ', 'multiplier'] = round(1/100, 10)
    filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    filtered_ff_merged.loc[filtered_ff_merged['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)

    filtered_ff_merged['per_g_amt'] = round(filtered_ff_merged.nutrient_amount * filtered_ff_merged.multiplier, 10)
# filtered_ff_merged.to_csv(os.path.join(Config.PROCESSED_DIR, 'pre_pivot.csv'))
    # Pivot the table by unique combinations of 'fdc_id', 'food_description', 'food_category_description' on 'nutrient_name' and 'nuntrient_unit
    ff_merged_pivot = pd.pivot_table(filtered_ff_merged,
                                    index=['fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_unit', 'portion_modifier', 'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_g_amt').reset_index()

    # Fill NaN nutrient values with zeros
    for col in ff_merged_pivot.columns:
        if col in relevant_nutrients:
            ff_merged_pivot[col] = ff_merged_pivot[col].fillna(0)
# ff_merged_pivot.to_csv(os.path.join(Config.PROCESSED_DIR, 'post_pivot.csv'))
    # Convert the column names to a list
    lst_col_names = ff_merged_pivot.columns.to_list()

    # Format the column names using the format_names function
    lst_col_names = format_names(lst_col_names)

    # Assign the formatted column names back to the df
    ff_merged_pivot.columns = lst_col_names

    # Reorder columns & remove energy_kJ #TODO excluded for hummus filtering
    # ff_merged_pivot = ff_merged_pivot[[
    #                         'fdc_id', 'food_description', 'category_description', 'portion_amount', 'portion_unit', 'portion_modifier', 
    #                         'portion_gram_weight', 'energy_kcal', 'energy_kj', 'protein', 'total_lipid_fat', 'carbohydrate_by_difference', 
    #                         'fiber_total_dietary', 'sugars_total', 'cholesterol', 'sodium_na', 'potassium_k', 'calcium_ca', 
    #                         'iron_fe', 'magnesium_mg', 'phosphorus_p', 'copper_cu', 'manganese_mn', 'selenium_se', 'zinc_zn', 
    #                         'retinol',  'vitamin_a_rae', 'vitamin_b12', 'vitamin_b6', 
    #                         'vitamin_c_total_ascorbic_acid', 'vitamin_d2_ergocalciferol', 'vitamin_d3_cholecalciferol', 
    #                         'vitamin_e_alphatocopherol', 'vitamin_k_dihydrophylloquinone', 'vitamin_k_menaquinone4', 
    #                         'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'pantothenic_acid', 
    #                         'folate_total', 'alanine', 'arginine', 
    #                         'aspartic_acid', 'betaine', 'carotene_beta', 'choline_total', 'cystine', 'fatty_acids_total_monounsaturated', 
    #                         'fatty_acids_total_polyunsaturated', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fructose', 
    #                         'galactose', 'glucose', 'glutamic_acid', 'glycine', 'histidine',  'isoleucine', 'lactose', 'leucine', 
    #                         'lysine', 'maltose', 'methionine', 'phenylalanine', 'proline', 'serine', 'starch', 'sucrose',
    #                         'threonine', 'tryptophan', 'tyrosine', 'valine'
    #                     ]]

# ff_merged_pivot.to_csv(os.path.join(Config.PROCESSED_DIR, 'post_formatting.csv'))
    return ff_merged_pivot

