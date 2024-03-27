import pandas as pd
from preprocessing._utils import *

def process_branded_food(data_paths):

    # TESTING
    branded_foods = pd.read_csv('fdc_data/FoodData_Central_branded_food_csv_2023-10-26/branded_food.csv', low_memory=False)
    food_nutrients = pd.read_csv('fdc_data/FoodData_Central_branded_food_csv_2023-10-26/food_nutrient.csv', low_memory=False)
    foods = pd.read_csv('fdc_data/FoodData_Central_branded_food_csv_2023-10-26/food.csv', low_memory=False)
    nutrients = pd.read_csv('fdc_data/FoodData_Central_branded_food_csv_2023-10-26/nutrient.csv', low_memory=False)

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
    branded_foods['brand_owner'] = branded_foods['brand_owner'].fillna('NA').astype(str)
    branded_foods['brand_name'] = branded_foods['brand_name'].fillna('NA').astype(str)
    branded_foods['ingredients'] = branded_foods['ingredients'].fillna('NA').astype(str)
    branded_foods['portion_amount'] = branded_foods['portion_amount'].fillna(0).astype(float)
    branded_foods['portion_unit'] = branded_foods['portion_unit'].fillna('NA').astype(str)
    branded_foods['portion_modifier'] = branded_foods['portion_modifier'].fillna('NA').astype(str)
    branded_foods['category'] = branded_foods['category'].fillna('NA').astype(str)

    food_nutrients['fdc_id'] = food_nutrients['fdc_id'].fillna(0).astype(int)
    food_nutrients['nutrient_id'] = food_nutrients['nutrient_id'].fillna(0).astype(int)
    food_nutrients['nutrient_amount'] = food_nutrients['nutrient_amount'].fillna(0).astype(float)

    foods['fdc_id'] = foods['fdc_id'].fillna(0).astype(int)
    foods['food_description'] = foods['food_description'].fillna('NA').astype(str)

    nutrients['nutrient_id'] = nutrients['nutrient_id'].fillna(0).astype(int)
    nutrients['nutrient_name'] = nutrients['nutrient_name'].fillna('NA').astype(str)
    nutrients['nutrient_unit'] = nutrients['nutrient_unit'].fillna('NA').astype(str)

    # branded_foods Columns:   ['fdc_id', 'brand_owner', 'brand_name', 'ingredients', 'portion_amount', 'portion_unit', 'portion_modifier', 'category']
    
    # foods Columns:           ['fdc_id', 'food_description']
    
    # food_nutrients Columns:  ['fdc_id', 'nutrient_id', 'nutrient_amount']
    # nutrients Columns:       ['nutrient_id', 'nutrient_name', 'nutrient_unit']
    
    nutrients_merged = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients_merged = nutrients_merged.drop(['nutrient_id'], axis=1)

    foods_and_nutrients_merged = pd.merge(foods, nutrients_merged, on='fdc_id', how='left')

    full_foods_merged = pd.merge(foods_and_nutrients_merged, branded_foods, on='fdc_id', how='left')

    # List of nutrients consumers care about
    relevant_nutrients = ['Energy', 'Protein', 'Carbohydrate, by difference', 'Total lipid (fat)']
                        # 'Iron, Fe', 'Sodium, Na', 'Cholesterol', 'Fatty acids, total trans', 'Fatty acids, total saturated', 
                        # 'Fiber, total dietary', 'Sugars, Total','Vitamin A, RAE', 'Vitamin C, total ascorbic acid', 
                        # 'Calcium, Ca', 'Retinol', 'Folate, total', 'Fatty acids, total monounsaturated', 'Fatty acids, total polyunsaturated', 
                        # 'Riboflavin', 'Vitamin B-12', 'Vitamin K (Dihydrophylloquinone)', 'Vitamin K (phylloquinone)', 
                        # 'Tryptophan', 'Threonine', 'Methionine', 'Phenylalanine', 'Carotene, beta', 'Thiamin', 
                        # 'Starch', 'Fructose', 'Lactose', 'Galactose', 'Magnesium, Mg', 'Phosphorus, P', 'Copper, Cu',
                        # 'Manganese, Mn', 'Tyrosine', 'Alanine', 'Glutamic acid', 'Glycine', 'Proline', 'Valine',
                        # 'Arginine', 'Histidine', 'Aspartic acid', 'Serine', 'Sucrose', 'Glucose', 'Maltose',
                        # 'Potassium, K', 'Zinc, Zn', 'Selenium, Se', 'Vitamin E (alpha-tocopherol)', 'Niacin', 'Pantothenic acid', 
                        # 'Vitamin B-6', 'Isoleucine', 'Leucine', 'Lysine', 'Cystine', 
                        # 'Choline, total', 'Betaine', 'Vitamin K (Menaquinone-4)', 
                        # 'Vitamin D3 (cholecalciferol)', 'Vitamin D2 (ergocalciferol)'

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

# full_foods_filtered.head(10000).to_csv(os.path.join(Config.OUTPUT_DIR, 'full_foods_filtered.csv'))

    # Aggregate rows with equal values
    full_foods_agg = full_foods_filtered.groupby(['food_description', 'category', 'nutrient_name', 'nutrient_unit', 'portion_modifier', 
                 'portion_unit', 'brand_owner', 'brand_name', 'ingredients', 'category']).mean(numeric_only=True).reset_index()

    full_foods_pivot = pd.pivot_table(full_foods_agg,
                                    index=['fdc_id', 'food_description', 'category', 'nutrient_amount', 'nutrient_unit', 'brand_owner', 
                                        'brand_name', 'ingredients', 'portion_amount', 'portion_unit', 'portion_modifier', 'category'],
                                    columns=['nutrient_name'],
                                    values='per_gram_amt').reset_index()

    # Add calorie estimate
    full_foods_pivot['portion_energy'] = full_foods_pivot['Energy'] * full_foods_pivot['portion_gram_weight']

    # Add source columns
    full_foods_pivot['usda_data_source'] = define_source(food_path)[0]
    full_foods_pivot['data_type'] = define_source(food_path)[1]

    # Format the column names using the format_names function
    lst_col_names = full_foods_pivot.columns.to_list()
    lst_col_names = format_names(lst_col_names)
    full_foods_pivot.columns = lst_col_names

    full_foods_pivot['ext_portion_amount'] = 'NA'

    if (full_foods_pivot['portion_unit'] == 'NA').all():
        # Apply the ingredient_slicer function to the portion_modifier column
        slicer_result = full_foods_pivot['portion_modifier'].apply(lambda x: apply_ingredient_slicer(x))

        # Filter rows where 'portion_unit' is 'NA'
        na_rows = full_foods_pivot['portion_unit'] == 'NA'

        # Update 'ext_portion_amount' and 'portion_unit' columns for the filtered rows
        full_foods_pivot.loc[na_rows, ['ext_portion_amount', 'portion_unit']] = slicer_result.tolist()

    full_foods_pivot = full_foods_pivot[[
                            'fdc_id', 'usda_data_source', 'data_type', 'food_description', 'category', 'portion_amount', 'ext_portion_amount', 
                            'portion_unit', 'portion_modifier', 'portion_gram_weight', 'portion_energy', 'energy', 
                            'carbohydrate_by_difference', 'protein', 'total_lipid_fat'
                        ]]

    return full_foods_pivot