import pandas as pd
from preprocessing._utils import *

# --------------------------------------------------------------------------
# Load data, filter columns, fill NA values, and speciefy column data types.
# --------------------------------------------------------------------------

# Load datasets
food_nutrient = pd.read_csv(data_paths['ff_food_nutrient'], low_memory=False)
food = pd.read_csv(data_paths['ff_food'], low_memory=False)
nutrient = pd.read_csv(data_paths['ff_nutrient'], low_memory=False)
category = pd.read_csv(data_paths['ff_category'], low_memory=False)
portion = pd.read_csv(data_paths['ff_portion'], low_memory=False)
measure_unit = pd.read_csv(data_paths['ff_measure_unit'], low_memory=False)

# Specify columns to keep for each dataframe
food_nutrient_cols = ['fdc_id', 'nutrient_id', 'amount']
food_cols = ['fdc_id', 'description', 'food_category_id']
nutrient_cols = ['id', 'name', 'unit_name']
category_cols = ['id', 'description']
portion_cols = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight']
measure_unit_cols = ['id', 'name']

# Execute column selection
food_nutrient = food_nutrient[food_nutrient_cols]
food = food[food_cols]
nutrient = nutrient[nutrient_cols]
category = category[category_cols]
portion = portion[portion_cols]
measure_unit = measure_unit[measure_unit_cols]

# Rename columns before merge
food_nutrient.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
food.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
nutrient.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
category.rename(columns={'id': 'category_id', 'description': 'category_description'}, inplace=True)
portion.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
measure_unit.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

# Set data types for all columns, and fill NA values
food_nutrient['fdc_id'] = food_nutrient['fdc_id'].fillna(0).astype(int)
food_nutrient['nutrient_id'] = food_nutrient['nutrient_id'].fillna(0).astype(int)
food_nutrient['nutrient_amount'] = food_nutrient['nutrient_amount'].fillna(0).astype(float)

food['fdc_id'] = food['fdc_id'].fillna(0).astype(int)
food['food_description'] = food['food_description'].fillna('NA').astype(str)
food['category_id'] = food['category_id'].fillna(0).astype(int)

nutrient['nutrient_id'] = nutrient['nutrient_id'].fillna(0).astype(int)
nutrient['nutrient_name'] = nutrient['nutrient_name'].fillna('NA').astype(str)
nutrient['nutrient_unit'] = nutrient['nutrient_unit'].fillna('NA').astype(str)

category['category_id'] = category['category_id'].fillna(0).astype(int)
category['category_description'] = category['category_description'].fillna('NA').astype(str)

portion['portion_id'] = portion['portion_id'].fillna(0).astype(int)
portion['fdc_id'] = portion['fdc_id'].fillna(0).astype(int)
portion['portion_amount'] = portion['portion_amount'].fillna(0).astype(float)
portion['measure_unit_id'] = portion['measure_unit_id'].fillna(0).astype(int)
portion['portion_modifier'] = portion['portion_modifier'].fillna('NA').astype(str)
portion['portion_gram_weight'] = portion['portion_gram_weight'].fillna(0).astype(float)

measure_unit['measure_unit_id'] = measure_unit['measure_unit_id'].fillna(0).astype(int)
measure_unit['portion_unit'] = measure_unit['portion_unit'].fillna('NA').astype(str)

# ----------------------------------------------------------------
# Merge data to 3 main datasets, each containing an fdc_id column.
# ----------------------------------------------------------------

# food Columns:           ['fdc_id', 'category_id', 'food_description']
# category Columns:       ['category_id', 'category_description']

# food_nutrient Columns:  ['fdc_id', 'nutrient_id', 'nutrient_amount']
# nutrient Columns:       ['nutrient_id', 'nutrient_name', 'nutrient_unit']

# portion Columns:        ['fdc_id', 'measure_unit_id', 'portion_id', 'portion_amount', 'portion_modifier', 'portion_gram_weight']
# measure_unit Columns:   ['measure_unit_id', 'portion_unit']

foods_merged = pd.merge(food, category, on='category_id', how='left')
foods_merged = foods_merged.drop(['category_id'], axis=1)

nutrients_merged = pd.merge(food_nutrient, nutrient, on='nutrient_id', how='left')
nutrients_merged = nutrients_merged.drop(['nutrient_id'], axis=1)

portions_merged = pd.merge(portion, measure_unit, on='measure_unit_id', how='left')
portions_merged = portions_merged.drop(['measure_unit_id'], axis=1)

foods_and_nutrients_merged = pd.merge(foods_merged, nutrients_merged, on='fdc_id', how='left')

full_foods_merged = pd.merge(foods_and_nutrients_merged, portions_merged, on='fdc_id', how='inner')
full_foods_merged = full_foods_merged.drop(['portion_id'], axis=1)


# List of nutrients consumers care about
relevant_nutrients = ['Protein', 'Energy', 'Carbohydrate, by difference', 'Total lipid (fat)']
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
filtered_foods = full_foods_merged[full_foods_merged['nutrient_name'].isin(relevant_nutrients)]

# Add new column for per gram amount for various nutrients
filtered_foods['multiplier'] = 0
filtered_foods.loc[filtered_foods['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
filtered_foods.loc[filtered_foods['nutrient_unit'] == 'kJ', 'multiplier'] = round(1/100, 10)
filtered_foods.loc[filtered_foods['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
filtered_foods.loc[filtered_foods['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
filtered_foods.loc[filtered_foods['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)
filtered_foods['per_gram_amt'] = round(filtered_foods.nutrient_amount * filtered_foods.multiplier, 10)
filtered_foods.drop(['multiplier'], axis=1, inplace=True)


# Aggregate rows with equal values for food_description and nutrient_name
filtered_foods_agg = filtered_foods.groupby(['food_description', 'category_description', 'nutrient_name', 'nutrient_unit', 'portion_modifier', 'portion_unit']).mean(numeric_only=True).reset_index()

filtered_foods_agg.to_csv(os.path.join(Config.PROCESSED_DIR, 'filtered_foods_agg.csv'))