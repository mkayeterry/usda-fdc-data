import os
import pandas as pd
import numpy as np
import gc
from preprocessing._utils import *
from preprocessing import _constants
from preprocessing._utils import _find_and_replace_imps_patterns

def process_srlegacy(
        url = None, 
        output_dir = None, 
        raw_dir = None, 
        keep_files = False, 
    ):

    if not os.path.exists(raw_dir):
        os.makedirs(raw_dir)

    download_usda_csv(url, raw_dir)

    # Find the directory containing the downloaded files to define srlegacy_dir
    for path in os.listdir(raw_dir):
        if 'sr_legacy' in path:
            srlegacy_dir = os.path.join(raw_dir, path)
            source = define_source(srlegacy_dir)[0]

    # Delete unnecessary files if keep_files flag is not specified
    if not keep_files:
        files_to_keep = ['food_nutrient.csv', 'food.csv', 'nutrient.csv', 'food_category.csv',
                          'food_portion.csv','measure_unit.csv',
                          'food_attribute.csv', 'food_attribute_type.csv']
        delete_unnecessary_files(srlegacy_dir, files_to_keep)

    print(f'Initializing processing for:\n> {source}\n')

    # Load datasets
    food_nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'food_nutrient.csv'), 
                                usecols    = ['fdc_id', 'nutrient_id', 'amount'], 
                                dtype      = {'fdc_id': 'int32', 'nutrient_id': 'int32', 'amount': 'float32'}, 
                                low_memory = False)

    foods = pd.read_csv(os.path.join(srlegacy_dir, 'food.csv'), 
                                usecols    = ['fdc_id', 'description', 'food_category_id'], 
                                dtype      = {'fdc_id': 'int32', 'description': 'str', 'food_category_id': 'float32'}, 
                                low_memory = False)

    nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'nutrient.csv'),
                                usecols    = ['id', 'name', 'unit_name'], 
                                dtype      = {'id': 'int32', 'name': 'str', 'unit_name': 'str'}, 
                                low_memory = False)

    categories = pd.read_csv(os.path.join(srlegacy_dir, 'food_category.csv'), 
                                usecols    = ['id', 'description'], 
                                dtype      = {'id': 'int32', 'description': 'str'}, 
                                low_memory = False)

    portions = pd.read_csv(os.path.join(srlegacy_dir, 'food_portion.csv'), 
                                usecols    = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight'], 
                                dtype      = {'id': 'int32', 'fdc_id': 'int32', 'amount': 'float32', 'measure_unit_id': 'int32', 'modifier': 'str', 'gram_weight': 'float32'}, 
                                low_memory = False)

    measure_units = pd.read_csv(os.path.join(srlegacy_dir, 'measure_unit.csv'),
                                usecols    = ['id', 'name'], 
                                dtype      = {'id': 'int32', 'name': 'str'}, 
                                low_memory=False)
    
    food_attribute = pd.read_csv(os.path.join(srlegacy_dir, 'food_attribute.csv'),
                                usecols    = ['fdc_id', 'food_attribute_type_id', 'value'], 
                                dtype      = {'fdc_id': 'int32', 'food_attribute_type_id': 'int32', 'value': 'str'}, 
                                low_memory=False)
    food_attribute_type = pd.read_csv(os.path.join(srlegacy_dir, 'food_attribute_type.csv'),
                                usecols    = ['id', 'name'], 
                                dtype      = {'id': 'int32', 'name': 'str'}, 
                                low_memory=False)
    

    food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
    foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
    nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
    categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
    portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
    measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)
    
    food_attribute.rename(columns = {'name': 'food_attribute_name'}, inplace=True)
    food_attribute_type.rename(columns = {'id': 'food_attribute_type_id', 'name': 'food_attribute_type'}, inplace=True)

    gc.collect()
    
    # TODO: inspect this new method for filling NAs and setting datatypes
    # Set data types for all columns, and fill NA values using fillna_and_set_dtypes function
    food_nutrients     = fillna_and_set_dtypes(food_nutrients)
    foods              = fillna_and_set_dtypes(foods)
    nutrients          = fillna_and_set_dtypes(nutrients)
    categories         = fillna_and_set_dtypes(categories)
    portions           = fillna_and_set_dtypes(portions)
    measure_units      = fillna_and_set_dtypes(measure_units)
    food_attribute     = fillna_and_set_dtypes(food_attribute)
    food_attribute_type= fillna_and_set_dtypes(food_attribute_type)

    # # Set data types for all columns, and fill NA values using fillna_and_define_dtype function
    # for df in [food_nutrients, foods, nutrients, categories, portions, measure_units, food_attribute, food_attribute_type]:
    #     for col in df.columns.tolist():
    #         fillna_and_define_dtype(df, col)

    # Join datasets
    foods = pd.merge(foods, categories, on='category_id', how='left')
    foods.drop(['category_id'], axis=1, inplace=True)

    nutrients = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
    nutrients.drop(['nutrient_id'], axis=1, inplace=True)

    food_attribute = pd.merge(food_attribute, food_attribute_type, on='food_attribute_type_id', how='left')
    food_attribute.drop(['food_attribute_type_id'], axis=1, inplace=True)

    ###################################################

    # TODO: Code ive added to process IMPS and URMIS strings in the food_attribute table
    # find and replace the IMPS and URIMIS patterns with "meat" or the appropriate category
    food_attribute['value'] = food_attribute['value'].str.replace(_constants.URMIS_PATTERN, "meat", regex=True)
    food_attribute['value'] = food_attribute["value"].apply(lambda x: _find_and_replace_imps_patterns(x))
    
    # fill in missing or NaN values with "no_value"
    # food_attribute['value'] = food_attribute['value'].fillna(pd.NA)
    food_attribute['value'] = food_attribute['value'].fillna("no_value")

    food_attribute.rename(columns = {'value': 'food_common_name'}, inplace=True)
    food_attribute.drop(['food_attribute_type'], axis=1, inplace=True)

    # NOTE: could probably be done with np.where() but not gonna take the time
    # food_attribute["extracted_names"] = np.where(
    # food_attribute['value'].str.contains("URMIS"), food_attribute['value'].str.extract(URMIS_PATTERN),
    #  np.where(food_attribute['value'].str.contains("IMPS"), food_attribute['value'].str.extract(IMPS_PATTERN),food_attribute['value']))
    
    ########################################################

    # If True, portion_units are non-applicable
    if (portions['measure_unit_id'] == 9999).all():
        portions['portion_unit'] = 'no_value'
        # portions['portion_unit'] = pd.NA
    else:
        portions = pd.merge(portions, measure_units, on='measure_unit_id', how='left')

    portions.drop(['measure_unit_id'], axis=1, inplace=True)
    portions.drop(['portion_id'], axis=1, inplace=True)

    full_foods = pd.merge(foods, nutrients, on='fdc_id', how='left')
    full_foods = pd.merge(full_foods, food_attribute, on='fdc_id', how='left')
    full_foods = pd.merge(full_foods, portions, on='fdc_id', how='inner')

    gc.collect()

    # Filter for rows with relevant_nutrients using filter_relevent_nutrients function
    filter_relevent_nutrients(full_foods)

    # Add new column for per gram amount using add_per_gram_amt function
    add_per_gram_amt(full_foods)

    gc.collect()

    # TODO: need to double check this checks out and makes sense
    # get just the unique food descriptions and common names and stash them to join back after aggregating
    common_names = full_foods[['food_description', 'food_common_name']]
    common_names = common_names.drop_duplicates(inplace=False)

    # full_foods.columns
    # Aggregate rows with equal values and pivot data
    full_foods = full_foods.groupby(['food_description', 'category', 
                                    #  'food_common_name',
                                     'nutrient_name', 'nutrient_unit', 'portion_modifier', 'portion_unit']).mean(numeric_only=True).reset_index()

    full_foods = pd.pivot_table(full_foods,
                                    index=['fdc_id', 
                                        'food_description', 
                                        'category', 
                                        # 'food_common_name',
                                        'portion_amount', 
                                        'portion_unit', 
                                        'portion_modifier', 
                                        'portion_gram_weight'],
                                    columns=['nutrient_name'],
                                    values='per_gram_amt').reset_index()
    
    # TODO: this is the part where we bring the food_common_name column back in
    # merge back the common names by the food description column
    full_foods = pd.merge(full_foods, common_names, on='food_description', how='left')

    gc.collect()

    # Add portion_energy column as calorie estimate
    full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_gram_weight']

    # Add source columns
    full_foods['usda_data_source'] = define_source(srlegacy_dir)[0]
    full_foods['data_type'] = define_source(srlegacy_dir)[1]

    # Add columns applying ingredient_slicer function
    full_foods['portion_combined'] = full_foods.loc[:, 'portion_amount'].astype(str) + ' ' + full_foods.loc[:, 'portion_unit'] + ' ' + full_foods.loc[:, 'portion_modifier']

    # TODO: NEW
    # TODO: New method for extracting ingredients from portion_combined column
    full_foods['parsed_ingredient']  = full_foods['portion_combined'].apply(lambda x: ingredient_slicer.IngredientSlicer(x).parsed_ingredient()) 
    # full_foods['parsed_ingredient']  = full_foods['portion_combined'].apply(lambda x: ingredient_slicer.IngredientSlicer(x).to_json())
    # full_foods['std_portion_amount'] = full_foods['parsed_ingredient'].apply(lambda x: x["quantity"] if x["quantity"] else pd.NA)
    # full_foods['std_portion_unit']   = full_foods['parsed_ingredient'].apply(lambda x: x["standardized_unit"] if x["standardized_unit"] else pd.NA)
    full_foods['std_portion_amount'] = full_foods['parsed_ingredient'].apply(lambda x: x["quantity"] if x["quantity"] else "no_value")
    full_foods['std_portion_unit']   = full_foods['parsed_ingredient'].apply(lambda x: x["standardized_unit"] if x["standardized_unit"] else "no_value")
    full_foods.drop(['portion_combined', 'parsed_ingredient'], axis=1, inplace=True)
    
    # full_foods['std_portion_amount'] = full_foods['portion_combined'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
    # full_foods['std_portion_unit'] = full_foods['portion_combined'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])
    # full_foods.drop(['portion_combined'], axis=1, inplace=True)
    gc.collect()

    # Format the column names using the format_col_names function
    lst_col_names      = full_foods.columns.to_list()
    lst_col_names      = format_col_names(lst_col_names)
    full_foods.columns = lst_col_names

    # Format the food_description and category values using the format_col_values function
    for col in ['food_description', 'category']:
        full_foods[col] = full_foods[col].str.replace(',', '').replace('(', '').replace(')', '').str.lower()

    # group by food_description and bfill() and ffill() the "food_common_name" column 
    full_foods['food_common_name'] = np.where(
                                            full_foods['food_common_name'] == 'no_value',
                                            np.nan,
                                            full_foods['food_common_name']
                                            )
    full_foods['food_common_name'] = full_foods.groupby('food_description')['food_common_name'].bfill().ffill()
    
    # Fill in any remaining NaN values with 'no_value' 
    full_foods['food_common_name'].fillna('no_value', inplace=True)

    # Save intermediary dataframe
    full_foods.to_parquet(os.path.join(output_dir, f'processed_srlegacy.parquet'))

    # Delete remaining files if keep_files flag is not specified
    if not keep_files:
        import shutil

        for root, dirs, files in os.walk(srlegacy_dir):
            for file in files:
                file_path = os.path.join(root, file)
                os.remove(file_path)

        try:
            os.removedirs(srlegacy_dir)
        except OSError:
            shutil.rmtree(srlegacy_dir)

#---------------------------------------------------------------------
# ---- OLD VERSION OF process_srlegacy FUNCTION ----
# ---------------------------------------------------------------------

# import os
# import pandas as pd
# import gc
# from preprocessing._utils import *
# from preprocessing._constants import *

# def process_srlegacy(
#         url = None, 
#         output_dir = None, 
#         raw_dir = None, 
#         keep_files = False, 
#     ):

#     if not os.path.exists(raw_dir):
#         os.makedirs(raw_dir)

#     download_usda_csv(url, raw_dir)

#     # Find the directory containing the downloaded files to define srlegacy_dir
#     for path in os.listdir(raw_dir):
#         if 'sr_legacy' in path:
#             srlegacy_dir = os.path.join(raw_dir, path)
#             source = define_source(srlegacy_dir)[0]

#     # Delete unnecessary files if keep_files flag is not specified
#     if not keep_files:
#         files_to_keep = ['food_nutrient.csv', 'food.csv', 'nutrient.csv', 'food_category.csv', 'food_portion.csv', 'measure_unit.csv']
#         delete_unnecessary_files(srlegacy_dir, files_to_keep)

#     print(f'Initializing processing for:\n> {source}\n')

#     # Load datasets
#     food_nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'food_nutrient.csv'), 
#                                 usecols    = ['fdc_id', 'nutrient_id', 'amount'], 
#                                 dtype      = {'fdc_id': 'int32', 'nutrient_id': 'int32', 'amount': 'float32'}, 
#                                 low_memory = False)

#     foods = pd.read_csv(os.path.join(srlegacy_dir, 'food.csv'), 
#                                 usecols    = ['fdc_id', 'description', 'food_category_id'], 
#                                 dtype      = {'fdc_id': 'int32', 'description': 'str', 'food_category_id': 'float32'}, 
#                                 low_memory = False)

#     nutrients = pd.read_csv(os.path.join(srlegacy_dir, 'nutrient.csv'),
#                                 usecols    = ['id', 'name', 'unit_name'], 
#                                 dtype      = {'id': 'int32', 'name': 'str', 'unit_name': 'str'}, 
#                                 low_memory = False)

#     categories = pd.read_csv(os.path.join(srlegacy_dir, 'food_category.csv'), 
#                                 usecols    = ['id', 'description'], 
#                                 dtype      = {'id': 'int32', 'description': 'str'}, 
#                                 low_memory = False)

#     portions = pd.read_csv(os.path.join(srlegacy_dir, 'food_portion.csv'), 
#                                 usecols    = ['id', 'fdc_id', 'amount', 'measure_unit_id', 'modifier', 'gram_weight'], 
#                                 dtype      = {'id': 'int32', 'fdc_id': 'int32', 'amount': 'float32', 'measure_unit_id': 'int32', 'modifier': 'str', 'gram_weight': 'float32'}, 
#                                 low_memory = False)

#     measure_units = pd.read_csv(os.path.join(srlegacy_dir, 'measure_unit.csv'),
#                                 usecols    = ['id', 'name'], 
#                                 dtype      = {'id': 'int32', 'name': 'str'}, 
#                                 low_memory=False)

#     food_nutrients.rename(columns={'amount': 'nutrient_amount'}, inplace=True)
#     foods.rename(columns={'description': 'food_description', 'food_category_id': 'category_id'}, inplace=True)
#     nutrients.rename(columns={'id': 'nutrient_id', 'name': 'nutrient_name', 'unit_name': 'nutrient_unit'}, inplace=True)
#     categories.rename(columns={'id': 'category_id', 'description': 'category'}, inplace=True)
#     portions.rename(columns={'id': 'portion_id', 'amount': 'portion_amount', 'modifier': 'portion_modifier', 'gram_weight': 'portion_gram_weight'}, inplace=True)
#     measure_units.rename(columns={'id': 'measure_unit_id', 'name': 'portion_unit'}, inplace=True)

#     gc.collect()

#     # Set data types for all columns, and fill NA values using fillna_and_define_dtype function
#     for df in [food_nutrients, foods, nutrients, categories, portions, measure_units]:
#         for col in df.columns.tolist():
#             fillna_and_define_dtype(df, col)

#     # Join datasets
#     foods = pd.merge(foods, categories, on='category_id', how='left')
#     foods.drop(['category_id'], axis=1, inplace=True)

#     nutrients = pd.merge(food_nutrients, nutrients, on='nutrient_id', how='left')
#     nutrients.drop(['nutrient_id'], axis=1, inplace=True)

#     # If True, portion_units are non-applicable
#     if (portions['measure_unit_id'] == 9999).all():
#         portions['portion_unit'] = 'no_value'
#     else:
#         portions = pd.merge(portions, measure_units, on='measure_unit_id', how='left')

#     portions.drop(['measure_unit_id'], axis=1, inplace=True)
#     portions.drop(['portion_id'], axis=1, inplace=True)

#     full_foods = pd.merge(foods, nutrients, on='fdc_id', how='left')
#     full_foods = pd.merge(full_foods, portions, on='fdc_id', how='inner')

#     gc.collect()

#     # Filter for rows with relevant_nutrients using filter_relevent_nutrients function
#     filter_relevent_nutrients(full_foods)

#     # Add new column for per gram amount using add_per_gram_amt function
#     add_per_gram_amt(full_foods)

#     gc.collect()

#     # Aggregate rows with equal values and pivot data
#     full_foods = full_foods.groupby(['food_description', 'category', 'nutrient_name', 'nutrient_unit', 'portion_modifier', 'portion_unit']).mean(numeric_only=True).reset_index()

#     full_foods = pd.pivot_table(full_foods,
#                                     index=['fdc_id', 
#                                         'food_description', 
#                                         'category', 
#                                         'portion_amount', 
#                                         'portion_unit', 
#                                         'portion_modifier', 
#                                         'portion_gram_weight'],
#                                     columns=['nutrient_name'],
#                                     values='per_gram_amt').reset_index()

#     gc.collect()

#     # Add portion_energy column as calorie estimate
#     full_foods['portion_energy'] = full_foods['Energy'] * full_foods['portion_gram_weight']

#     # Add source columns
#     full_foods['usda_data_source'] = define_source(srlegacy_dir)[0]
#     full_foods['data_type'] = define_source(srlegacy_dir)[1]

#     # Add columns applying ingredient_slicer function
#     full_foods['portion_combined'] = full_foods.loc[:, 'portion_amount'].astype(str) + ' ' + full_foods.loc[:, 'portion_unit'] + ' ' + full_foods.loc[:, 'portion_modifier']
#     full_foods['std_portion_amount'] = full_foods['portion_combined'].apply(lambda x: list(apply_ingredient_slicer(x).values())[0])
#     full_foods['std_portion_unit'] = full_foods['portion_combined'].apply(lambda x: list(apply_ingredient_slicer(x).values())[1])
#     full_foods.drop(['portion_combined'], axis=1, inplace=True)
#     gc.collect()

#     # Format the column names using the format_col_names function
#     lst_col_names = full_foods.columns.to_list()
#     lst_col_names = format_col_names(lst_col_names)
#     full_foods.columns = lst_col_names

#     # Format the food_description and category values using the format_col_values function
#     for col in ['food_description', 'category']:
#         full_foods[col] = full_foods[col].str.replace(',', '').replace('(', '').replace(')', '').str.lower()

#     # Save intermediary dataframe
#     full_foods.to_parquet(os.path.join(output_dir, f'processed_srlegacy.parquet'))

#     # Delete remaining files if keep_files flag is not specified
#     if not keep_files:
#         import shutil

#         for root, dirs, files in os.walk(srlegacy_dir):
#             for file in files:
#                 file_path = os.path.join(root, file)
#                 os.remove(file_path)

#         try:
#             os.removedirs(srlegacy_dir)
#         except OSError:
#             shutil.rmtree(srlegacy_dir)
