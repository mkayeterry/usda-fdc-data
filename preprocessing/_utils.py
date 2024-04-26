import requests
from bs4 import BeautifulSoup
import os
import zipfile
import re
import ingredient_slicer
import json
import numpy as np
import pandas as pd
from preprocessing import _constants

def get_usda_urls(USDA_URL = "https://fdc.nal.usda.gov/download-datasets.html", URL_PREFIX = "https://fdc.nal.usda.gov"):
    """
    Retrieve URLs for downloading USDA CSV files from the USDA website.

    Returns:
        csv_download_links (list of str): A list of URLs pointing to the downloadable CSV files.
    """
    # Get USDA HTML content
    html_content = requests.get(USDA_URL).text
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find table with the H2 tag "Latest Downloads" above it
    latest_downloads_table = soup.find('h2', text='Latest Downloads').find_next('table')

    # Find all <a> tags in the table
    download_a_tags = latest_downloads_table.find_all('a', href=True)

    # Extract the href attribute from each <a> tag
    csv_download_links = [URL_PREFIX + link['href'] for link in download_a_tags if 'csv' in link['href'] and "survey_food" not in link["href"]]

    return csv_download_links



def download_usda_csv(csv_url, raw_dir):
    """
    Download and extract a USDA CSV file from the provided URL.

    Parameters:
        csv_url (str): The URL pointing to the USDA CSV file to be downloaded.
        raw_dir (str): The directory where the downloaded and extracted file will be saved.

    Returns:
        None
    """
    def download_url(url, save_path, chunk_size=128):
        """
        Download a file from a URL and save it to the specified path.

        Parameters:
            url (str): The URL of the file to be downloaded.
            save_path (str): The path where the downloaded file will be saved.
            chunk_size (int): The size of each download chunk (default is 128 bytes).

        Returns:
            None
        """
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

    # Download the csv file
    filename = os.path.basename(csv_url)
    filepath = os.path.join(raw_dir, filename)

    print(f'Downloading file paths to:\n> {filepath}\n')

    download_url(csv_url, filepath)

    with zipfile.ZipFile(filepath, 'r') as zip:
        zip.extractall(raw_dir)

    # Delete the zip file
    os.remove(filepath)



def delete_unnecessary_files(dir, files_to_keep):
    """
    Delete unnecessary files from a directory, keeping only specified files.

    Parameters:
        dir_path (str): The path to the directory containing the files.
        files_to_keep (list of str): A list of filenames to keep in the directory.

    Returns:
        None
    """
    for root, dirs, files in os.walk(dir):

        for file in files:

            if file not in files_to_keep:
                
                file_path = os.path.join(root, file)
                os.remove(file_path)



def filter_relevent_nutrients(df):
    """
    Filter the DataFrame to include only rows with relevant nutrients that consumers care about.

    Parameters:
    - df (DataFrame): The input DataFrame containing nutrient data.

    Returns:
    - None
    """
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

    df.drop(df[~df['nutrient_name'].isin(relevant_nutrients) | (df['nutrient_unit'] == 'kJ')].index, inplace=True)




def add_per_gram_amt(df):
    """
    Add a new column for the per gram amount of various nutrients to the DataFrame.

    This function calculates the per gram amount for each nutrient based on its unit and adds a new column 
    'per_gram_amt' to the DataFrame.

    Parameters:
    - df (DataFrame): The input DataFrame containing nutrient data.

    Returns:
    - None
    """
    df['multiplier'] = 0

    df.loc[df['nutrient_unit'] == 'KCAL', 'multiplier'] = round(1/100, 10)
    df.loc[df['nutrient_unit'] == 'G', 'multiplier'] = round(1/100, 10)
    df.loc[df['nutrient_unit'] == 'MG', 'multiplier'] = round(0.001/100, 10)
    df.loc[df['nutrient_unit'] == 'UG', 'multiplier'] = round(0.000001/100, 10)

    df['per_gram_amt'] = round(df.nutrient_amount * df.multiplier, 10)
    
    df.drop(['multiplier'], axis=1, inplace=True)



def format_col_names(col_names):
    """
    Formats a list of column names:
    - Removes special characters,
    - Replaces whitespaces with underscores,
    - Forces all lowercase
    
    Parameters:
        col_names (list): A list of strings representing column names.
        
    Returns:
        formatted_names (list): A list of formatted column names.
    """
    formatted_names = []
    
    for name in col_names:

        # Split the name by whitespace
        split_name = name.split()

        # Join the words with underscore, remove special characters, and convert to lowercase
        joined_name = '_'.join(split_name)
        formatted_name = re.sub(r'[^\w\s]', '', joined_name.lower()).strip('_')

        formatted_names.append(formatted_name)
    
    return formatted_names
    


def format_ingredients(ingredients):
    """
    Format the ingredients string by performing various preprocessing steps:
    1. Remove substrings that contain less than a certain number of percentage symbols.
    2. Convert the ingredients to lowercase and remove specific substrings like 'ingredients:' and 'made from:'.
    3. Replace opening parentheses with commas.
    4. Remove special characters except commas and normalize whitespace.
    5. Split the ingredients string into a list, splitting at commas.
    6. Strip whitespace from each item in the ingredients list.
    
    Parameters:
        ingredients (str): The input string containing ingredients information.
        
    Returns:
        formatted_ingredients (list): A list of formatted ingredients.
    """

    # Matches phrases like "contains less than NUMBER %" or "contains less than NUMBER % of:"
    CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_REGEX = re.compile(r'contains less than\s*(?:\d*\.\d+|\d+\s*/\s*\d+|\d+)\s*%', re.IGNORECASE)
    CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_OF_REGEX = re.compile(r'contains less than\s*(?:\d*\.\d+|\d+\s*/\s*\d+|\d+)\s*%\s*of:', re.IGNORECASE)

    # Remove substrings that contain less than a certain number of percentage symbols from the formatted ingredients
    formatted_ingredients = CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_OF_REGEX.sub('', ingredients)
    formatted_ingredients = CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_REGEX.sub('', formatted_ingredients)

    # Convert the ingredients to lowercase and remove specific substrings
    formatted_ingredients = ingredients.lower()
    formatted_ingredients = formatted_ingredients.replace('ingredients:', '').replace('made from:', '')
    formatted_ingredients = formatted_ingredients.replace('(', ',')

    # Remove special characters except commas and normalize whitespace
    formatted_ingredients = re.sub(r'[^\w\s,]', '', formatted_ingredients)
    formatted_ingredients = re.sub(r'\s+', ' ', formatted_ingredients).strip()

    # Split the ingredients string into a list, splitting at commas and stripping whitespace
    formatted_ingredients = formatted_ingredients.split(',')
    for idx, val in enumerate(formatted_ingredients):
        formatted_ingredients[idx] = val.strip()

    return formatted_ingredients

def _find_and_replace_imps_patterns(category_value: str) -> str:
    """
    Find and replace matches of IMPS_PATTERN in the given string with the corresponding values in IMPS_MEAT_SERIES.
    """
    # category_value
    # category_value = "beef, cathcertoyu"
    if category_value is np.nan:
        return np.nan
    
    offset = 0
    pattern_iter = _constants.IMPS_PATTERN.finditer(category_value)

    for match in pattern_iter:
        match_string    = match.group()

        # Get the start and end of the match and the modified start and end positions given the offset
        start, end = match.start(), match.end()
        modified_start = start + offset
        modified_end = end + offset

        DIGIT_PATTERN = re.compile(r'(\d+)')

        digits = DIGIT_PATTERN.findall(match_string)[0]

        first_digit      = digits[0]
        first_two_digits = digits[0:2]

        if first_two_digits == "11":
            replacement_str = _constants.IMPS_MEAT_SERIES["11"][1]
        else:
            replacement_str = _constants.IMPS_MEAT_SERIES.get(first_digit, ["meat", "meat"])[1]
        
        
        # replacement_str = IMPS_MEAT_SERIES[DIGIT_PATTERN.findall(match_string)[0]][1]

        # Construct the modified string with the replacement applied
        category_value = category_value[:modified_start] + str(replacement_str) + category_value[modified_end:]

        # Update the offset for subsequent removals 
        offset += len(str(replacement_str)) - (end - start)

    return category_value

def define_source(path):
    """
    Determines the data source type based on the given path.

    Parameters:
        path (str): The file path from which to extract the data source type.

    Returns:
        usda_data_source, data_type (tuple): A tuple containing the USDA data source and the corresponding data type.
    """
    data_type = 'unspecified'

    if 'foundation' in path:
        data_type = 'foundation'

    if 'sr_legacy' in path:
        data_type = 'sr_legacy'

    if 'branded' in path:
        data_type = 'branded'

    split_path = path.split('/')
    usda_data_source = split_path[-1] if len(split_path) > 2 else 'FoodData_Central_csv'

    return (usda_data_source, data_type)



def apply_ingredient_slicer(entry):
    """
    Applies the IngredientSlicer to a portion modifier and returns the quantity and standardized unit.

    Parameters:
        entry (str): The row entry to be processed.

    Returns:
        selected_data (dict): A dictionary containing the quantity and standardized unit extracted from the portion modifier.
    """
    try:
        res = ingredient_slicer.IngredientSlicer(entry).to_json()

        selected_data = {k: res[k] for k in ('quantity', 'standardized_unit')}

        if selected_data['quantity'] is None:
            selected_data['quantity'] = 'no_value'

        if selected_data['standardized_unit'] is None:
            selected_data['standardized_unit'] = 'no_value'
            
    except Exception as e:  
        print(f'There was an error processing entry :"{entry}". {e}') 
        selected_data = {'quantity': 'no_value', 'standardized_unit': 'no_value'}

    return selected_data

# NOTE: this removes an extra iteration from the original version of this function (apply_ingredient_slicer())
def apply_ingredient_slicer2(entry):
    """
    Applies the IngredientSlicer to a portion modifier and returns the quantity and standardized unit.

    Parameters:
        entry (str): The row entry to be processed.

    Returns:
        selected_data (dict): A dictionary containing the quantity and standardized unit extracted from the portion modifier.
    """
    
    # # Test data
    # entry = "1/2 cup of oats, diced"

    # set some default values
    selected_data = {'quantity': 'no_value', 'standardized_unit': 'no_value'}

    try:
        res = ingredient_slicer.IngredientSlicer(entry).to_json()

        selected_data["quantity"]          = res.get("quantity", "no_value")
        selected_data["standardized_unit"] = res.get("standardized_unit", "no_value")
    
    except Exception as e:  
        print(f'There was an error processing entry :"{entry}". {e}') 

    return selected_data

# NOTE: Updated version of the original function fillna_and_define_dtype, 
# that fills NaN values using the data_types dictionary in a single function call to fillna() 
# and sets the data types for each column iteratively
# ****  The function now is not IN PLACE, rather it returns the updated DataFrame ****
# NOTE: fillna() can take a dictionary with column names as keys and values to fill NaN values with (i.e {col1: val1, col2: val2, ...})
# Reference to the "value" parameter in pandas documentation: 
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.fillna.html
def fillna_and_set_dtypes(df):
    """
    Fill NaN values in the specified column of the DataFrame with predefined values 
    based on the data types defined in the 'data_types' dictionary. Also, convert 
    the data type of the column to float32 if it matches the float type.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be processed.
        col (str): The column name to be processed.

    Returns:
        DataFrame: The processed DataFrame.
    """
    data_types = {
        'alanine': 0.0, 
        'arginine': 0.0, 
        'betaine': 0.0, 
        'brand_name': 'no_value', 
        'brand_owner': 'no_value', 
        'calcium_ca': 0.0, 
        'carbohydrate_by_difference': 0.0, 
        'carotene_beta': 0.0, 
        'category': 'no_value', 
        'category_id': 0, 
        'cholesterol': 0.0, 
        'choline_total': 0.0, 
        'copper_cu': 0.0, 
        'cystine': 0.0, 
        'data_type': 'no_value', 
        'energy': 0.0, 
        'fatty_acids_total_monounsaturated': 0.0, 
        'fatty_acids_total_polyunsaturated': 0.0, 
        'fatty_acids_total_saturated': 0.0, 
        'fatty_acids_total_trans': 0.0, 
        'fdc_id': 0, 
        'fiber_total_dietary': 0.0, 
        'folate_total': 0.0, 
        'food_description': 'no_value', 
        'food_common_name': 'no_value', 
        'food_common_category': 'no_value', 
        'fructose': 0.0, 
        'galactose': 0.0, 
        'glucose': 0.0, 
        'glutamic_acid': 0.0, 
        'glycine': 0.0, 
        'histidine': 0.0, 
        'ingredients': 'no_value', 
        # 'ingredients': 'no_value', 
        'iron_fe': 0.0, 
        'isoleucine': 0.0, 
        'lactose': 0.0, 
        'leucine': 0.0, 
        'lysine': 0.0, 
        'magnesium_mg': 0.0, 
        'maltose': 0.0, 
        'manganese_mn': 0.0, 
        'measure_unit_id': 0, 
        'methionine': 0.0, 
        'niacin': 0.0, 
        'nutrient_amount': 0.0, 
        'nutrient_id': 0, 
        'nutrient_name': 'no_value', 
        'nutrient_unit': 'no_value', 
        'pantothenic_acid': 0.0, 
        'phenylalanine': 0.0, 
        'phosphorus_p': 0.0, 
        'portion_amount': 0.0, 
        'portion_energy': 0.0, 
        'portion_gram_weight': 0.0, 
        'portion_id': 0, 
        'portion_modifier': 'no_value', 
        'portion_unit': 'no_value', 
        'potassium_k': 0.0, 
        'proline': 0.0, 
        'protein': 0.0, 
        'retinol': 0.0, 
        'riboflavin': 0.0, 
        'selenium_se': 0.0, 
        'serine': 0.0, 
        'sodium_na': 0.0, 
        'std_portion_unit': 'no_value', 
        'std_portion_amount': 0.0, 
        'sucrose': 0.0, 
        'sugars_total': 0.0, 
        'thiamin': 0.0, 
        'threonine': 0.0, 
        'total_lipid_fat': 0.0, 
        'tryptophan': 0.0, 
        'tyrosine': 0.0, 
        'usda_data_source': 'no_value', 
        'valine': 0.0, 
        'vitamin_a_rae': 0.0, 
        'vitamin_b12': 0.0, 
        'vitamin_b6': 0.0, 
        'vitamin_c_total_ascorbic_acid': 0.0, 
        'vitamin_d2_ergocalciferol': 0.0, 
        'vitamin_d3_cholecalciferol': 0.0, 
        'vitamin_e_alphatocopherol': 0.0, 
        'vitamin_k_dihydrophylloquinone': 0.0, 
        'vitamin_k_menaquinone4': 0.0, 
        'vitamin_k_phylloquinone': 0.0, 
        'zinc_zn': 0.0
        }
    
    # df = stacked_data
    df = df.fillna(data_types, inplace=False)

    # Get the data types of each column
    dtypes = df.dtypes.to_dict()

    # go through each column and set the data type for integer columns to int32 and float columns to float32
    for col in dtypes:
        if col in df.columns:
            if dtypes[col] in ["int32", "int64"]:
                df[col] = df[col].astype("int32", errors='ignore')
                
            elif dtypes[col] in ["float32", "float64"]:
                df[col] = df[col].astype("float32", errors='ignore')
    return df

# NOTE: this was the original method for filling NaN values and defining data types
def fillna_and_define_dtype(df, col):
    """
    Fill NaN values in the specified column of the DataFrame with predefined values 
    based on the data types defined in the 'data_types' dictionary. Also, convert 
    the data type of the column to float32 if it matches the float type.

    Parameters:
        df (pandas.DataFrame): The DataFrame to be processed.
        col (str): The column name to be processed.

    Returns:
        None
    """
    data_types = {
        'alanine': 0.0, 
        'arginine': 0.0, 
        'betaine': 0.0, 
        'brand_name': 'no_value', 
        'brand_owner': 'no_value', 
        'calcium_ca': 0.0, 
        'carbohydrate_by_difference': 0.0, 
        'carotene_beta': 0.0, 
        'category': 'no_value', 
        'category_id': 0, 
        'cholesterol': 0.0, 
        'choline_total': 0.0, 
        'copper_cu': 0.0, 
        'cystine': 0.0, 
        'data_type': 'no_value', 
        'energy': 0.0, 
        'fatty_acids_total_monounsaturated': 0.0, 
        'fatty_acids_total_polyunsaturated': 0.0, 
        'fatty_acids_total_saturated': 0.0, 
        'fatty_acids_total_trans': 0.0, 
        'fdc_id': 0, 
        'fiber_total_dietary': 0.0, 
        'folate_total': 0.0, 
        'food_description': 'no_value', 
        'food_common_name': 'no_value', 
        'food_common_category': 'no_value', 
        'fructose': 0.0, 
        'galactose': 0.0, 
        'glucose': 0.0, 
        'glutamic_acid': 0.0, 
        'glycine': 0.0, 
        'histidine': 0.0, 
        'ingredients': 'no_value', 
        # 'ingredients': 'no_value', 
        'iron_fe': 0.0, 
        'isoleucine': 0.0, 
        'lactose': 0.0, 
        'leucine': 0.0, 
        'lysine': 0.0, 
        'magnesium_mg': 0.0, 
        'maltose': 0.0, 
        'manganese_mn': 0.0, 
        'measure_unit_id': 0, 
        'methionine': 0.0, 
        'niacin': 0.0, 
        'nutrient_amount': 0.0, 
        'nutrient_id': 0, 
        'nutrient_name': 'no_value', 
        'nutrient_unit': 'no_value', 
        'pantothenic_acid': 0.0, 
        'phenylalanine': 0.0, 
        'phosphorus_p': 0.0, 
        'portion_amount': 0.0, 
        'portion_energy': 0.0, 
        'portion_gram_weight': 0.0, 
        'portion_id': 0, 
        'portion_modifier': 'no_value', 
        'portion_unit': 'no_value', 
        'potassium_k': 0.0, 
        'proline': 0.0, 
        'protein': 0.0, 
        'retinol': 0.0, 
        'riboflavin': 0.0, 
        'selenium_se': 0.0, 
        'serine': 0.0, 
        'sodium_na': 0.0, 
        'std_portion_unit': 'no_value', 
        'std_portion_amount': 0.0, 
        'sucrose': 0.0, 
        'sugars_total': 0.0, 
        'thiamin': 0.0, 
        'threonine': 0.0, 
        'total_lipid_fat': 0.0, 
        'tryptophan': 0.0, 
        'tyrosine': 0.0, 
        'usda_data_source': 'no_value', 
        'valine': 0.0, 
        'vitamin_a_rae': 0.0, 
        'vitamin_b12': 0.0, 
        'vitamin_b6': 0.0, 
        'vitamin_c_total_ascorbic_acid': 0.0, 
        'vitamin_d2_ergocalciferol': 0.0, 
        'vitamin_d3_cholecalciferol': 0.0, 
        'vitamin_e_alphatocopherol': 0.0, 
        'vitamin_k_dihydrophylloquinone': 0.0, 
        'vitamin_k_menaquinone4': 0.0, 
        'vitamin_k_phylloquinone': 0.0, 
        'zinc_zn': 0.0
        }

    if col in data_types:
        df[col].fillna(data_types[col], inplace=True)

        if type(data_types[col]) == float:
            df[col] = df[col].astype('float32', errors='ignore')

        if type(data_types[col]) == int:
            df[col] = df[col].astype('int32', errors='ignore')

def dict_to_json(dictionary):
    return json.dumps(dictionary, ensure_ascii=False)


# Takes in the pd.concat() of the 3 processed USDA datasets (i.e. Foundation, Branded, and SR Legacy), and 
# does some post processing steps before the data is finally saved
# Steps:
    # Selecting relevant columns
    # Cleaning up string columns (removing special characters, normalizing whitespace, etc.)
    # Filling in missing data for food_common_name and food_common_category
    # Converting the ingredients column to a JSON string

def postprocess_stacked_df(df, verbose = False):
    
    """Apply final cleaning processes to the concatenated USDA datasets.
    Args:
        df (pd.DataFrame): The concatenated USDA datasets. (The 3 dataframes are the output results of process_foundation(), process_branded(), and process_sr_legacy() functions.
        verbose (bool): Whether to print additional information (default is False).
    Returns:
        df (pd.DataFrame): The cleaned Pandas DataFrame.
    """

    print(f"Applying final cleaning processes...") if verbose else None

    df.reset_index(drop=True, inplace=True)

    df = df[[
                            'fdc_id', 'usda_data_source', 'data_type', 'category', 'brand_owner', 'brand_name', 
                            'food_description', 'food_common_name', 'food_common_category', 'ingredients', 
                            'portion_amount', 'portion_unit', 'portion_modifier', 'std_portion_amount', 'std_portion_unit', 'portion_gram_weight', 
                            'portion_energy', 'energy', 'carbohydrate_by_difference', 'protein', 'total_lipid_fat', 'fiber_total_dietary', 'sugars_total', 
                            'calcium_ca', 'iron_fe', 'vitamin_c_total_ascorbic_acid', 'vitamin_a_rae', 'vitamin_e_alphatocopherol', 
                            'sodium_na', 'cholesterol', 'fatty_acids_total_saturated', 'fatty_acids_total_trans', 'fatty_acids_total_monounsaturated', 
                            'fatty_acids_total_polyunsaturated', 'vitamin_k_phylloquinone', 'thiamin', 'riboflavin', 'niacin', 'vitamin_b6', 'folate_total', 
                            'vitamin_b12', 'vitamin_d3_cholecalciferol', 'vitamin_d2_ergocalciferol', 'pantothenic_acid', 'phosphorus_p', 'magnesium_mg', 
                            'potassium_k', 'zinc_zn', 'copper_cu', 'manganese_mn', 'selenium_se', 'carotene_beta', 'retinol', 'vitamin_k_dihydrophylloquinone', 
                            'vitamin_k_menaquinone4', 'tryptophan', 'threonine', 'methionine', 'phenylalanine', 'tyrosine', 'valine', 'arginine', 'histidine', 
                            'isoleucine', 'leucine', 'lysine', 'cystine', 'alanine', 'glutamic_acid', 'glycine', 'proline', 'serine', 'sucrose', 'glucose', 
                            'maltose', 'fructose', 'lactose', 'galactose', 'choline_total', 'betaine'
                        ]]

    # Set data types for all columns, and fill NA values using fillna_and_set_dtypes function
    df     = fillna_and_set_dtypes(df)

    # for col in df.columns.tolist():
    #     fillna_and_define_dtype(df, col)

    df['ingredients'] = df['ingredients'].apply(lambda x: [s.strip() for s in x] if not isinstance(x, str) else [""])
    df['ingredients'] = df['ingredients'].apply(lambda x: [re.sub(r'[\x00-\x19]', '', s) for s in x])
    df['ingredients'] = df.apply(lambda row: {'ingredients':row['ingredients']}, axis=1)
    df['ingredients'] = df['ingredients'].map(dict_to_json)

    # NOTE: Added part to clean up string columns
    for str_cols in df.select_dtypes(include=['object']).columns:
        # print(f"Processing: {str_cols}")
        if str_cols == "ingredients":
            # TODO: THIS IS A Super lazy hack to skip this column lol
            continue
        else:
            df[str_cols] = df[str_cols].astype(str)
            df[str_cols] = df[str_cols].str.strip()
            df[str_cols] = df[str_cols].apply(lambda x: re.sub(r'[\x00-\x19]', '', x))

    # For each food_description, if the food_description has a food_common_name in one of the other rows of the group, we want to
    # fill out any of the missing data for each food_description with the
    #  data from the valid food_common_name data in the rest of the group

    # # group by food_description and bfill() and ffill() the "food_common_name" column 
    # df['food_common_name'] = np.where(
    #                                         df['food_common_name'] == 'no_value',
    #                                         np.nan,
    #                                         df['food_common_name']
    #                                         )
    # df['food_common_name'] = df.groupby('food_description')['food_common_name'].bfill().ffill()

    # # group by food_description and bfill() and ffill() the "food_common_category" column 
    # df['food_common_category'] = np.where(
    #                                         df['food_common_category'] == 'no_value',
    #                                         np.nan,
    #                                         df['food_common_category']
    #                                         )
    # df['food_common_category'] = df.groupby('food_description')['food_common_category'].bfill().ffill()
    
    # # Fill in any remaining NaN values with 'no_value'
    # df['food_common_name'].fillna('no_value', inplace=True)
    # df['food_common_category'].fillna('no_value', inplace=True)

    # Convert the fdc_id to an integer, it should NOT have floating point values
    df["fdc_id"] = df["fdc_id"].astype('int32', errors='ignore')

    # coerce std_portion_amount to a float from a string
    df["std_portion_amount"] = df["std_portion_amount"].astype('float32', errors='ignore')

    return df