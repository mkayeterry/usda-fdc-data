import requests
from bs4 import BeautifulSoup
import os
import zipfile
import re
import ingredient_slicer



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
        'fructose': 0.0, 
        'galactose': 0.0, 
        'glucose': 0.0, 
        'glutamic_acid': 0.0, 
        'glycine': 0.0, 
        'histidine': 0.0, 
        'ingredients': 'no_value', 
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


