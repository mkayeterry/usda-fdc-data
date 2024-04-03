import re
import ingredient_slicer



def format_col_names(col_names):
    """
    Formats a list of column names:
    - Removes special characters,
    - Replaces whitespaces with underscores,
    - Forces all lowercase
    
    Args:
        col_names (list): A list of strings representing column names.
        
    Returns:
        list: A list of formatted column names.
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



def format_col_values(col_value):
    """
    Format the given values by removing commas, parenthesis, and converting to lowercase.

    Args:
    - col_value (str): The column value to format.

    Returns:
    - formatted_value (str): The formatted column value.
    """
    formatted_value = col_value.replace(',', '').replace('(', '').replace(')', '').lower()

    return formatted_value
    



def format_ingredients(ingredients):
    """
    Format the ingredients string by performing various preprocessing steps:
    1. Remove substrings that contain less than a certain number of percentage symbols.
    2. Convert the ingredients to lowercase and remove specific substrings like 'ingredients:' and 'made from:'.
    3. Replace opening parentheses with commas.
    4. Remove special characters except commas and normalize whitespace.
    5. Split the ingredients string into a list, splitting at commas.
    6. Strip whitespace from each item in the ingredients list.
    
    Args:
        ingredients (str): The input string containing ingredients information.
        
    Returns:
        list: A list of formatted ingredients.
    """

    # Matches phrases like "contains less than NUMBER %" or "contains less than NUMBER % of:"
    CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_REGEX = re.compile(r'contains less than\s*(?:\d*\.\d+|\d+\s*/\s*\d+|\d+)\s*%', re.IGNORECASE)
    CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_OF_REGEX = re.compile(r'contains less than\s*(?:\d*\.\d+|\d+\s*/\s*\d+|\d+)\s*%\s*of:', re.IGNORECASE)

    # Remove substrings that contain less than a certain number of percentage symbols from the formatted ingredients
    formatted_ingredients = CONTAINS_LESS_THEN_NUMBER_PCT_SYMBOL_OF_REGEX.sub('', formatted_ingredients)
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

    Args:
        path (str): The file path from which to extract the data source type.

    Returns:
        tuple: A tuple containing the USDA data source and the corresponding data type.
    """
    data_type = 'unspecified'

    if 'foundation' in path:
        data_type = 'foundation'

    if 'sr_legacy' in path:
        data_type = 'sr_legacy'

    if 'branded' in path:
        data_type = 'branded'

    split_path = path.split('/')
    usda_data_source = split_path[-2] if len(split_path) > 2 else 'FoodData_Central_csv'

    return (usda_data_source, data_type)



def apply_ingredient_slicer(entry):
    """
    Applies the IngredientSlicer to a portion modifier and returns the quantity and standardized unit.

    Parameters:
        entry (str): The row entry to be processed.

    Returns:
        tuple: A tuple containing the quantity and standardized unit extracted from the portion modifier.
    """
    try:
        res = ingredient_slicer.IngredientSlicer(entry).to_json()

        selected_data = {k: res[k] for k in ('quantity', 'standardized_unit')}

        if selected_data['quantity'] is None:
            selected_data['quantity'] = 'no_value_given'

        if selected_data['standardized_unit'] is None:
            selected_data['standardized_unit'] = 'no_value_given'
            
    except Exception as e:  
        print(f'There was an error processing entry :"{entry}". {e}') 
        selected_data = {'quantity': 'no_value_given', 'standardized_unit': 'no_value_given'}

    return selected_data
