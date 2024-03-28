import re
import ingredient_slicer

def format_names(col_names):
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



def apply_ingredient_slicer(portion_modifier):
    """
    Applies the IngredientSlicer to a portion modifier and returns the quantity and standardized unit.

    Parameters:
        portion_modifier (str): The portion modifier to be processed.

    Returns:
        tuple: A tuple containing the quantity and standardized unit extracted from the portion modifier.
    """
    try:
        res = ingredient_slicer.IngredientSlicer(portion_modifier).to_json()

        selected_data = {k: res[k] for k in ('quantity', 'standardized_unit')}

        if selected_data['quantity'] is None and selected_data['standardized_unit'] is None:
            selected_result = 'NA'

        # quantity, unit = res['quantity'], res['standardized_unit']

        # quantity = quantity if quantity else 'NA'
        # unit = unit if unit else 'NA'

    except Exception as e:  
        print(f'There was an error processing {portion_modifier}. {e}') 
        selected_data = 'NA'

    return selected_data