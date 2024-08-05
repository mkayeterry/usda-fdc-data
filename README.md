<center>

# **USDA Food Data Central Processing**

</center>


## **Table of contents**
1. [Overview](#overview)
2. [Usage](#usage)
    - [Dependencies](#dependencies)
    - [Running the Scripts](#running-the-scripts)
    - [Options](#options)
    - [Output Data](#output-data)
3. [Contributions](#contributions)
4. [License](#license)

<br/>

## **Overview**


This repository contains scripts to download and process datasets from the USDA Food Data Central (FDC) for easy analysis and integration into other projects.
<br/>

1. **Preprocessing**:
   - The preprocessing pipeline consists of three scripts, each handling a specific data type (Foundation Foods, SR Legacy Foods, and Branded Foods).
   - Within each script:
     - Data is downloaded and read in from URLs gathered in `main.py`.
     - Only relevant dataframes and columns are kept, unless the `keep_files` flag is specified in arguments.
     - Data is cleaned, merged, aggregated, supplemented, and saved as an intermediary Parquet file.

2. **Data Stacking**:
   - Upon completion of individual processing, intermediary Parquet files are read into `main.py`.
   - The data is stacked together.
   - Missing values are filled.
   - The resulting data is saved within the output directory as a CSV file.

3. **Postprocessing**:
    - The postprocess_stacked_df function finalizes the cleaning of USDA concatenated datasets by:
        - Resetting the indices
        - Setting data types
        - Filling missing values
        - Cleaning string data

4. **Cleanup**:
   - Any remaining files other than the complete, processed data are deleted unless the `keep_files` flag is specified in arguments.


<br/>

## **Usage**


- ### **Dependencies**

    - Python 3.6 or later
    - pandas
    - requests
    - pyarrow
    - beautifulsoup4
    - ingredient-slicer


- ### **Running the Scripts**

    1. Clone the repository:

        ```bash
        git clone https://github.com/your_username/usda-fdc
        ```

    2. Navigate to the repository directory:

        ```bash
        cd usda-fdc
        ```

    3. Run the main script:

        ```bash
        python3 main.py
        ```

- ### **Options**

    `--output_dir`: Specify the output directory path (default: `fdc_data`).<br/>
    `--filename`: Specify output filename (default: `usda_food_nutrition_data.csv`).<br/>
    `--keep_files`: Keep raw and individual files after processing (for optimal memory utilization).

    ```bash
    python3 main.py --output_dir data -- filename data.csv --keep_files
    ```



- ### **Output Data**

    The processed USDA data contains a total of 650,701 entries and 78 columns. The output data is standardized and contains the following information:

    #### **String Identifiers**
    - **`fdc_id`**: The unique identifier assigned to each food item within the USDA Food Data Central.
    - **`usda_data_source`**: Indicates the source of the food item, denoting the specific downloaded file it originated from.
    - **`data_type`**: Describes the type of data associated with the food item, including branded, foundation, or sr_legacy.
    - **`category`**: The category or type of food.
    - **`brand_owner`**: The owner or manufacturer of the brand, for branded_foods only.
    - **`brand_name`**: The name of the brand, for branded_foods only.
    - **`food_description`**: A description of the food item.
    - **`ingredients`**: The ingredients used in the food item, for branded_foods only.

    #### **Portion Specifications**
    - **`portion_amount`**: The amount of the food item in the portion.
    - **`portion_unit`**: The unit of measurement for the portion.
    - **`portion_modifier`**: Any modifier applied to the portion, such as "large" or "1/8 of crust".
    - **`portion_gram_weight`**: The weight of the portion in grams.
    - **`portion_energy`**: The energy content in calories per portion.
    - **`std_portion_amount`**: Standardized portion amount, derived from the combination of portion_amount, portion_unit, and portion_modifier (i.e. 'one' --> 1).
    - **`std_portion_unit`**: Standardized portion unit, derived from the combination of portion_amount, portion_unit, and portion_modifier (i.e. 'oz' --> 'ounces').

    #### **Macronutrients Per Gram**
    - **`energy`**: The energy content per gram of the food item.
    - **`protein`**: The protein content per gram of the food item.
    - **`total_lipid_fat`**: The total lipid (fat) content per gram of the food item.
    - **`carbohydrate_by_difference`**: The carbohydrate content per gram of the food item.

    #### **Minerals Per Gram**
    - **`calcium_ca`**: Calcium
    - **`iron_fe`**: Iron
    - **`magnesium_mg`**: Magnesium
    - **`phosphorus_p`**: Phosphorus
    - **`potassium_k`**: Potassium
    - **`sodium_na`**: Sodium
    - **`zinc_zn`**: Zinc
    - **`copper_cu`**: Copper
    - **`manganese_mn`**: Manganese
    - **`selenium_se`**: Selenium

    #### **Vitamins Per Gram**
    - **`vitamin_a_rae`**: Vitamin A
    - **`vitamin_c_total_ascorbic_acid`**: Vitamin C
    - **`vitamin_e_alphatocopherol`**: Vitamin E
    - **`vitamin_k_phylloquinone`**: Vitamin K
    - **`thiamin`**: Thiamin (Vitamin B1)
    - **`riboflavin`**: Riboflavin (Vitamin B2)
    - **`niacin`**: Niacin (Vitamin B3)
    - **`vitamin_b6`**: Vitamin B6
    - **`folate_total`**: Folate
    - **`vitamin_b12`**: Vitamin B12
    - **`vitamin_d3_cholecalciferol`**: Vitamin D3
    - **`vitamin_d2_ergocalciferol`**: Vitamin D2
    - **`pantothenic_acid`**: Pantothenic Acid (Vitamin B5)
    - **`vitamin_k_dihydrophylloquinone`**: Vitamin K1 (Dihydrophylloquinone)
    - **`vitamin_k_menaquinone4`**: Vitamin K2 (Menaquinone-4)
    - **`carotene_beta`**: Beta-Carotene
    - **`retinol`**: Retinol (Vitamin A1)

    #### **Amino Acids Per Gram**
    - **`tryptophan`**, **`threonine`**, **`methionine`**, **`phenylalanine`**, **`tyrosine`**, **`valine`**, **`arginine`**, **`histidine`**, **`isoleucine`**, **`leucine`**, **`lysine`**, **`cystine`**, **`alanine`**, **`glutamic_acid`**, **`glycine`**, **`proline`**, **`serine`**

    #### **Carbohydrates and Sugars Per Gram**
    - **`sucrose`**, **`glucose`**, **`maltose`**, **`fructose`**, **`lactose`**, **`galactose`**

    #### **Other Compounds Per Gram**
    - **`choline_total`**: Total Choline
    - **`betaine`**: Betaine

<br/>

## **Contributions**


Data for this processing project was obtained from the [USDA FoodData Central (FDC) website](https://fdc.nal.usda.gov/).

<br/>

## **License**


This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.





