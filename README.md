<center>

# **USDA Food Data Central Processing**

</center>


## **Table of contents**
1. [Overview](#overview)
2. [Usage](#usage)
    - [Dependencies](#dependencies)
    - [Running the Scripts](#running-the-scripts)
    - [Options](#options)
    - [Output Data](#data)
3. [Contributions](#contributions)
4. [License](#license)

<br/>

## **Overview**


This repository contains scripts to download and process datasets from the USDA Food Data Central (FDC) for easy analysis and integration into other projects.
<br/>

The preprocessing pipeline consists of three main steps:

1. **Downloading Data:** The scripts fetch data by scraping information from the USDA FoodData Central (FDC) website. There are separate scripts for each dataset: Foundation Foods, SR Legacy Foods, and Branded Foods.

2. **Processing Data:** Each dataset is downloaded and processed individually. The preprocessing involves handling missing values, cleaning, joining, and aggregating data. Upon preprocessing completion, the intermediary file is saved, and the downloaded data is deleted (unless keep_files flag is specified).

3. **Stacking Data:** After processing each dataset, the individual processed files are stacked together to create a single comprehensive dataset and saved as a CSV to the specified or default base directory.

<br/>

## **Usage**


- ### **Dependencies**

    - Python 3.6 or later
    - pandas
    - BeautifulSoup
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

    `--base_dir`: Specify the base directory path (default: `fdc_data`).<br/>
    `--keep_files`: Keep raw and individual files after processing (optional).

    ```bash
    python3 main.py --base_dir data --keep_files
    ```



- ### **Output Data**

    The output data from the preprocessing pipeline is standardized and contains the following information:

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





