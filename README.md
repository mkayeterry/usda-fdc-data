<center>

# **USDA Food Data Central Processing**

</center>


## **Table of contents**
1. [Overview](#overview)
2. [Usage](#usage)
    - [Dependencies](#dependencies)
    - [Running the Scripts](#running-the-scripts)
    - [Options](#options)
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

<br/>

## **Contributions**


Data for this processing project was obtained from the [USDA FoodData Central (FDC) website](https://fdc.nal.usda.gov/).

<br/>

## **License**


This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
