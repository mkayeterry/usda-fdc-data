# USDA Food Data Central Processing

This repository contains scripts to download and process datasets from the USDA Food Data Central (FDC) for easy analysis and integration into other projects.

## Overview

The preprocessing pipeline consists of three main steps:

1. **Downloading Data:** The scripts fetch data from the USDA FDC API. There are separate scripts for each dataset: Foundation Foods, SR Legacy Foods, and Branded Foods.

2. **Processing Data:** Each dataset is processed individually to clean, format, and aggregate the data. The preprocessing involves handling missing values, data type conversion, and data aggregation.

3. **Stacking Data:** After processing each dataset, the individual processed files are stacked together to create a single comprehensive dataset and saved as a CSV.

## Usage

### Dependencies

- Python 3.6 or later
- pandas
- BeautifulSoup
- ingredient-slicer

### Running the Scripts

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

### Options

- `--base_dir`: Specify the base directory (default: `fdc_data`).
- `--keep_files`: Keep raw and individual files after processing (optional).

```bash
python3 main.py --base_dir data --keep_files
```


## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
