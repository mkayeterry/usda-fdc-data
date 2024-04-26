import re

# IMPS Meat standard series names from USDA / AMS standards
# Source: https://www.ams.usda.gov/grades-standards/imps
    # General Requirements (GR)
    # Quality Assurance Provisions (QAP)
    # Fresh Beef Series 100
    # Fresh Lamb Series 200
    # Fresh Veal and Calf Series 300
    # Fresh Pork Series 400
    # Cured, Cured and Smoked, and Fully Cooked Pork Products Series 500
    # Cured, Dried, and Smoked Beef Products Series 600
    # Variety Meats and By-Products Series 700
    # Sausage Products Series 800
    # Fresh Goat Series 11

IMPS_MEAT_SERIES = {
    "1": ("Fresh Beef Series", "beef"),
    "2": ("Fresh Lamb Series", "lamb"),
    "3": ("Fresh Veal and Calf Series", "veal"),
    "4": ("Fresh Pork Series", "pork"),
    "5": ("Cured, Cured and Smoked, and Fully Cooked Pork Products Series", "pork"),
    "6": ("Cured, Dried, and Smoked Beef Products Series", "beef"),
    "7": ("Variety Meats and By-Products Series", "variety meats and meat by-products"), 
    "8": ("Sausage Products Series", "sauage"),
    "11": ("Fresh Goat Series", "goat")
}

# -----------------------------------------------------
# ---- Regular Expressions constants ----
# -----------------------------------------------------

# matches any digits
DIGIT_PATTERN = re.compile(r'(\d+)')

# Matches strings with the URMIS/IMPS string patterns
URMIS_PATTERN = re.compile(r'URMIS\s*#\s*(?:\d*)', re.IGNORECASE)
IMPS_PATTERN  = re.compile(r'(IMPS.*(?:\d*))', re.IGNORECASE)
