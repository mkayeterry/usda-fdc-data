import requests
from bs4 import BeautifulSoup
import os
import zipfile

def download_usda_data(raw_dir):

    # DONT CHANGE THESE
    USDA_URL = "https://fdc.nal.usda.gov/download-datasets.html"
    URL_PREFIX = "https://fdc.nal.usda.gov"

    # Get USDA HTML content
    html_content = requests.get(USDA_URL).text
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find table with the H2 tag "Latest Downloads" above it
    latest_downloads_table = soup.find('h2', text='Latest Downloads').find_next('table')

    # Find all <a> tags in the table
    download_a_tags = latest_downloads_table.find_all('a', href=True)

    # Extract the href attribute from each <a> tag
    csv_download_links = [URL_PREFIX + link['href'] for link in download_a_tags if 'csv' in link['href'] and "survey_food" not in link["href"]]
    json_download_links = [URL_PREFIX + link['href'] for link in download_a_tags if 'json' in link['href'] and "survey_food" not in link["href"]]

    def download_url(url, save_path, chunk_size=128):
        r = requests.get(url, stream=True)
        with open(save_path, 'wb') as fd:
            for chunk in r.iter_content(chunk_size=chunk_size):
                fd.write(chunk)

    # Download the csv files
    for url in csv_download_links:
        filename = os.path.basename(url)
        filepath = os.path.join(raw_dir, filename)

        print(f"Downloading file paths to:\n> {filepath}")

        download_url(url, filepath)

        with zipfile.ZipFile(filepath, 'r') as zip:
            zip.extractall(raw_dir)

        # Delete the zip file
        os.remove(filepath)

if __name__ == "__main__":
    download_usda_data()