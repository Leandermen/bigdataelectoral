import requests
import zipfile
import json
from io import BytesIO
import os
import pandas as pd

url="https://segundavotacion.servel.cl/nomina_completa_4.zip"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def download_and_process_nomina(url):
    output_dir = "gen20252v/datasets"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Download the zip file
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # Extract nomina_completa_4.json from the zip
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        json_filename = os.path.basename(url).replace(".zip", ".json")
        with zip_ref.open(json_filename) as nomina_file:
            nomina_data = json.load(nomina_file)
    
    # Convert JSON data to DataFrame
    df_nomina = pd.DataFrame(nomina_data)
    
    # Save DataFrame to CSV in output directory with UTF-8 encoding
    output_path = os.path.join(output_dir, "nomina_completa_4.csv")
    df_nomina.to_csv(output_path, index=False, encoding="utf-8")
    
    print(f"nomina_completa_4.csv saved to {output_path}")

if __name__ == "__main__":
    download_and_process_nomina(url)
