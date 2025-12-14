import requests
import zipfile
import json
from io import BytesIO
import os
import pandas as pd

#Variables de asistencia
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def download_and_extract(url_path):
    url = url_path
    output_dir = "gen20252v/outputs"
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Download the zip file
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # Extract config.json from the zip
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        json_filename = os.path.basename(url).replace(".zip", ".json")
        with zip_ref.open(json_filename) as config_file:
            config_data = json.load(config_file)
    
    # Save config.json to output directory with UTF-8 encoding
    output_path = os.path.join(output_dir, json_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    
    print(f"config.json saved to {output_path}")

if __name__ == "__main__":
    download_and_extract("https://segundavotacion.servel.cl/config.zip")
    download_and_extract("https://segundavotacion.servel.cl/iteracion.zip")
    download_and_extract("https://segundavotacion.servel.cl/candidaturas.zip")
    download_and_extract("https://segundavotacion.servel.cl/territorios.zip")
    download_and_extract("https://segundavotacion.servel.cl/territorios_ext.zip")
    download_and_extract("https://segundavotacion.servel.cl/nomina_completa_4.zip")
    download_and_extract("https://segundavotacion.servel.cl/total_votacion_4.zip")