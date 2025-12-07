import requests
import zipfile
import json
from io import BytesIO
import os
import pandas as pd
from arcgis.gis import GIS
from PIL import Image

# URL of the zip file
url = "https://elecciones.servel.cl/nomina_completa_5.zip"

# Download the zip file
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
response = requests.get(url, headers=headers)
response.raise_for_status()
tiff_dir = 'gen2025/tiffs'
png_dir = 'gen2025/pngs'
os.makedirs(tiff_dir, exist_ok=True)
os.makedirs(png_dir, exist_ok=True)



# Extract and read the JSON file
with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
    # List files in the zip
    file_list = zip_ref.namelist()
    # Find and read the JSON file
    json_file = next((f for f in file_list if f.endswith('.json')), None)
    if json_file:
        with zip_ref.open(json_file) as f:
            data = json.load(f)
        print(f"Successfully loaded: {json_file}")
        print(f"Data structure: {type(data)}, Keys: {list(data.keys()) if isinstance(data, dict) else 'N/A'}")
        print(f"Data size: {len(json.dumps(data))} bytes")
        output_dir = "gen2025/outputs"
        os.makedirs(output_dir, exist_ok=True)
        # Convert to tabular format and export to CSV
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict) and any(isinstance(v, list) for v in data.values()):
            # If dict contains a list, use the largest list
            list_key = max((k for k, v in data.items() if isinstance(v, list)), 
                  key=lambda k: len(data[k]))
            df = pd.DataFrame(data[list_key])
        else:
            df = pd.DataFrame([data])
        #print(f"Table shape: {df.shape}")
        #print(f"\nFirst few rows:\n{df.head()}")
        # Create columns n41900101..n41900108 from the 'candidatos' list-of-dicts per row
        candidate_ids = [41900101, 41900102, 41900103, 41900104, 41900105, 41900106, 41900107, 41900108]

        def extract_votos(candidatos, target_id):
            if candidatos is None:
                return None
            # If stored as JSON string, try to parse
            if isinstance(candidatos, str):
                try:
                    candidatos = json.loads(candidatos)
                except Exception:
                    return None
            if not isinstance(candidatos, list):
                return None
            for c in candidatos:
                if not isinstance(c, dict):
                    continue
                cid = c.get('id_candidato')
                if cid is None:
                    continue
                if str(cid) == str(target_id):
                    return c.get('votos')
            return None

        if 'candidatos' in df.columns:
            for cid in candidate_ids:
                col_name = f"n{cid}"
                df[col_name] = df['candidatos'].apply(lambda x, tid=cid: extract_votos(x, tid))
        else:
            # If no candidatos field, create the columns with None
            for cid in candidate_ids:
                df[f"n{cid}"] = None

        print("Created columns:", [f"n{cid}" for cid in candidate_ids])
        #"[{'id_candidato': 41900101, 'id_partido': 157, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 1, 'electo': 0}, {'id_candidato': 41900102, 'id_partido': 6, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 2, 'electo': 0}, {'id_candidato': 41900103, 'id_partido': 99, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 3, 'electo': 0}, {'id_candidato': 41900104, 'id_partido': 235, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 4, 'electo': 0}, {'id_candidato': 41900105, 'id_partido': 150, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 5, 'electo': 0}, {'id_candidato': 41900106, 'id_partido': 99, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 6, 'electo': 0}, {'id_candidato': 41900107, 'id_partido': 3, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 7, 'electo': 0}, {'id_candidato': 41900108, 'id_partido': 99, 'id_pacto': 999, 'id_subpacto': 999, 'votos': None, 'orden_voto': 8, 'electo': 0}]"
        # Drop unnecessary columns
        columns_to_drop = ['iteracion','candidatos']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        #cols_to_fill = ['blancos', 'nulos', 'total_emitidos', 'total_general', 'n41900101', 'n41900102', 'n41900103', 'n41900104', 'n41900105', 'n41900106', 'n41900107', 'n41900108']
        #df[cols_to_fill] = df[cols_to_fill].fillna(0).infer_objects(copy=False)

        # Filter dataframe where id_region equals 3011
        df = df[df['id_region'] == 3011]

        # Download TIFF files from path_s3 column
        tiff_dir = "gen2025/actas"
        os.makedirs(tiff_dir, exist_ok=True)

    for idx, row in df.iterrows():
        if pd.notna(row['path_s3']):
            tiff_url = f"https://actas-interno.servel.cl/{row['path_s3']}"
            try:
                tiff_response = requests.get(tiff_url, headers=headers, timeout=20)
                tiff_response.raise_for_status()

                # nombre base y guardado del tiff (opcional)
                tiff_filename = row['path_s3'].split('/')[-1]
                tiff_path = os.path.join(tiff_dir, tiff_filename)
                with open(tiff_path, 'wb') as tiff_file:
                    tiff_file.write(tiff_response.content)
                print(f"Downloaded TIFF: {tiff_filename}")

                # convertir a PNG(s) en memoria y guardarlos
                img = Image.open(BytesIO(tiff_response.content))
                n_frames = getattr(img, "n_frames", 1)

                base_name, _ = os.path.splitext(tiff_filename)
                for page in range(n_frames):
                    try:
                        img.seek(page)
                        out_name = f"{base_name}_page_{page+1}.png"
                        out_path = os.path.join(png_dir, out_name)

                        # Si quieres reducir tamaño o convertir a RGB:
                        frame = img.convert("RGBA") if img.mode != "RGBA" else img.copy()
                        frame.save(out_path, format="PNG")
                        print(f"  -> Saved {out_name}")
                    except Exception as e_page:
                        print(f"  ! Error processing page {page+1} of {tiff_filename}: {e_page}")

                # cerrar imagen
                img.close()

            except Exception as e:
                print(f"Failed to download or process {tiff_url}: {e}")

        # Export to CSV
        csv_file = os.path.join(output_dir, json_file.replace('.json', '_mesasex.csv'))
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Exported to: {csv_file}")

        # Export back to JSON (pretty-printed)
        #output_file = os.path.join(output_dir, json_file)
        #with open(output_file, 'w', encoding='utf-8') as out_f:
        #    json.dump(data, out_f, indent=2, ensure_ascii=False)
        #print(f"Exported to: {output_file}")