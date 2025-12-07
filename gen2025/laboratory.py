import requests
import zipfile
import json
from io import BytesIO
import os
import pandas as pd
from arcgis.gis import GIS

# URL of the zip file
url = "https://elecciones.servel.cl/nomina_completa_4.zip"

# Download the zip file
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
response = requests.get(url, headers=headers)
response.raise_for_status()

#Prepare GIS Data
print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemSource=gis.content.get('b0e79cbbdf0049e5b815dba16652ae31')

naclocal=ItemSource.layers[0]
comuchl=ItemSource.layers[1]
regichl=ItemSource.layers[2]

qlocn=naclocal.query(out_fields='OBJECTID,id_local,envio,blancos,nulos,total_emitidos,total_general,electores,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)
qregi=regichl.query(out_fields='OBJECTID,id_region,envio,blancos,nulos,total_emitidos,total_general,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)
qcomu=comuchl.query(out_fields='OBJECTID,id_comuna,blancos,nulos,total_emitidos,total_general,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)

tloca,tregi,tcomu=[],[],[]


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
        columns_to_drop = ['iteracion', 'orden_comuna', 'id_colegio', 'orden_local', 'candidatos','id_circ_provincial','instalada','envio']
        df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])
        cols_to_fill = ['blancos', 'nulos', 'total_emitidos', 'total_general', 'n41900101', 'n41900102', 'n41900103', 'n41900104', 'n41900105', 'n41900106', 'n41900107', 'n41900108']
        df[cols_to_fill] = df[cols_to_fill].fillna(0).infer_objects(copy=False)

        # Export to CSV
        csv_file = os.path.join(output_dir, json_file.replace('.json', '_mesas.csv'))
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print(f"Exported to: {csv_file}")

        # Export back to JSON (pretty-printed)
        output_file = os.path.join(output_dir, json_file)
        with open(output_file, 'w', encoding='utf-8') as out_f:
            json.dump(data, out_f, indent=2, ensure_ascii=False)
        print(f"Exported to: {output_file}")

# Group by id_local and aggregate
        groupby_cols = ['id_local', 'cod_eleccion', 'id_region', 'id_cirsen', 'id_distrito', 'id_provincia', 'id_comuna']
        sum_cols = ['blancos', 'nulos', 'total_emitidos', 'total_general', 'electores', 'n41900101', 'n41900102', 'n41900103', 'n41900104', 'n41900105', 'n41900106', 'n41900107', 'n41900108']

        df_locals = df.groupby('id_local', as_index=False).agg({
            **{col: 'first' for col in ['cod_eleccion', 'id_region', 'id_cirsen', 'id_distrito', 'id_provincia', 'id_comuna']},
            **{col: 'sum' for col in sum_cols}
        })

        # Determine winner among candidate columns per grouped record
        candidate_cols = [f"n{cid}" for cid in candidate_ids if f"n{cid}" in df_locals.columns]

        if candidate_cols:
            # Ensure numeric
            df_locals[candidate_cols] = df_locals[candidate_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

            def _determine_ganador(row):
                vals = row[candidate_cols]
                max_val = vals.max()
                winners = vals[vals == max_val].index.tolist()
                return "Empate" if len(winners) > 1 else winners[0]

            df_locals['ganador'] = df_locals.apply(_determine_ganador, axis=1)
        else:
            df_locals['ganador'] = None

        if 'id_local' in df_locals.columns:
            for i, row in df_locals.iterrows():
                val = row['id_local']
                locmodifier = [f for f in qlocn if f.attributes['id_local']==val][0]
                #locmodifier.attributes['envio'] = row['envio']
                locmodifier.attributes['blancos'] = row['blancos']
                locmodifier.attributes['nulos'] = row['nulos']
                locmodifier.attributes['total_emitidos'] = row['total_emitidos']
                locmodifier.attributes['total_general'] = row['total_general']
                locmodifier.attributes['n41900101'] = row['n41900101']
                locmodifier.attributes['n41900102'] = row['n41900102']
                locmodifier.attributes['n41900103'] = row['n41900103']
                locmodifier.attributes['n41900104'] = row['n41900104']
                locmodifier.attributes['n41900105'] = row['n41900105']
                locmodifier.attributes['n41900106'] = row['n41900106']
                locmodifier.attributes['n41900107'] = row['n41900107']
                locmodifier.attributes['n41900108'] = row['n41900108']
                locmodifier.attributes['ganador'] = row['ganador']
                tloca.append(locmodifier)
                #print(regmodifier.attributes)
        else:
            print("id_comuna column not found")
        #print(tcomu)
        naclocal.edit_features(updates=tloca)




        # Drop id_mesa and mesa columns if they exist
        df_locals = df_locals.drop(columns=['id_mesa', 'mesa'], errors='ignore')
        # Export grouped data to CSV
        grouped_csv_file = os.path.join(output_dir, json_file.replace('.json', '_locales.csv'))
        df_locals.to_csv(grouped_csv_file, index=False, encoding='utf-8')
        print(f"Exported grouped data to: {grouped_csv_file}")

               
# Group by id_comuna and aggregate
        groupby_cols = ['id_comuna', 'cod_eleccion', 'id_region', 'id_cirsen', 'id_distrito', 'id_provincia']
        sum_cols = ['blancos', 'nulos', 'total_emitidos', 'total_general', 'electores', 'n41900101', 'n41900102', 'n41900103', 'n41900104', 'n41900105', 'n41900106', 'n41900107', 'n41900108']

        df_comunas = df.groupby('id_comuna', as_index=False).agg({
            **{col: 'first' for col in ['cod_eleccion', 'id_region', 'id_cirsen', 'id_distrito', 'id_provincia']},
            **{col: 'sum' for col in sum_cols}
        })

        # Determine winner among candidate columns per grouped record
        candidate_cols = [f"n{cid}" for cid in candidate_ids if f"n{cid}" in df_comunas.columns]

        if candidate_cols:
            # Ensure numeric
            df_comunas[candidate_cols] = df_comunas[candidate_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

            def _determine_ganador(row):
                vals = row[candidate_cols]
                max_val = vals.max()
                winners = vals[vals == max_val].index.tolist()
                return "Empate" if len(winners) > 1 else winners[0]

            df_comunas['ganador'] = df_comunas.apply(_determine_ganador, axis=1)
        else:
            df_comunas['ganador'] = None

        if 'id_comuna' in df_comunas.columns:
            for i, row in df_comunas.iterrows():
                val = row['id_comuna']
                commodifier = [f for f in qcomu if f.attributes['id_comuna']==val][0]
                #commodifier.attributes['envio'] = row['envio']
                commodifier.attributes['blancos'] = row['blancos']
                commodifier.attributes['nulos'] = row['nulos']
                commodifier.attributes['total_emitidos'] = row['total_emitidos']
                commodifier.attributes['total_general'] = row['total_general']
                commodifier.attributes['n41900101'] = row['n41900101']
                commodifier.attributes['n41900102'] = row['n41900102']
                commodifier.attributes['n41900103'] = row['n41900103']
                commodifier.attributes['n41900104'] = row['n41900104']
                commodifier.attributes['n41900105'] = row['n41900105']
                commodifier.attributes['n41900106'] = row['n41900106']
                commodifier.attributes['n41900107'] = row['n41900107']
                commodifier.attributes['n41900108'] = row['n41900108']
                commodifier.attributes['ganador'] = row['ganador']
                tcomu.append(commodifier)
                #print(regmodifier.attributes)
        else:
            print("id_comuna column not found")
        #print(tcomu)
        comuchl.edit_features(updates=tcomu)

        # Drop id_mesa and mesa columns if they exist
        df_comunas = df_comunas.drop(columns=['id_mesa', 'mesa','id_local'], errors='ignore')
        # Export grouped data to CSV
        grouped_csv_file = os.path.join(output_dir, json_file.replace('.json', '_comunas.csv'))
        df_comunas.to_csv(grouped_csv_file, index=False, encoding='utf-8')
        print(f"Exported grouped data to: {grouped_csv_file}")

 # Group by id_region and aggregate
        groupby_cols = ['id_region', 'cod_eleccion']
        sum_cols = ['blancos', 'nulos', 'total_emitidos', 'total_general', 'electores', 'n41900101', 'n41900102', 'n41900103', 'n41900104', 'n41900105', 'n41900106', 'n41900107', 'n41900108']

        df_regiones = df.groupby('id_region', as_index=False).agg({
            **{col: 'first' for col in ['cod_eleccion', 'id_region']},
            **{col: 'sum' for col in sum_cols}
        })

        # Determine winner among candidate columns per grouped record
        candidate_cols = [f"n{cid}" for cid in candidate_ids if f"n{cid}" in df_regiones.columns]

        if candidate_cols:
            # Ensure numeric
            df_regiones[candidate_cols] = df_regiones[candidate_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
            def _determine_ganador(row):
                vals = row[candidate_cols]
                max_val = vals.max()
                winners = vals[vals == max_val].index.tolist()
                return "Empate" if len(winners) > 1 else winners[0]

            df_regiones['ganador'] = df_regiones.apply(_determine_ganador, axis=1)
        else:
            df_regiones['ganador'] = None
        
        if 'id_region' in df_regiones.columns:
            for i, row in df_regiones.iterrows():
                val = row['id_region']
                regmodifier = [f for f in qregi if f.attributes['id_region']==val][0]
                #regmodifier.attributes['envio'] = row['envio']
                regmodifier.attributes['blancos'] = row['blancos']
                regmodifier.attributes['nulos'] = row['nulos']
                regmodifier.attributes['total_emitidos'] = row['total_emitidos']
                regmodifier.attributes['total_general'] = row['total_general']
                regmodifier.attributes['n41900101'] = row['n41900101']
                regmodifier.attributes['n41900102'] = row['n41900102']
                regmodifier.attributes['n41900103'] = row['n41900103']
                regmodifier.attributes['n41900104'] = row['n41900104']
                regmodifier.attributes['n41900105'] = row['n41900105']
                regmodifier.attributes['n41900106'] = row['n41900106']
                regmodifier.attributes['n41900107'] = row['n41900107']
                regmodifier.attributes['n41900108'] = row['n41900108']
                regmodifier.attributes['ganador'] = row['ganador']
                tregi.append(regmodifier)
                #print(regmodifier.attributes)
        else:
            print("id_region column not found")
        #print(tregi)
        regichl.edit_features(updates=tregi)

        # Drop id_mesa and mesa columns if they exist
        df_regiones = df_regiones.drop(columns=['id_mesa', 'mesa','id_local','id_comuna', 'id_cirsen', 'id_distrito', 'id_provincia'], errors='ignore')
        # Export grouped data to CSV
        grouped_csv_file = os.path.join(output_dir, json_file.replace('.json', '_regiones.csv'))
        df_regiones.to_csv(grouped_csv_file, index=False, encoding='utf-8')
        print(f"Exported grouped data to: {grouped_csv_file}")

        
    else:
        print("No JSON file found in the zip archive")