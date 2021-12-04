import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer


ce=open('elecciones/inputs/continentes.json',encoding='utf-8')
jce=json.load(ce)
tabla=pd.DataFrame(columns=['c_cont','c_pais','c_circ','n_circ','idservel','local'])

for cont in jce:
    c=cont['c']
    dpais="https://www.servelelecciones.cl/data/elecciones_presidente/filters/paises/bycontinente/{}.json".format(c)
    payload={}
    headers = {}
    response = requests.request("GET", dpais, headers=headers, data=payload)
    paises=json.loads(response.text)
    for pais in paises:
        p=pais['c']
        dlocal="https://www.servelelecciones.cl/data/elecciones_presidente/filters/circ_electoral/bypais/{}.json".format(p)
        payload={}
        headers = {}
        response = requests.request("GET", dlocal, headers=headers, data=payload)
        sedes=json.loads(response.text)
        for sede in sedes:
            l=sede['c']
            ld=sede['d']
            dlocal="https://www.servelelecciones.cl/data/elecciones_presidente/filters/locales/bycirc_electoral/{}.json".format(l)
            payload={}
            headers = {}
            response = requests.request("GET", dlocal, headers=headers, data=payload)
            locales=json.loads(response.text)
            for loc in locales:
                idservel=loc['c']
                puesto=loc['d']
                fila={'c_cont':c,'c_pais':p,'c_circ':l,'n_circ':ld,'idservel':idservel,'local':puesto}
                tabla=tabla.append(fila,ignore_index=True)

tabla.to_json(path_or_buf='elecciones/outputs/localesext.json',orient='records')
ce.close()