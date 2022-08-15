import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer

evento='pleb2022'
context_event='elecciones_constitucion'
folder='lookups'

ce=open('{}/{}/circelectoral.json'.format(evento,folder),encoding='utf-8')
jce=json.load(ce)
tabla=pd.DataFrame(columns=['circelec','nce','idservel','local','kei'])

for key in jce:
    k=key['c']
    d=key['d']
    print(d)
    direccion="https://www.servelelecciones.cl/data/{}/filters/locales/bycirc_electoral/{}.json".format(context_event,k)
    payload={}
    headers = {}
    response = requests.request("GET", direccion, headers=headers, data=payload)
    datas=json.loads(response.text)
    for esc in datas:
        a=esc['c']
        b=esc['d']
        j=b+'0p0'+d
        fila={'circelec':k,'nce':d,'idservel':a,'local':b,'kei':j}
        tabla=tabla.append(fila,ignore_index=True)

tabla.to_json(path_or_buf='{}/{}/localesservel.json'.format(evento,folder),orient='records',force_ascii=False)
ce.close()