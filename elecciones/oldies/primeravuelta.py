import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer

#com=open('elecciones/inputs/comunas.json',encoding='utf-8')
#prov=open('elecciones/inputs/provincias.json',encoding='utf-8')
#reg=open('elecciones/inputs/regiones.json',encoding='utf-8')
ce=open('elecciones/inputs/circelectoral.json',encoding='utf-8')
jce=json.load(ce)
tabla=pd.DataFrame(columns=['idservel','c1','c2','c3','c4','c5','c6','c7','vn','vb','ve','tv','tm','tm','tperc'])



for key in jce:
    k=key['c']
    d=key['d']
    direccion="https://www.servelelecciones.cl/data/elecciones_presidente/filters/locales/bycirc_electoral/{}.json".format(k)
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

tabla.to_csv(path_or_buf='elecciones/outputs/localesservel.csv',index=False,encoding='utf-8')
ce.close()