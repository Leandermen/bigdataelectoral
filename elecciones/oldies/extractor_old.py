import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer

i=0

while i<70:
    url = "https://www.servelelecciones.cl/data/elecciones_presidente/computo/global/19001.json"
    payload={}
    headers = {}
    response = requests.request("GET", url, headers=headers, data=payload)
    respuesta=json.loads(response.text)
    dt=datetime.now()
    boric=respuesta['data'][0]['c']
    boric=boric.replace(".","")
    kast=respuesta['data'][1]['c']
    kast=kast.replace(".","")
    provoste=respuesta['data'][2]['c']
    provoste=provoste.replace(".","")
    sichel=respuesta['data'][3]['c']
    sichel=sichel.replace(".","")
    artes=respuesta['data'][4]['c']
    artes=artes.replace(".","")
    enriquez=respuesta['data'][5]['c']
    enriquez=enriquez.replace(".","")
    parisi=respuesta['data'][6]['c']
    parisi=parisi.replace(".","")
    mesas=respuesta['mesasEscrutadas']
    mesas=mesas.replace(".","")
    fila={'fechahora':dt,'boric':boric,'kast':kast,'provoste':provoste,'sichel':sichel,'artes':artes,'enriquez':enriquez,'parisi':parisi,'totmesas':mesas}
    field_names=['fechahora','boric','kast','provoste','sichel','artes','enriquez','parisi','totmesas']
    with open('outputs\event.csv', 'a') as f_object:
        dictwriter_object = DictWriter(f_object, fieldnames=field_names)
        dictwriter_object.writerow(fila)
        f_object.close()
    time.sleep(300)
    i=i+1
