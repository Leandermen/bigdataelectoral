import requests
import json
import pandas as pd
import time
from datetime import datetime


reg=open("elecciones/inputs/continentes.json",encoding="utf-8")
jreg=json.load(reg)
tablapres=pd.DataFrame(columns=['c_pais','c_cont'])

for continente in jreg:
    ccon=continente['c']
    urlpais="https://www.servelelecciones.cl/data/elecciones_presidente/filters/paises/bycontinente/{}.json".format(ccon)
    rppais = requests.request("GET", urlpais, headers={}, data={})
    paises=json.loads(rppais.text)
    for pais in paises:
        cpai=pais['c']
        fila={
            'c_pais':cpai,
            'c_cont':ccon
        }
        tablapres=tablapres.append(fila,ignore_index=True)

tablapres.to_json(path_or_buf='elecciones/outputs/refs/cpais.json',orient='records')
tablapres.to_csv(path_or_buf='elecciones/outputs/refs/cpais.csv',index=False,encoding='utf-8')
reg.close()