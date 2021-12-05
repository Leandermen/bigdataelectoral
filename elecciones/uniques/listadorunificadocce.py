import requests
import json
import pandas as pd
import time
from datetime import datetime


reg=open("elecciones/inputs/regiones.json")
jreg=json.load(reg)
tablapres=pd.DataFrame(columns=['c_reg','c_prov','c_com','c_circ'])

for region in jreg:
    creg=region['c']
    urlprov="https://www.servelelecciones.cl/data/elecciones_presidente/filters/provincias/byregion/{}.json".format(creg)
    rpprov = requests.request("GET", urlprov, headers={}, data={})
    provincias=json.loads(rpprov.text)
    for provincia in provincias:
        cpro=provincia['c']
        urlcomuna="https://www.servelelecciones.cl/data/elecciones_presidente/filters/comunas/byprovincia/{}.json".format(cpro)
        rpcom = requests.request("GET", urlcomuna, headers={}, data={})
        comunas=json.loads(rpcom.text)
        for comuna in comunas:
            ccom=comuna['c']
            ncom=comuna['d']
            print(ncom)
            urlcirc="https://www.servelelecciones.cl/data/elecciones_presidente/filters/circ_electoral/bycomuna/{}.json".format(ccom)
            rpcirc = requests.request("GET", urlcirc, headers={}, data={})
            circelecs=json.loads(rpcirc.text)
            for circelec in circelecs:
                ccel=circelec['c']
                ncel=circelec['d']
                fila={
                    'c_reg':creg,
                    'c_prov':cpro,
                    'c_com':ccom,
                    'c_circ':ccel
                }
                tablapres=tablapres.append(fila,ignore_index=True)

tablapres.to_json(path_or_buf='elecciones/outputs/refs/cdpa.json',orient='records')
tablapres.to_csv(path_or_buf='elecciones/outputs/refs/cdpa.csv',index=False,encoding='utf-8')
reg.close()