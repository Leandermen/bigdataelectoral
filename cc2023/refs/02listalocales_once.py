import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer

evento='cc2023'
context_event='elecciones_consejo_gen'
folder='lookups'
s = requests.Session()
ce=open('{}/{}/circelectoral.json'.format(evento,folder),encoding='utf-8')
jce=json.load(ce)
tabla=pd.DataFrame(columns=['c_circ','nce','idservel','local','kei'])

reg=open("{}/{}/regiones.json".format(evento,folder))
jreg=json.load(reg)
tablapres=pd.DataFrame(columns=['c_reg','c_prov','c_com','c_circ','c_csen'])

for key in jce:
    k=key['c']
    d=key['d']
    print(d)
    direccion="https://www.servelelecciones.cl/data/{}/filters/locales/bycirc_electoral/{}.json".format(context_event,k)
    payload={}
    headers = {}
    #response = requests.request("GET", direccion, headers=headers, data=payload)
    response=s.get(direccion)
    datas=json.loads(response.text)
    for esc in datas:
        a=esc['c']
        b=esc['d']
        j=b+'0p0'+d
        fila={'c_circ':k,'nce':d,'idservel':a,'local':b,'kei':j}
        tabla=tabla.append(fila,ignore_index=True)

for region in jreg:
    creg=region['c']
    urlcsen="https://www.servelelecciones.cl/data/{}/filters/circ_senatorial/byregion/{}.json".format(context_event,creg)
    urlprov="https://www.servelelecciones.cl/data/{}/filters/provincias/byregion/{}.json".format(context_event,creg)
    rpcsen = requests.request("GET", urlcsen, headers={}, data={})
    csen=json.loads(rpcsen.text)
    keycsen=csen[0]['c']
    rpprov = requests.request("GET", urlprov, headers={}, data={})
    provincias=json.loads(rpprov.text)
    for provincia in provincias:
        cpro=provincia['c']
        urlcomuna="https://www.servelelecciones.cl/data/{}/filters/comunas/byprovincia/{}.json".format(context_event,cpro)
        rpcom = requests.request("GET", urlcomuna, headers={}, data={})
        comunas=json.loads(rpcom.text)
        for comuna in comunas:
            ccom=comuna['c']
            ncom=comuna['d']
            print(ncom)
            urlcirc="https://www.servelelecciones.cl/data/{}/filters/circ_electoral/bycomuna/{}.json".format(context_event,ccom)
            rpcirc = requests.request("GET", urlcirc, headers={}, data={})
            circelecs=json.loads(rpcirc.text)
            for circelec in circelecs:
                ccel=circelec['c']
                ncel=circelec['d']
                fila={
                    'c_reg':creg,
                    'c_prov':cpro,
                    'c_com':ccom,
                    'c_circ':ccel,
                    'c_csen':keycsen
                }
                tablapres=tablapres.append(fila,ignore_index=True)

shintabla = tabla.join(tablapres.set_index('c_circ'),on='c_circ')


shintabla.to_csv(path_or_buf='{}/{}/tbllocalesservel.csv'.format(evento,folder),index=False,encoding='utf-8')
tablapres.to_json(path_or_buf='{}/{}/idsdpac.json'.format(evento,folder),orient='records',force_ascii=False)
#tabla.to_csv(path_or_buf='{}/{}/tbllocalesservel.csv'.format(evento,folder),index=False,encoding='utf-8')
reg.close()
ce.close()