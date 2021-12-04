import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer


reg=open("elecciones/inputs/regiones.json")
jreg=json.load(reg)
tablapres=pd.DataFrame(columns=['c_reg','n_reg','c_prov','n_prov','c_com','n_com','c_circ','n_circ','c_loc','n_loc','c_mesa','n_mesa','md','me'])

for region in jreg:
    creg=region['c']
    nreg=region['d']
    urlprov="https://www.servelelecciones.cl/data/elecciones_presidente/filters/provincias/byregion/{}.json".format(creg)
    rpprov = requests.request("GET", urlprov, headers={}, data={})
    provincias=json.loads(rpprov.text)
    for provincia in provincias:
        cpro=provincia['c']
        npro=provincia['d']
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
                urlloc="https://www.servelelecciones.cl/data/elecciones_presidente/filters/locales/bycirc_electoral/{}.json".format(ccel)
                rploc = requests.request("GET", urlloc, headers={}, data={})
                locales=json.loads(rploc.text)
                for locl in locales:
                    cloc=locl['c']
                    nloc=locl['d']
                    urlmesas="https://www.servelelecciones.cl/data/elecciones_presidente/filters/mesas/bylocales/{}.json".format(cloc)
                    rpmesas = requests.request("GET", urlmesas, headers={}, data={})
                    mesas=json.loads(rpmesas.text)
                    for mesa in mesas:
                        cmes=mesa['c']
                        nmes=mesa['d']
                        fila={
                            'c_reg':creg,
                            'n_reg':nreg,
                            'c_prov':cpro,
                            'n_prov':npro,
                            'c_com':ccom,
                            'n_com':ncom,
                            'c_circ':ccel,
                            'n_circ':ncel,
                            'c_loc':cloc,
                            'n_loc':nloc,
                            'c_mesa':cmes,
                            'n_mesa':nmes,
                            'md':mesa['md'],
                            'me':mesa['me']
                        }
                        tablapres=tablapres.append(fila,ignore_index=True)

tablapres.to_json(path_or_buf='elecciones/inputs/mesas_cl.json',orient='records')
tablapres.to_csv(path_or_buf='elecciones/inputs/mesas_cl.csv',index=False,encoding='utf-8')
reg.close()