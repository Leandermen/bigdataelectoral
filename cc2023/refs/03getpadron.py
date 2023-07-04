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
context_event='participacion'
folder='lookups'

s = requests.Session()

reg=open("{}/{}/regiones.json".format(evento,folder))
jreg=json.load(reg)
com=open("{}/{}/comunas.json".format(evento,folder))
jcom=json.load(com)
loc=open("{}/{}/localesservel.json".format(evento,folder))
jloc=json.load(loc)
tablapreg=pd.DataFrame(columns=['idservel','electores'])
tablapcom=pd.DataFrame(columns=['idservel','electores'])
tablaploc=pd.DataFrame(columns=['idservel','electores'])

for key in jreg:
    k=key['c']
    datareg="https://www.servelelecciones.cl/data/{}/computo/regiones/{}.json".format(context_event,k)
    rresp=s.get(datareg)
    datas=json.loads(rresp.text)
    v=int(datas['resumen'][0]['c'].replace('.',''))
    filar={'idservel':k,'electores':v}
    tablapreg=tablapreg.append(filar,ignore_index=True)
    
for loc in jloc:
    l=loc['idservel']
    dataloc="https://www.servelelecciones.cl/data/{}/computo/locales/{}.json".format(context_event,l)
    lresp=s.get(dataloc)
    datal=json.loads(lresp.text)
    y=int(datal['resumen'][0]['c'].replace('.',''))
    filal={'idservel':l,'electores':y}
    tablaploc=tablaploc.append(filal,ignore_index=True)

for comm in jcom:
    c=comm['c']
    datacom="https://www.servelelecciones.cl/data/{}/computo/comunas/{}.json".format(context_event,c)
    print(datacom)
    cresp=s.get(datacom)
    datac=json.loads(cresp.text)
    n=int(datac['resumen'][0]['c'].replace('.',''))
    filac={'idservel':c,'electores':n}
    tablapcom=tablapcom.append(filac,ignore_index=True)



tablapreg.to_csv(path_or_buf='{}/{}/tblparticreg.csv'.format(evento,folder),index=False,encoding='utf-8')
tablapcom.to_csv(path_or_buf='{}/{}/tblparticcom.csv'.format(evento,folder),index=False,encoding='utf-8')
tablaploc.to_csv(path_or_buf='{}/{}/tblparticloc.csv'.format(evento,folder),index=False,encoding='utf-8')
        

