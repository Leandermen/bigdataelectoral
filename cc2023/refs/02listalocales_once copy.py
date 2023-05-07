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
ccs=open("{}/{}/circsenat.json".format(evento,folder),encoding='utf-8')
jccs=json.load(ccs)
tabla=pd.DataFrame(columns=['local','csen'])

for key in jccs:
    k=key['c']
    d=key['d']
    urlcom="https://www.servelelecciones.cl/data/{}/filters/circ_electoral/bycirc_senatorial/{}.json".format(context_event,k)
    rpcom = requests.request("GET", urlcom, headers={}, data={})
    circelecs=json.loads(rpcom.text)
    for comunita in circelecs:
        ck=comunita['c']
        print (comunita['d'])
        urlloc="https://www.servelelecciones.cl/data/{}/filters/locales/bycirc_electoral/{}.json".format(context_event,ck)
        rplocs = requests.request("GET", urlloc, headers={}, data={})
        locales=json.loads(rplocs.text)
        for localitos in locales:
            record = {'local':localitos['c'],'csen':k}
            tabla=tabla.append(record,ignore_index=True)

tabla.to_json(path_or_buf='{}/{}/localcsen.json'.format(evento,folder),orient='records',force_ascii=False)
#tabla.to_csv(path_or_buf='{}/{}/tbllocalesservel.csv'.format(evento,folder),index=False,encoding='utf-8')
ccs.close()