import requests
from requests.sessions import Session
import json
import time
import datetime
from datetime import datetime

def clasificador(json):
    inarray=[]
    for record in json:
        #if record['amb']!='locales':        
            inoid=record['oid']
            inids=record['ids']
            ter=record['amb']
            subarray=[inoid,inids,ter]
            inarray.append(subarray)
        #else: pass
    return inarray

def sessioncrawler(uri):
    r = s.get(uri)
    jmesas=json.loads(r.text)
    print (int(jmesas['resumen'][0]['d'].replace(".","")))


s = requests.Session()
event_context='mesas_constituidas'
evento='pleb2022'
folder='lookups'
keyindex=open('{}/{}/codigosfs.json'.format(evento,folder))
jkey=json.load(keyindex)
terinput=clasificador(jkey)
urinput=[]
for uris in terinput:
    idservel=uris[1]
    ambito=uris[2]
    mesas="https://www.servelelecciones.cl/data/{}/computo/{}/{}.json".format(event_context,ambito,idservel)
    print(mesas)
    urinput.append(mesas)
#for sitio in urinput:
    #sessioncrawler(sitio)