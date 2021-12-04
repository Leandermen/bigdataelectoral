import requests
import json
import pandas as pd
import time
from datetime import datetime
import csv
from csv import DictWriter
from csv import writer

ext=open('elecciones/outputs/localesext.json')
loc=open('elecciones/outputs/localesservel.json')
jloc=json.load(loc)
jext=json.load(ext)
tabla=pd.DataFrame(columns=['ts','circelec','idservel','mex','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vt','me','tm','cm','w1V'])
i=0
q=1
dt=datetime.now()

for key in jloc:
    i=i+1
    a=key['circelec']   
    b=key['idservel']
    direccion="https://www.servelelecciones.cl/data/elecciones_presidente/computo/locales/{}.json".format(b)
    payload={}
    headers = {}
    response = requests.request("GET", direccion, headers=headers, data=payload)
    datas=json.loads(response.text)
    mE=int(datas['mesasEscrutadas'].replace(".",""))
    tM=int(datas['totalMesas'].replace(".",""))
    if tM != 0:
        cm=round((mE/tM)*100,2)
    else: cm=0
    mex=False
    boric=int(datas['data'][0]['c'].replace(".",""))
    kast=int(datas['data'][1]['c'].replace(".",""))
    provoste=int(datas['data'][2]['c'].replace(".",""))
    sichel=int(datas['data'][3]['c'].replace(".",""))
    artes=int(datas['data'][4]['c'].replace(".",""))
    enriquez=int(datas['data'][5]['c'].replace(".",""))
    parisi=int(datas['data'][6]['c'].replace(".",""))
    comp={  'Boric':boric,
            'Kast':kast,
            'Provoste':provoste,
            'Sichel':sichel,
            'Artés':artes,
            'ME-O':enriquez,
            'Parisi':parisi}
    winner=max(comp,key=comp.get)
    vve=int(datas['resumen'][0]['c'].replace(".",""))
    vnu=int(datas['resumen'][1]['c'].replace(".",""))
    vbl=int(datas['resumen'][2]['c'].replace(".",""))
    tv=vve+vnu+vbl 
    fila={  'ts':dt,
            'circelec':a,
            'idservel':b,
            'mex':mex,
            'c1':boric,
            'c2':kast,
            'c3':provoste,
            'c4':sichel,
            'c5':artes,
            'c6':enriquez,
            'c7':parisi,
            'vv':vve,
            'vn':vnu,
            'vb':vbl,
            'vt':tv,
            'me':mE,
            'tm':tM,
            'cm':cm,
            'w1V':winner}
    tabla=tabla.append(fila,ignore_index=True)
    prog=i/2810
    if prog > 0.25 and q==1:
        print("Avance 25%")
        q=2
    if prog > 0.5 and q==2:
        print("Avance 50%")
        q=3
    if prog > 0.75 and q==3:
        print("Avance 75%")
        q=4
    #if i==25: break

print("Avance 100%")

for key in jext:
    i=i+1
    a=key['c_circ']   
    b=key['idservel']
    direccion="https://www.servelelecciones.cl/data/elecciones_presidente/computo/locales/{}.json".format(b)
    payload={}
    headers = {}
    response = requests.request("GET", direccion, headers={}, data={})
    datas=json.loads(response.text)
    mE=int(datas['mesasEscrutadas'].replace(".",""))
    tM=int(datas['totalMesas'].replace(".",""))
    if tM != 0:
        cm=round((mE/tM)*100,2)
    else: cm=0
    mex=True
    boric=int(datas['data'][0]['c'].replace(".",""))
    kast=int(datas['data'][1]['c'].replace(".",""))
    provoste=int(datas['data'][2]['c'].replace(".",""))
    sichel=int(datas['data'][3]['c'].replace(".",""))
    artes=int(datas['data'][4]['c'].replace(".",""))
    enriquez=int(datas['data'][5]['c'].replace(".",""))
    parisi=int(datas['data'][6]['c'].replace(".",""))
    comp={  'Boric':boric,
            'Kast':kast,
            'Provoste':provoste,
            'Sichel':sichel,
            'Artés':artes,
            'ME-O':enriquez,
            'Parisi':parisi}
    winner=max(comp,key=comp.get)
    vve=int(datas['resumen'][0]['c'].replace(".",""))
    vnu=int(datas['resumen'][1]['c'].replace(".",""))
    vbl=int(datas['resumen'][2]['c'].replace(".",""))
    tv=vve+vnu+vbl 
    fila={  'ts':dt,
            'circelec':a,
            'idservel':b,
            'mex':mex,
            'c1':boric,
            'c2':kast,
            'c3':provoste,
            'c4':sichel,
            'c5':artes,
            'c6':enriquez,
            'c7':parisi,
            'vv':vve,
            'vn':vnu,
            'vb':vbl,
            'vt':tv,
            'me':mE,
            'tm':tM,
            'cm':cm,
            'w1V':winner}
    tabla=tabla.append(fila,ignore_index=True)
    prog=i/248
    if prog > 0.25 and q==1:
        print("Avance 25%")
        q=2
    if prog > 0.5 and q==2:
        print("Avance 50%")
        q=3
    if prog > 0.75 and q==3:
        print("Avance 75%")
        q=4

print("Avance 100%") 



dt2=datetime.now()
timedelta=dt2-dt
seconds=timedelta.total_seconds()
mins=str(round(seconds/60,3))
tabla.to_csv(path_or_buf='elecciones/outputs/datalocales.csv',index=False,encoding='utf-8')
loc.close()
print('Tiempo Elapsado: '+mins+' minutos')