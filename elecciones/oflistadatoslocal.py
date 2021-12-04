import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
import datetime
import arcgis

def getServelloc(input):
    key=input[0]
    ex=input[1]
    circ=input[2]
    direccion="https://www.servelelecciones.cl/data/elecciones_presidente/computo/locales/{}.json".format(key)
    response = requests.request("GET", direccion, headers={}, data={})
    datas=json.loads(response.text)
    mE=int(datas['mesasEscrutadas'].replace(".",""))
    tM=int(datas['totalMesas'].replace(".",""))
    if tM != 0:
        cm=round((mE/tM)*100,2)
    else: cm=0
    mex=ex
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
    fila={  'idservel':key,
            'circelec':circ,
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
    return fila

if __name__ == '__main__':
    print("Inicio Proceso")
    initialarray=[]
    ext=open('elecciones/inputs/localesext.json')
    loc=open('elecciones/inputs/localesservel.json')
    jloc=json.load(loc)
    jext=json.load(ext)
    tabla=pd.DataFrame(columns=['ts','circelec','idservel','mex','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vt','me','tm','cm','w1V'])
    dt = datetime.datetime.now()
    dt2=datetime.datetime.now()

    print('Generación de Arreglo de Llaves')
    for key in jext:
        subarray=[]
        a=key['idservel']   
        b=True
        c=key['c_circ']         
        subarray.append(a)
        subarray.append(b)
        subarray.append(c)
        initialarray.append(subarray)
    
    for key in jloc:
        subarray=[]
        a=key['idservel']   
        b=False
        c=key['circelec']         
        subarray.append(a)
        subarray.append(b)
        subarray.append(c)
        initialarray.append(subarray)

    #Prueba de Fuego
    print('Iniciando Cosecha')
    
    with Pool(12) as ptest:
        results=ptest.map(getServelloc,initialarray)
    timedelta=datetime.datetime.now()-dt
    mins=str(round(timedelta.total_seconds()/60,3))
    print('Tiempo Elapsado: '+mins+' minutos')
    print('Preparando Tabla') 

    for r in results:
        r['ts']=dt
        tabla=tabla.append(r,ignore_index=True)
    dt2=datetime.datetime.now()
    timedelta=dt2-dt
    mins=str(round(timedelta.total_seconds()/60,3))
    print('Tiempo Elapsado: '+mins+' minutos')
    print('Escribiendo Salida')
    tabla.to_csv(path_or_buf='elecciones/outputs/territorial/locales.csv',index=False,encoding='utf-8')
    ext.close()
    loc.close()
    print('Listo')
    dt2=datetime.datetime.now()
    timedelta=dt2-dt
    mins=str(round(timedelta.total_seconds()/60,3))
    print('Tiempo Elapsado: '+mins+' minutos')