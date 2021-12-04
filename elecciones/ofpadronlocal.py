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
    direccion="https://www.servelelecciones.cl/data/participacion/computo/locales/{}.json".format(key)
    response = requests.request("GET", direccion, headers={}, data={})
    r=json.loads(response.text)
    padron=int(r['resumen'][0]['c'].replace(".",""))
    vt1v=int(r['resumen'][0]['d'].replace(".",""))
    if padron != 0:
        c1v=round((vt1v/padron)*100,3)
    else: c1v=0
    fila={  'idservel':key,
            'circelec':circ,
            'mex':ex,
            'padron':padron,
            'vt1v':vt1v,
            'c1v':c1v}
    return fila

if __name__ == '__main__':
    print("Inicio Proceso")
    initialarray=[]
    ext=open('elecciones/inputs/localesext.json')
    loc=open('elecciones/inputs/localesservel.json')
    jloc=json.load(loc)
    jext=json.load(ext)
    tabla=pd.DataFrame(columns=['ts','circelec','idservel','mex','padron','vt1v','c1v'])
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
    tabla.to_csv(path_or_buf='elecciones/outputs/representacion/locales.csv',index=False,encoding='utf-8')
    ext.close()
    loc.close()
    print('Listo')
    dt2=datetime.datetime.now()
    timedelta=dt2-dt
    mins=str(round(timedelta.total_seconds()/60,3))
    print('Tiempo Elapsado: '+mins+' minutos')