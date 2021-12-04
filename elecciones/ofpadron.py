import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
from datetime import datetime

#Global
def GetGlobalPadron():
    dt=datetime.now()
    masterurl="https://www.servelelecciones.cl/data/participacion/computo/global/19001.json"
    tabla=pd.DataFrame(columns=['ts','padron','vt1v','c1v'])
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    ts=dt
    padron=int(r['resumen'][0]['c'].replace(".",""))
    vt1v=int(r['resumen'][0]['d'].replace(".",""))
    if padron != 0:
        c1v=round((vt1v/padron)*100,3)
    else: c1v=0
    print(c1v)
    fila={
        'ts':ts,
        'padron':padron,
        'vt1v':vt1v,
        'c1v':c1v
    }
    #Cambiar por envío a servicio y hacer return de fila
    tabla=tabla.append(fila,ignore_index=True)
    outfn='elecciones/outputs/representacion/global.csv'
    tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')

#Territorial
def GetTerritorialPadron(divterritorial):
    print('Analizando: '+divterritorial)
    dt=datetime.now()
    tabla=pd.DataFrame(columns=['ts','idservel','padron','vt1v','c1v'])
    fn='elecciones/inputs/{}.json'.format(divterritorial)
    loc=open(fn,encoding='utf-8')
    jloc=json.load(loc)
    for key in jloc:  
        b=key['c']
        terurl="https://www.servelelecciones.cl/data/participacion/computo/{}/{}.json".format(divterritorial,b)
        response = requests.request("GET", terurl, headers={}, data={})
        r=json.loads(response.text)
        ts=dt
        padron=int(r['resumen'][0]['c'].replace(".",""))
        vt1v=int(r['resumen'][0]['d'].replace(".",""))
        if padron != 0:
            c1v=round((vt1v/padron)*100,3)
        else: c1v=0
        fila={
            'ts':ts,
            'idservel':b,
            'padron':padron,
            'vt1v':vt1v,
            'c1v':c1v
        }
        #Cambiar por envío a servicio y hacer return de fila
        tabla=tabla.append(fila,ignore_index=True)
        outfn='elecciones/outputs/representacion/{}.csv'.format(divterritorial)
        tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')
    print('Finalizado: '+divterritorial)
    loc.close()

if __name__ == '__main__':
    UTerritorial=['regiones','provincias','comunas','pais']
    print('Computo Global en Progreso')
    GetGlobalPadron()
    print('Computo Territorial en Progreso')
    with Pool(5) as p:
        p.map(GetTerritorialPadron,UTerritorial)
    print('Listo')    