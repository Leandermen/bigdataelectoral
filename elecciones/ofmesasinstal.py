import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
from datetime import datetime

#Global
def GetGlobalMesas():
    dt=datetime.now()
    masterurl="https://www.servelelecciones.cl/data/mesas_instaladas/computo/global/19001.json"
    tabla=pd.DataFrame(columns=['ts','tM','iM','pM','cM'])
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    ts=dt
    tM=int(r['resumen'][0]['b'].replace(".",""))
    iM=int(r['resumen'][0]['c'].replace(".",""))
    pM=int(r['resumen'][0]['d'].replace(".",""))
    if tM != 0:
        cM=round((iM/tM)*100,3)
    else: cM=0
    print(cM)
    fila={
        'ts':ts,
        'tM':tM,
        'iM':iM,
        'pM':pM,
        'cM':cM,
    }
    #Cambiar por envío a servicio y hacer return de fila
    tabla=tabla.append(fila,ignore_index=True)
    outfn='elecciones/outputs/mesas/global.csv'
    tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')

#Territorial
def GetTerritorialMesas(divterritorial):
    print('Analizando: '+divterritorial)
    dt=datetime.now()
    tabla=pd.DataFrame(columns=['ts','idservel','tM','iM','pM','cM'])
    fn='elecciones/inputs/{}.json'.format(divterritorial)
    loc=open(fn,encoding='utf-8')
    jloc=json.load(loc)
    for key in jloc:  
        b=key['c']
        terurl="https://www.servelelecciones.cl/data/mesas_instaladas/computo/{}/{}.json".format(divterritorial,b)
        response = requests.request("GET", terurl, headers={}, data={})
        r=json.loads(response.text)
        ts=dt
        tM=int(r['resumen'][0]['b'].replace(".",""))
        iM=int(r['resumen'][0]['c'].replace(".",""))
        pM=int(r['resumen'][0]['d'].replace(".",""))
        if tM != 0:
            cM=round((iM/tM)*100,3)
        else: cM=0
        fila={
            'ts':ts,
            'idservel':b,
            'tM':tM,
            'iM':iM,
            'pM':pM,
            'cM':cM
        }
        #Cambiar por envío a servicio y hacer return de fila
        tabla=tabla.append(fila,ignore_index=True)
        outfn='elecciones/outputs/mesas/{}.csv'.format(divterritorial)
        tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')
    print('Finalizado: '+divterritorial)
    loc.close()

if __name__ == '__main__':
    UTerritorial=['regiones','provincias','comunas','pais']
    print('Computo Global en Progreso')
    GetGlobalMesas()
    print('Computo Territorial en Progreso')
    with Pool(4) as p:
        p.map(GetTerritorialMesas,UTerritorial)
    print('Listo')    