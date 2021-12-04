import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
from datetime import datetime

def GetGlobalResultados():
    dt=datetime.now()
    masterurl="https://www.servelelecciones.cl/data/elecciones_presidente/computo/global/19001.json"
    tabla=pd.DataFrame(columns=['ts','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vt','me','tm','cm','w1V'])
    response = requests.request("GET", masterurl, headers={}, data={})
    datas=json.loads(response.text)
    mE=int(datas['mesasEscrutadas'].replace(".",""))
    tM=int(datas['totalMesas'].replace(".",""))
    if tM != 0:
        cm=round((mE/tM)*100,3)
    else: cm=0
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
    outfn='elecciones/outputs/territorial/global.csv'
    tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')


def extractorTerritorial(divterritorial):
    dt=datetime.now()
    print("Iniciando Análisis")
    print(divterritorial)
    fn='elecciones/inputs/{}.json'.format(divterritorial)
    loc=open(fn,encoding='utf-8')
    jloc=json.load(loc)
    tabla=pd.DataFrame(columns=['ts','idservel','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vt','me','tm','cm','w1V'])
    for key in jloc:  
        b=key['c']
        direccion="https://www.servelelecciones.cl/data/elecciones_presidente/computo/{}/{}.json".format(divterritorial,b)
        response = requests.request("GET", direccion, headers={}, data={})
        datas=json.loads(response.text)
        mE=int(datas['mesasEscrutadas'].replace(".",""))
        tM=int(datas['totalMesas'].replace(".",""))
        if tM != 0:
            cm=round((mE/tM)*100,3)
        else: cm=0
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
                'idservel':b,
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
    print("Avance 100%") 
    dt2=datetime.now()
    timedelta=dt2-dt
    seconds=timedelta.total_seconds()
    mins=str(round(seconds/60,3))
    outfn='elecciones/outputs/territorial/{}.csv'.format(divterritorial)
    tabla.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')
    loc.close()
    print('Tiempo Elapsado: '+mins+' minutos')

if __name__ == '__main__':
    UTerritorial=['regiones','provincias','comunas','pais']
    print('Computo Global en Progreso')
    GetGlobalResultados()
    print('Computo Territorial en Progreso')
    with Pool(5) as p:
        p.map(extractorTerritorial,UTerritorial)
    print('Listo') 