import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
import datetime
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from datetime import datetime

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
#ItemTS=gis.content.get('39107a24dd2847b2b3c41f1fe594c354')
paisext=ItemSource.layers[2]
comuchl=ItemSource.layers[3]
provchl=ItemSource.layers[4]
regichl=ItemSource.layers[5]
layergroup={'pais':paisext,'regiones':regichl,'provincias':provchl,'comunas':comuchl}

def obtenerservicio(ambito):
    return layergroup[ambito]

def clasificador(json):
    inarray=[]
    for record in json:
        if record['amb']!='locales':        
            inoid=record['oid']
            inids=record['ids']
            ter=record['amb']
            subarray=[inoid,inids,ter]
            inarray.append(subarray)
        else: pass
    return inarray

def Territorial(tercore):
    fcoid=tercore[0]
    idservel=tercore[1]
    ambito=tercore[2]
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    resultados="https://www.servelelecciones.cl/data/elecciones_presidente/computo/{}/{}.json".format(ambito,idservel)
    mesas="https://www.servelelecciones.cl/data/mesas_instaladas/computo/{}/{}.json".format(ambito,idservel)
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    servicio=obtenerservicio(ambito)
    qnation=servicio.query(out_fields='*',return_geometry=False,object_ids=fcoid)
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Resultados 2V
    modregister.attributes['c12v']=int(jresults['data'][0]['c'].replace(".",""))
    modregister.attributes['c22v']=int(jresults['data'][1]['c'].replace(".",""))
    comp={  'Boric':modregister.attributes['c12v'],
            'Kast':modregister.attributes['c22v']
            }
    if (int(jresults['resumen'][0]['c'].replace(".","")) != 0)and(modregister.attributes['c12v']!=modregister.attributes['c22v']):
        modregister.attributes['w2v']=max(comp,key=comp.get)
    else: modregister.attributes['w2v']='Empate'
    modregister.attributes['vv2v']=int(jresults['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn2v']=int(jresults['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb2v']=int(jresults['resumen'][2]['c'].replace(".",""))    
    modregister.attributes['vt2v']=modregister.attributes['vv2v']+modregister.attributes['vn2v']+modregister.attributes['vb2v']
    #Mesas    
    modregister.attributes['tM2v']=int(jmesas['resumen'][0]['b'].replace(".",""))
    modregister.attributes['iM2v']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['pM2v']=int(jmesas['resumen'][0]['d'].replace(".",""))
    modregister.attributes['eM2v']=int(jresults['mesasEscrutadas'].replace(".",""))
    if modregister.attributes['tM2v'] != 0:
        modregister.attributes['cM2v']=round((modregister.attributes['iM2v']/modregister.attributes['tM2v'])*100,3)
    else: modregister.attributes['cM2v']=0
    #Participación
    if modregister.attributes['tPad'] != 0:
        modregister.attributes['cv2v']=round((modregister.attributes['vt2v']/modregister.attributes['tPad'])*100,3)
    else: modregister.attributes['cv2v']=0
    print("Preparado "+str(idservel))
    servicio.edit_features(updates=[modregister])
    
    
if __name__ == '__main__':
    print('Inicio de Edición')
    stime=datetime.now()
    keyindex=open('elecciones/inputs/codigosfs.json')
    jkey=json.load(keyindex)
    terinput=clasificador(jkey)
    with Pool(24) as p:
        print('Analizando Territorios')
        p.map(Territorial,terinput)
    etime=datetime.now()
    timedelta=etime-stime
    mins=str(round(timedelta.total_seconds()/60,3))
    print('Tiempo Elapsado: '+mins+' minutos')
    print("listo")