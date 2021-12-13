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
lext=ItemSource.layers[0]
lchl=ItemSource.layers[1]
layergroup={'nacional':lchl,'extranjero':lext}

def obtenerservicio(ambito):
    return layergroup[ambito]

def clasificador(json,territorio):
    inarray=[]
    for record in json:
        if record['amb']==territorio:
            inoid=record['oid']
            inids=record['ids']
            if record['ext']:        
                ter='extranjero'
            else: ter='nacional'
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
    resultados="https://www.servelelecciones.cl/data/elecciones_presidente/computo/locales/{}.json".format(idservel)
    mesas="https://www.servelelecciones.cl/data/mesas_instaladas/computo/locales/{}.json".format(idservel)
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    servicio=obtenerservicio(ambito)
    qnation=servicio.query(out_fields='ts,tPad,c12v,c22v,vv2v,vn2v,vb2v,vt2v,iM2v,pM2v,tM2v,cM2v,w2v,cv2v',return_geometry=False,object_ids=fcoid)
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
    modregister.attributes['tM2v']=0
    modregister.attributes['iM2v']=0
    modregister.attributes['pM2v']=0
    for mesa in jmesas['data']:
        modregister.attributes['tM2v']=modregister.attributes['tM2v']+1
        if mesa['b']=="Instalada":
            modregister.attributes['iM2v']=modregister.attributes['iM2v']+1
        else: modregister.attributes['pM2v']=modregister.attributes['pM2v']+1
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
    keyindex=open('elecciones/inputs/codigosfs.json')
    jkey=json.load(keyindex)
    locservel=clasificador(jkey,'locales')
    with Pool(3) as p:
        print('Analizando Locales')
        p.map(Territorial,locservel)
    print("listo")