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
    padron="https://www.servelelecciones.cl/data/participacion/computo/locales/{}.json".format(idservel)
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    rpadron = requests.request("GET", padron, headers={}, data={})
    jpadron=json.loads(rpadron.text)
    servicio=obtenerservicio(ambito)
    qnation=servicio.query(out_fields='ts,c11v,c21v,c31v,c41v,c51v,c61v,c71v,vv1v,vn1v,vb1v,vt1v,eM1v,tM1v,cM1v,w1V,iM1v,pM1v,tPad,cv1v,c12v,c22v,vv2v,vn2v,vb2v,vt2v,iM2v,pM2v,tM2v,cM2v,w2v,cv2v',return_geometry=False,object_ids=fcoid)
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Resultados 1V
    modregister.attributes['c11v']=int(jresults['data'][0]['c'].replace(".",""))
    modregister.attributes['c21v']=int(jresults['data'][1]['c'].replace(".",""))
    modregister.attributes['c31v']=int(jresults['data'][2]['c'].replace(".",""))
    modregister.attributes['c41v']=int(jresults['data'][3]['c'].replace(".",""))
    modregister.attributes['c51v']=int(jresults['data'][4]['c'].replace(".",""))
    modregister.attributes['c61v']=int(jresults['data'][5]['c'].replace(".",""))
    modregister.attributes['c71v']=int(jresults['data'][6]['c'].replace(".",""))
    comp={  'Boric':modregister.attributes['c11v'],
            'Kast':modregister.attributes['c21v'],
            'Provoste':modregister.attributes['c31v'],
            'Sichel':modregister.attributes['c41v'],
            'Artés':modregister.attributes['c51v'],
            'ME-O':modregister.attributes['c61v'],
            'Parisi':modregister.attributes['c71v']}
    modregister.attributes['w1V']=max(comp,key=comp.get)
    modregister.attributes['vv1v']=int(jresults['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn1v']=int(jresults['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb1v']=int(jresults['resumen'][2]['c'].replace(".",""))    
    modregister.attributes['vt1v']=modregister.attributes['vv1v']+modregister.attributes['vn1v']+modregister.attributes['vb1v']
    #Mesas    
    modregister.attributes['tM1v']=0
    modregister.attributes['iM1v']=0
    modregister.attributes['pM1v']=0
    for mesa in jmesas['data']:
        modregister.attributes['tM1v']=modregister.attributes['tM1v']+1
        if mesa['b']=="Instalada":
            modregister.attributes['iM1v']=modregister.attributes['iM1v']+1
        else: modregister.attributes['pM1v']=modregister.attributes['pM1v']+1
    modregister.attributes['eM1v']=int(jresults['mesasEscrutadas'].replace(".",""))
    if modregister.attributes['tM1v'] != 0:
        modregister.attributes['cM1v']=round((modregister.attributes['iM1v']/modregister.attributes['tM1v'])*100,3)
    else: modregister.attributes['cM1v']=0
    #Participación
    modregister.attributes['tPad']=int(jpadron['resumen'][0]['c'].replace(".",""))
    if modregister.attributes['tPad'] != 0:
        modregister.attributes['cv1v']=round((modregister.attributes['vt1v']/modregister.attributes['tPad'])*100,3)
    else: modregister.attributes['cv1v']=0
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