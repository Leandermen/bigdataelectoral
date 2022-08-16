#Fase 1: Constitución de Mesas (3 de septiembre)
import requests
import json
import time
import datetime
from multiprocessing import Pool
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from datetime import datetime

masterurl="https://www.servelelecciones.cl/data/mesas_instaladas/computo/global/19001.json"
dato=1
update=0

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
ItemTS=gis.content.get('39107a24dd2847b2b3c41f1fe594c354')
ttsnation=ItemTS.tables[0]
ttspais=ItemTS.tables[1]
ttsregi=ItemTS.tables[5]
ttsprov=ItemTS.tables[4]
ttscomu=ItemTS.tables[2]
tnation=ItemSource.tables[0]
paisext=ItemSource.layers[2]
comuchl=ItemSource.layers[3]
provchl=ItemSource.layers[4]
regichl=ItemSource.layers[5]
layergroup={'pais':paisext,'regiones':regichl,'provincias':provchl,'comunas':comuchl}
ttsgroup={'pais':ttspais,'regiones':ttsregi,'provincias':ttsprov,'comunas':ttscomu}

def obtenerservicio(ambito,valor):
    if valor == 0:            
        return layergroup[ambito]
    else: return ttsgroup[ambito]

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

#Global
def GlobalNational(mainnational):
    #Timestamps
    print('Actualizando Mesas a Nivel Nacional')
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/mesas_instaladas/computo/global/19001.json"
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['tM2v']=int(jmesas['resumen'][0]['b'].replace(".",""))
    modregister.attributes['iM2v']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['pM2v']=int(jmesas['resumen'][0]['d'].replace(".",""))
    if modregister.attributes['tM2v'] != 0:
        modregister.attributes['cM2v']=round((modregister.attributes['iM2v']/modregister.attributes['tM2v'])*100,3)
    else: modregister.attributes['cM2v']=0
    print("Preparado")
    tnation.edit_features(updates=[modregister])
    ttsnation.edit_features(adds=[modregister])

def Territorial(tercore):
    fcoid=tercore[0]
    idservel=tercore[1]
    ambito=tercore[2]
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/mesas_instaladas/computo/{}/{}.json".format(ambito,idservel)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    servicio=obtenerservicio(ambito,0)
    timetable=obtenerservicio(ambito,1)
    qnation=servicio.query(out_fields='*',return_geometry=False,object_ids=fcoid)
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['tM2v']=int(jmesas['resumen'][0]['b'].replace(".",""))
    modregister.attributes['iM2v']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['pM2v']=int(jmesas['resumen'][0]['d'].replace(".",""))
    print("Preparado "+str(idservel))
    servicio.edit_features(updates=[modregister])
    timetable.edit_features(adds=[modregister])
    time.sleep(0.2)


def CheckNovedad(url):
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    mesasinst=int(r['resumen'][0]['c'].replace(".",""))
    return mesasinst

if __name__ == '__main__':
    keyindex=open('elecciones/inputs/codigosfs.json')
    jkey=json.load(keyindex)
    terinput=clasificador(jkey)
    while True:
        update=CheckNovedad(masterurl)
        if update!=dato:
            stime=datetime.now()
            GlobalNational(tnation)
            with Pool(4) as p:
                p.map(Territorial,terinput)
            dato=update
            etime=datetime.now()
            timedelta=etime-stime
            mins=str(round(timedelta.total_seconds()/60,3))
            print('Tiempo Elapsado: '+mins+' minutos')
        else: print ("Todo Igual")
        time.sleep(5)