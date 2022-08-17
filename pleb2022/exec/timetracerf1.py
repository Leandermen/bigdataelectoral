#Fase 1: Constitución de Mesas (3 de septiembre)
import requests
import json
import time
import datetime
from multiprocessing import Pool
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from datetime import datetime

event_context='mesas_constituidas'
evento='pleb2022'
folder='lookups'

masterurl="https://www.servelelecciones.cl/data/{}/computo/global/19001.json".format(event_context)
dato=1
update=0

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
#ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
#ItemTS=gis.content.get('39107a24dd2847b2b3c41f1fe594c354')
ItemSource=gis.content.get('dacbfde953734f3f9a3c9e604b89dfb1')
ItemTS=gis.content.get('1cde3f8c703c457c9d7f91f8af42a004')
ttsnation=ItemTS.tables[0]
ttspais=ItemTS.tables[1]
ttsregi=ItemTS.tables[2]
ttsprov=ItemTS.tables[3]
ttscomu=ItemTS.tables[4]
tnation=ItemSource.tables[0]
paisext=ItemSource.layers[5]
comuchl=ItemSource.layers[4]
provchl=ItemSource.layers[3]
regichl=ItemSource.layers[2]
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
    print('Actualizando Mesas Constituidas a Nivel Nacional')
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/{}/computo/global/19001.json".format(event_context)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['cM']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['dcM']=modregister.attributes['mesas']-int(jmesas['resumen'][0]['c'].replace(".",""))
    print("Preparado Computo Global")
    tnation.edit_features(updates=[modregister])
    ttsnation.edit_features(adds=[modregister])

def Territorial(tercore):
    fcoid=tercore[0]
    idservel=tercore[1]
    ambito=tercore[2]
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/{}/computo/{}/{}.json".format(event_context,ambito,idservel)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    servicio=obtenerservicio(ambito,0)
    timetable=obtenerservicio(ambito,1)
    qnation=servicio.query(out_fields='*',return_geometry=False,object_ids=fcoid)
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['cM']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['dcM']=modregister.attributes['mesas']-int(jmesas['resumen'][0]['c'].replace(".",""))
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
    keyindex=open('{}/{}/codigosfs.json'.format(evento,folder))
    jkey=json.load(keyindex)
    terinput=clasificador(jkey)
    while True:
        update=CheckNovedad(masterurl)
        if update!=dato:
            stime=datetime.now()
            GlobalNational(tnation)
            with Pool(3) as p:
                p.map(Territorial,terinput)
            dato=update
            etime=datetime.now()
            timedelta=etime-stime
            mins=str(round(timedelta.total_seconds()/60,3))
            print('Tiempo Elapsado: '+mins+' minutos')
        else: print ("Todo Igual")
        time.sleep(5)