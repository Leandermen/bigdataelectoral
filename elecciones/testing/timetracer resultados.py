#Fase 1: Instalación de Mesas
import requests
import json
import time
import datetime
from multiprocessing import Pool
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from datetime import datetime

masterurl="https://www.servelelecciones.cl/data/elecciones_presidente/computo/global/19001.json"
cr = requests.request("GET", masterurl, headers={}, data={})
jr=json.loads(cr.text)
dato=int(jr['mesasEscrutadas'].replace(".",""))
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
    resultados="https://www.servelelecciones.cl/data/elecciones_presidente/computo/global/19001.json"
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    #Consulta Servicio    
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Resultados 2V
    modregister.attributes['c12v']=int(jresults['data'][0]['c'].replace(".",""))
    modregister.attributes['c22v']=int(jresults['data'][1]['c'].replace(".",""))
    comp={  'Boric':modregister.attributes['c12v'],
            'Kast':modregister.attributes['c22v']}
    if (int(jresults['resumen'][0]['c'].replace(".","")) != 0)and(modregister.attributes['c12v']!=modregister.attributes['c22v']):
        modregister.attributes['w2v']=max(comp,key=comp.get)
    else: modregister.attributes['w2v']='Empate'
    modregister.attributes['vv2v']=int(jresults['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn2v']=int(jresults['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb2v']=int(jresults['resumen'][2]['c'].replace(".",""))    
    modregister.attributes['vt2v']=modregister.attributes['vv2v']+modregister.attributes['vn2v']+modregister.attributes['vb2v']
    #Mesas Escrutadas    
    modregister.attributes['eM2v']=int(jresults['mesasEscrutadas'].replace(".",""))
    if modregister.attributes['tM2v'] != 0:
        modregister.attributes['cM2v']=round((modregister.attributes['eM2v']/modregister.attributes['tM2v'])*100,3)
    else: modregister.attributes['cM2v']=0
    if modregister.attributes['tPad'] != 0:
        modregister.attributes['cv2v']=round((modregister.attributes['vt2v']/modregister.attributes['tPad'])*100,3)
    else: modregister.attributes['cv2v']=0
    print("Preparado Dato Global")
    tnation.edit_features(updates=[modregister])
    ttsnation.edit_features(adds=[modregister])

def Territorial(tercore):
    fcoid=tercore[0]
    idservel=tercore[1]
    ambito=tercore[2]
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    resultados="https://www.servelelecciones.cl/data/elecciones_presidente/computo/{}/{}.json".format(ambito,idservel)
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    #Consulta Servicio
    servicio=obtenerservicio(ambito,0)
    timetable=obtenerservicio(ambito,1)
    #Query para modificar
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
    modregister.attributes['eM2v']=int(jresults['mesasEscrutadas'].replace(".",""))
    if modregister.attributes['tM2v'] != 0:
        modregister.attributes['cM2v']=round((modregister.attributes['eM2v']/modregister.attributes['tM2v'])*100,3)
    else: modregister.attributes['cM2v']=0
    #Participación
    if modregister.attributes['tPad'] != 0:
        modregister.attributes['cv2v']=round((modregister.attributes['vt2v']/modregister.attributes['tPad'])*100,3)
    else: modregister.attributes['cv2v']=0
    print("Preparado Territorio: "+str(idservel))
    servicio.edit_features(updates=[modregister])
    timetable.edit_features(adds=[modregister])
    time.sleep(0.1)

def CheckNovedad(url):
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    mesasescr=int(r['mesasEscrutadas'].replace(".",""))
    return mesasescr

if __name__ == '__main__':
    keyindex=open('elecciones/inputs/codigosfs.json')
    jkey=json.load(keyindex)
    terinput=clasificador(jkey)
    while True:
        update=CheckNovedad(masterurl)
        ctime=datetime.now()
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
        else: print ("Todo Igual "+ctime.strftime("%H:%M:%S"))
        time.sleep(5)