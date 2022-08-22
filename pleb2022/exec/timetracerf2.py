#Fase 2: Instalación de Mesas (4 de septiembre)
import requests
import json
import time
import datetime
import concurrent.futures
from arcgis.gis import GIS
from datetime import datetime

event_context='mesas_instaladas'
evento='pleb2022'
folder='lookups'
s = requests.Session()
masterurl="https://www.servelelecciones.cl/data/{}/computo/global/19001.json".format(event_context)
dato=1
update=0
print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemSource=gis.content.get('dacbfde953734f3f9a3c9e604b89dfb1')
ItemTS=gis.content.get('1cde3f8c703c457c9d7f91f8af42a004')
#Nación
ttsnation=ItemTS.tables[0]
tnation=ItemSource.tables[0]
#Territorios
ttspais=ItemTS.tables[1]
ttsregi=ItemTS.tables[2]
ttsprov=ItemTS.tables[3]
ttscomu=ItemTS.tables[4]
paisext=ItemSource.layers[5]
comuchl=ItemSource.layers[4]
provchl=ItemSource.layers[3]
regichl=ItemSource.layers[2]
#Locales
ttslocext=ItemTS.tables[6]
ttslocnac=ItemTS.tables[5]
extlocal=ItemSource.layers[1]
naclocal=ItemSource.layers[0]

print("Inicio Consultas")
qpext=paisext.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,NAME_ES',return_geometry=False)
qcomu=comuchl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,COMUNA',return_geometry=False)
qprov=provchl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,NOM_PROV',return_geometry=False)
qregi=regichl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,NOM_CORTO',return_geometry=False)
qlocn=naclocal.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,LOCAL',return_geometry=False)
qloce=extlocal.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,n_cce',return_geometry=False)
#a paises | b regiones |c provincias |d comunas |e extranjero |f nacional
ta,tb,tc,td,te,tf=[],[],[],[],[],[]
layergroup={'pais':paisext,'regiones':regichl,'provincias':provchl,'comunas':comuchl}
ttsgroup={'pais':ttspais,'regiones':ttsregi,'provincias':ttsprov,'comunas':ttscomu}
querygroup={'pais':qpext,'regiones':qregi,'provincias':qprov,'comunas':qcomu}
querygroupext={'nac':qlocn,'ext':qloce}

ambitos={'pais':ta,'regiones':tb,'provincias':tc,'comunas':td}

print("Inicio Funciones")
def obtenerquery(ambito):
    return querygroup[ambito]

def obtenerqueryext(ext):
    if ext: return querygroupext['ext']
    else: return querygroupext['nac']



def resetglobal():
    global ta
    global tb
    global tc
    global td
    global te
    global tf
    ta,tb,tc,td,te,tf=[],[],[],[],[],[]

def sessioncrawler(uri):
    r = s.get(uri)
    jmesas=json.loads(r.text)
    return jmesas

def obtenerservicio(ambito,valor):
    if valor == 0:            
        return layergroup[ambito]
    else: return ttsgroup[ambito]

def asignador(dato,ambito,indext):
    global ta
    global tb
    global tc
    global td
    global te
    global tf
    if ambito == 'pais':
        ta.append(dato)
    elif ambito == 'regiones':
        tb.append(dato)
    elif ambito == 'provincias':
        tc.append(dato)
    elif ambito == 'comunas':
        td.append(dato)
    elif ambito == 'locales':
        if indext:
            te.append(dato)
        else: tf.append(dato)

def clasificador(json,ambito):
    inarray=[]
    if ambito:
        for record in json:
            if record['amb']!='locales':        
                inoid=record['oid']
                inids=record['ids']
                ter=record['amb']
                subarray=[inoid,inids,ter]
                inarray.append(subarray)
            else: pass
    else:
        for record in json:
            if record['amb']=='locales':        
                inoid=record['oid']
                inids=record['ids']
                ter=record['amb']
                ext=record['ext']
                subarray=[inoid,inids,ter,ext]
                inarray.append(subarray)
            else: pass
    return inarray

#Global
def GlobalNational(mainnational):
    #Timestamps
    print('Actualizando Mesas Instaladas a Nivel Nacional')
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/{}/computo/global/19001.json".format(event_context)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['iM']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['diM']=modregister.attributes['mesas']-int(jmesas['resumen'][0]['c'].replace(".",""))
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
    jmesas=sessioncrawler(mesas)
    qnation=obtenerquery(ambito)
    modregister=[f for f in qnation if f.attributes['OBJECTID']==fcoid][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['iM']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['diM']=modregister.attributes['mesas']-int(jmesas['resumen'][0]['c'].replace(".",""))
    asignador(modregister,ambito,False)

def Local(localcore):
    fcoid=localcore[0]
    idservel=localcore[1]
    ambito=localcore[2]
    indext=localcore[3]
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/{}/computo/{}/{}.json".format(event_context,ambito,idservel)
    jmesas=sessioncrawler(mesas)
    qnation=obtenerqueryext(indext)
    modregister=[f for f in qnation if f.attributes['OBJECTID']==fcoid][0]
    modregister.attributes['ts']=dt
    #Mesas
    mc=0
    mnc=0
    for mesa in jmesas['data']:
        if mesa['b']=="No Instalada":
            mnc=mnc+1
        else: mc=mc+1
    modregister.attributes['iM']=mc
    modregister.attributes['diM']=mnc
    asignador(modregister,ambito,indext)


def CheckNovedad(url):
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    mesasinst=int(r['resumen'][0]['c'].replace(".",""))
    return mesasinst

if __name__ == '__main__':
    print("Inicio Main Program")
    keyindex=open('{}/{}/codigosfs.json'.format(evento,folder))
    jkey=json.load(keyindex)
    terinput=clasificador(jkey,True)
    locinput=clasificador(jkey,False)
    while True:
        update=CheckNovedad(masterurl)
        if update!=dato:
            stime=datetime.now()
            GlobalNational(tnation)
            print("Iniciando computo territorial")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for d in terinput:
                    executor.submit(Territorial,tercore=d)
            del executor
            print("Inicio Edición")
            print("Paises Extranjeros")
            paisext.edit_features(updates=ta)
            ttspais.edit_features(adds=ta)
            print("Regiones")
            regichl.edit_features(updates=tb)
            ttsregi.edit_features(adds=tb)
            print("Provincias")
            provchl.edit_features(updates=tc)
            ttsprov.edit_features(adds=tc)
            print("Comunas")
            comuchl.edit_features(updates=td)
            ttscomu.edit_features(adds=td)
            print("Locales")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for l in locinput:
                    executor.submit(Local,localcore=l)
            del executor
            print("Edición Locales")
            naclocal.edit_features(updates=tf)
            extlocal.edit_features(updates=te)
            ttslocext.edit_features(adds=te)
            ttslocnac.edit_features(adds=tf)
            resetglobal()
            dato=update
            etime=datetime.now()
            timedelta=etime-stime
            mins=str(round(timedelta.total_seconds()/60,3))
            print('Tiempo Elapsado: '+mins+' minutos')
        else: 
            print ("Todo Igual")
        time.sleep(5)