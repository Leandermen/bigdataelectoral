#Fase 2: Instalación de Mesas (7 de mayo AM)
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import requests
import json
import time
#import datetime
import concurrent.futures
from arcgis.gis import GIS
from datetime import datetime

event_context='mesas_instaladas'
evento='cc2023'
folder='lookups'
s = requests.Session()
masterurl="https://www.servelelecciones.cl/data/{}/computo/pais/8056.json".format(event_context)
dato=1
update=0
print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemSource=gis.content.get('8f0e3c359a594606bb7a86bdd83c1971')
#Nación
tnation=ItemSource.tables[0]
#Territorios
comuchl=ItemSource.layers[1]
regichl=ItemSource.layers[2]
#Locales
naclocal=ItemSource.layers[0]

print("Inicio Consultas")
qcomu=comuchl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,COMUNA',return_geometry=False)
qregi=regichl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,NOM_CORTO',return_geometry=False)
qlocn=naclocal.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,iM,diM,idservel,LOCAL',return_geometry=False)
#a paises | b regiones |c provincias |d comunas |e extranjero |f nacional
ta,tb,tc,td,te,tf=[],[],[],[],[],[]
layergroup={'regiones':regichl,'comunas':comuchl}
querygroup={'regiones':qregi,'comunas':qcomu}
querygroupext={'nac':qlocn}

ambitos={'pais':ta,'regiones':tb,'comunas':td}

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
    r = s.get(uri,headers={"referer":"https://www.servelelecciones.cl/","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.68"})
    #print (r.status_code)
    jmesas=json.loads(r.text)
    time.sleep(0.05)
    return jmesas

def obtenerservicio(ambito,valor):
    if valor == 0:            
        return layergroup[ambito]

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
    mesas="https://www.servelelecciones.cl/data/{}/computo/pais/8056.json".format(event_context)
    #rmesas = requests.request("GET", mesas, headers={}, data={})
    rmesas = s.get(mesas)
    jmesas=json.loads(rmesas.text)
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Mesas    
    modregister.attributes['iM']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['diM']=modregister.attributes['mesas']-int(jmesas['resumen'][0]['c'].replace(".",""))
    print("Preparado Computo Global")
    tnation.edit_features(updates=[modregister])
    
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
    modregister.attributes['mesas']=int(jmesas['resumen'][0]['b'].replace(".",""))
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
    #response = requests.request("GET", masterurl, headers={}, data={})
    response = s.get(url,headers={"referer":"https://www.servelelecciones.cl/","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.68"})
    print (response.status_code)
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
            time.sleep(3)
            stime=datetime.now()
            GlobalNational(tnation)
            print("Iniciando computo territorial")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for d in terinput:
                    executor.submit(Territorial,tercore=d)
            del executor
            print("Inicio Edición")
            print("Regiones")
            regichl.edit_features(updates=tb)
            print("Comunas")
            comuchl.edit_features(updates=td)
            print("Computando Locales")
            with concurrent.futures.ThreadPoolExecutor() as executor:
                for l in locinput:
                    executor.submit(Local,localcore=l)
            del executor
            print("Edición Locales")
            naclocal.edit_features(updates=tf)
            resetglobal()
            dato=update
            etime=datetime.now()
            timedelta=etime-stime
            mins=str(round(timedelta.total_seconds()/60,3))
            print('Tiempo Elapsado: '+mins+' minutos')
        else: 
            print ("Todo Igual "+stime.strftime("%H:%M:%S"))
        time.sleep(5)