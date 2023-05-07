#Fase 3: Resultados Electorales (4 de septiembre-18:00)
import requests
import json
import time
#import datetime
import concurrent.futures
from arcgis.gis import GIS
from datetime import datetime

event_context='elecciones_consejo_gen'
evento='cc2023'
folder='lookups'
s = requests.Session()
masterurl="https://www.servelelecciones.cl/data/{}/computo/pais/8056.json".format(event_context)
dato=1
update=0
print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemSource=gis.content.get('8f0e3c359a594606bb7a86bdd83c1971')
ItemTS=gis.content.get('9d294991f5b6457aa8ba7a2dce7498f5')
#Nación
ttsnation=ItemTS.tables[0]
tnation=ItemSource.tables[0]
#Territorios
comuchl=ItemSource.layers[1]
regichl=ItemSource.layers[2]
#Locales
naclocal=ItemSource.layers[0]

print("Inicio Consultas")
qcomu=comuchl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart,idservel,COMUNA',return_geometry=False)
qregi=regichl.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart,idservel,NOM_CORTO',return_geometry=False)
qlocn=naclocal.query(out_fields='OBJECTID,mesas,padron,ts,cM,dcM,opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart,idservel,LOCAL',return_geometry=False)
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

def asignaOpcion(texto):
    if texto == "A. ":
        return 'opc1'
    elif texto == "B. ":
        return 'opc2'
    elif texto == "C. ":
        return 'opc3'
    elif texto == "D. ":
        return 'opc4'
    elif texto == "E. ":
        return 'opc5'
    else: return 'opc6'


#Global
def GlobalNational(mainnational):
    #Timestamps
    print('Actualizando Resultados a Nivel Nacional')
    dt=datetime.now()
    #Obtención de Jsons
    mesas="https://www.servelelecciones.cl/data/{}/computo/pais/8056.json".format(event_context)
    #rmesas = requests.request("GET", mesas, headers={}, data={})
    rmesas = s.get(mesas)
    jmesas=json.loads(rmesas.text)
    qnation=mainnational.query(out_fields='*')
    modregister=[f for f in qnation][0]
    modregister.attributes['ts']=dt
    #Votos
    #opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart   
    modregister.attributes['opc1']=int(jmesas['data'][0]['c'].replace(".",""))
    modregister.attributes['opc2']=int(jmesas['data'][1]['c'].replace(".",""))
    modregister.attributes['opc3']=int(jmesas['data'][2]['c'].replace(".",""))
    modregister.attributes['opc4']=int(jmesas['data'][3]['c'].replace(".",""))
    modregister.attributes['opc5']=int(jmesas['data'][4]['c'].replace(".",""))
    modregister.attributes['opc6']=int(jmesas['data'][5]['c'].replace(".",""))
    comp={  'A':modregister.attributes['opc1'],
            'B':modregister.attributes['opc2'],
            'C':modregister.attributes['opc3'],
            'D':modregister.attributes['opc4'],
            'E':modregister.attributes['opc5'],
            'I':modregister.attributes['opc6'],
            }
    if (int(jmesas['resumen'][0]['c'].replace(".","")) != 0):
        maximo = [k for k, v in comp.items() if v == max(comp.values())]
        if len(maximo)==1:
            modregister.attributes['win']=maximo
        else: modregister.attributes['win']='T'    
    else: modregister.attributes['win']='T'
    modregister.attributes['vv']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn']=int(jmesas['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb']=int(jmesas['resumen'][2]['c'].replace(".",""))
    modregister.attributes['vt']=modregister.attributes['vv']+modregister.attributes['vn']+modregister.attributes['vb']
    modregister.attributes['eM']=int(jmesas['mesasEscrutadas'].replace(".",""))
    #Proporción Mesas
    modregister.attributes['ceM']=round((modregister.attributes['eM']/modregister.attributes['mesas'])*100,3)
    #Proporción Padrón
    modregister.attributes['cpart']=round((modregister.attributes['vt']/modregister.attributes['padron'])*100,3)
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
    #Resultados    
    #opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart   
    modregister.attributes['opc1']=0
    modregister.attributes['opc2']=0
    modregister.attributes['opc3']=0
    modregister.attributes['opc4']=0
    modregister.attributes['opc5']=0
    modregister.attributes['opc6']=0
    for part in jmesas['data']:
        opcion=asignaOpcion(part['a'][:3])
        modregister.attributes[opcion]=int(part['c'].replace(".",""))
    comp={  'A':modregister.attributes['opc1'],
            'B':modregister.attributes['opc2'],
            'C':modregister.attributes['opc3'],
            'D':modregister.attributes['opc4'],
            'E':modregister.attributes['opc5'],
            'I':modregister.attributes['opc6'],
            }
    if (int(jmesas['resumen'][0]['c'].replace(".","")) != 0):
        maximo = [k for k, v in comp.items() if v == max(comp.values())]
        if len(maximo)==1:
            modregister.attributes['win']=maximo
        else: modregister.attributes['win']='T'    
    else: modregister.attributes['win']='T'
    modregister.attributes['vv']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn']=int(jmesas['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb']=int(jmesas['resumen'][2]['c'].replace(".",""))
    modregister.attributes['vt']=modregister.attributes['vv']+modregister.attributes['vn']+modregister.attributes['vb']
    modregister.attributes['eM']=int(jmesas['mesasEscrutadas'].replace(".",""))
    #Proporción Mesas
    modregister.attributes['ceM']=round((modregister.attributes['eM']/modregister.attributes['mesas'])*100,3)
    #Proporción Padrón
    #modregister.attributes['cpart']=round((modregister.attributes['vt']/modregister.attributes['padron'])*100,3)    
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
    #Resultados       
    #opc1,opc2,opc3,opc4,opc5,opc6,vv,vn,vb,vt,eM,ceM,win,cpart   
    modregister.attributes['opc1']=0
    modregister.attributes['opc2']=0
    modregister.attributes['opc3']=0
    modregister.attributes['opc4']=0
    modregister.attributes['opc5']=0
    modregister.attributes['opc6']=0
    for part in jmesas['data']:
        opcion=asignaOpcion(part['a'][:3])
        modregister.attributes[opcion]=int(part['c'].replace(".",""))
    comp={  'A':modregister.attributes['opc1'],
            'B':modregister.attributes['opc2'],
            'C':modregister.attributes['opc3'],
            'D':modregister.attributes['opc4'],
            'E':modregister.attributes['opc5'],
            'I':modregister.attributes['opc6'],
            }
    if (int(jmesas['resumen'][0]['c'].replace(".","")) != 0):
        maximo = [k for k, v in comp.items() if v == max(comp.values())]
        if len(maximo)==1:
            modregister.attributes['win']=maximo
        else: modregister.attributes['win']='T'    
    else: modregister.attributes['win']='T'
    modregister.attributes['vv']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['vn']=int(jmesas['resumen'][1]['c'].replace(".",""))
    modregister.attributes['vb']=int(jmesas['resumen'][2]['c'].replace(".",""))
    modregister.attributes['vt']=modregister.attributes['vv']+modregister.attributes['vn']+modregister.attributes['vb']
    modregister.attributes['eM']=int(jmesas['mesasEscrutadas'].replace(".",""))
    #Proporción Mesas
    modregister.attributes['ceM']=round((modregister.attributes['eM']/modregister.attributes['mesas'])*100,3)
    #Proporción Padrón
    #modregister.attributes['cpart']=round((modregister.attributes['vt']/modregister.attributes['padron'])*100,3)    
    asignador(modregister,ambito,indext)


def CheckNovedad(url):
    #response = requests.request("GET", masterurl, headers={}, data={})
    print(url)
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
            hilost=[]
            hilos=[]
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