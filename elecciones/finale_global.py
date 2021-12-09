import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
import datetime
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor
from datetime import datetime

#Global
def GlobalNational(mainnational):
    #Timestamps
    dt=datetime.now()
    #Obtención de Jsons
    resultados="https://www.servelelecciones.cl/data/elecciones_presidente/computo/global/19001.json"
    mesas="https://www.servelelecciones.cl/data/mesas_instaladas/computo/global/19001.json"
    padron="https://www.servelelecciones.cl/data/participacion/computo/global/19001.json"
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
    rpadron = requests.request("GET", padron, headers={}, data={})
    jpadron=json.loads(rpadron.text)
    #mainnational=ItemSource.tables[0]
    qnation=mainnational.query(out_fields='*')
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
    modregister.attributes['tM1v']=int(jmesas['resumen'][0]['b'].replace(".",""))
    modregister.attributes['iM1v']=int(jmesas['resumen'][0]['c'].replace(".",""))
    modregister.attributes['pM1v']=int(jmesas['resumen'][0]['d'].replace(".",""))
    modregister.attributes['eM1v']=int(jresults['mesasEscrutadas'].replace(".",""))
    if modregister.attributes['tM1v'] != 0:
        modregister.attributes['cM1v']=round((modregister.attributes['iM1v']/modregister.attributes['tM1v'])*100,3)
    else: modregister.attributes['cM1v']=0
    #Participación
    modregister.attributes['tPad']=int(jpadron['resumen'][0]['c'].replace(".",""))
    if modregister.attributes['tPad'] != 0:
        modregister.attributes['cv1v']=round((modregister.attributes['vt1v']/modregister.attributes['tPad'])*100,3)
    else: modregister.attributes['cv1v']=0
    print("Preparado")
    return modregister
    
    
if __name__ == '__main__':
    #UTerritorial=['regiones','provincias','comunas','pais']
    print('Computo Global en Progreso')
    gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
    ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
    ItemTS=gis.content.get('39107a24dd2847b2b3c41f1fe594c354')
    ttsnation=ItemTS.tables[0]
    tnation=ItemSource.tables[0]
    arreglo=GlobalNational(tnation)
    tnation.edit_features(updates=[arreglo])
    ttsnation.edit_features(adds=[arreglo])
    print("listo")