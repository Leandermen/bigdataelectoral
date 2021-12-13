import requests
import json
import time
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
    rresults = requests.request("GET", resultados, headers={}, data={})
    jresults=json.loads(rresults.text)
    rmesas = requests.request("GET", mesas, headers={}, data={})
    jmesas=json.loads(rmesas.text)
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