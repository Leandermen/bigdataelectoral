import pandas as pd
from arcgis.gis import GIS

evento='cc2023'
folder='lookups'

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
#ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
ItemSource=gis.content.get('8f0e3c359a594606bb7a86bdd83c1971')

def processservice(layer,filename):
    datalocal=layer.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
    datalocal.to_json(path_or_buf='{}/{}/{}.json'.format(evento,folder,filename),orient='records')

localchl=ItemSource.layers[0]
comuchl=ItemSource.layers[1]
regichl=ItemSource.layers[2]

processservice(localchl,"oidlocales")
processservice(comuchl,"oidcomunas")
processservice(regichl,"oidregiones")