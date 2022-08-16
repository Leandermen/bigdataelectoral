import pandas as pd
from arcgis.gis import GIS

evento='pleb2022'
folder='lookups'

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
#ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')
ItemSource=gis.content.get('dacbfde953734f3f9a3c9e604b89dfb1')

def processservice(layer,filename):
    datalocal=layer.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
    datalocal.to_json(path_or_buf='{}/{}/{}.json'.format(evento,folder,filename),orient='records')

localext=ItemSource.layers[1]
localchl=ItemSource.layers[0]
paisext=ItemSource.layers[5]
comuchl=ItemSource.layers[4]
provchl=ItemSource.layers[3]
regichl=ItemSource.layers[2]

processservice(localext,"oidlocalext")
processservice(localchl,"oidlocales")
processservice(paisext,"oidpaises")
processservice(comuchl,"oidcomunas")
processservice(provchl,"oidprovincias")
processservice(regichl,"oidregiones")