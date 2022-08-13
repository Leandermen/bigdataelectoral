import pandas as pd
from arcgis.gis import GIS

evento='pleb2022'
folder='lookups'

gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016')
ItemSource=gis.content.get('d82682aef8f440deb1a35129502ae5a7')

localext=ItemSource.layers[0]
localchl=ItemSource.layers[1]
paisext=ItemSource.layers[2]
comuchl=ItemSource.layers[3]
provchl=ItemSource.layers[4]
regichl=ItemSource.layers[5]
#mainnational=ItemSource.tables[0]

datalocal=localext.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidlocalext.json'.format(evento,folder),orient='records')

datalocal=localchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidlocales.json'.format(evento,folder),orient='records')

datalocal=paisext.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidpaises.json'.format(evento,folder),orient='records')

datalocal=regichl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidregiones.json'.format(evento,folder),orient='records')

datalocal=provchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidprovincias.json'.format(evento,folder),orient='records')

datalocal=comuchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='{}/{}/oidcomunas.json'.format(evento,folder),orient='records')
