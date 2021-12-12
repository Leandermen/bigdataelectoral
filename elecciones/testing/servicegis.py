import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
import datetime
from arcgis.gis import GIS
from arcgis.features import GeoAccessor, GeoSeriesAccessor

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
datalocal.to_json(path_or_buf='elecciones/testing/oidlocalext.json',orient='records')

datalocal=localchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='elecciones/testing/oidlocales.json',orient='records')

datalocal=paisext.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='elecciones/testing/oidpaises.json',orient='records')

datalocal=regichl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='elecciones/testing/oidregiones.json',orient='records')

datalocal=provchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='elecciones/testing/oidprovincias.json',orient='records')

datalocal=comuchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
datalocal.to_json(path_or_buf='elecciones/testing/oidcomunas.json',orient='records')


