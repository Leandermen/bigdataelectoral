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
mainnational=ItemSource.tables[0]

#datalocal=localchl.query(out_fields='objectid,idservel',return_geometry='false',as_df=True)
#datalocal.to_json(path_or_buf='elecciones/testing/oidlocales.json',orient='records')

c11v=123
c21v=100



qreg=regichl.query(out_fields='objectid,c11v,c21v',return_geometry='false')
featureV2=[f for f in qreg]

for ft in featureV2:
    ft.attributes['c11v']=c11v
    ft.attributes['c21v']=c21v
    regichl.edit_features(updates=[ft])
