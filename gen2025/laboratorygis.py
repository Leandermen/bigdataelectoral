#Fase 3: Resultados Electorales (4 de septiembre-18:00)
import requests
import json
import time
import uuid
import numpy as np
#import datetime
import concurrent.futures
from arcgis.gis import GIS
from datetime import datetime
import pandas as pd

print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemSource=gis.content.get('b0e79cbbdf0049e5b815dba16652ae31')

naclocal=ItemSource.layers[0]
comuchl=ItemSource.layers[1]
regichl=ItemSource.layers[2]


qlocn=naclocal.query(out_fields='OBJECTID,id_local,envio,blancos,nulos,total_emitidos,total_general,electores,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)
qregi=regichl.query(out_fields='OBJECTID,id_region,envio,blancos,nulos,total_emitidos,total_general,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)
qcomu=comuchl.query(out_fields='OBJECTID,id_comuna,blancos,nulos,total_emitidos,total_general,n41900101,n41900102,n41900103,n41900104,n41900105,n41900106,n41900107,n41900108,ganador',return_geometry=False)

record_data = [f for f in qregi if f.attributes['id_region']==3001][0]

print(record_data.attributes)