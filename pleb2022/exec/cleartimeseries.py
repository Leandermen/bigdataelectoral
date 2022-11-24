from arcgis.gis import GIS

print("Inicio Conexión GIS")
gis = GIS("https://www.arcgis.com", 'soportaltda', 'Mhilo.2016', expiration=9999)
ItemTS=gis.content.get('1cde3f8c703c457c9d7f91f8af42a004')
#Nación
ttsnation=ItemTS.tables[0]
#Territorios
ttspais=ItemTS.tables[1]
ttsregi=ItemTS.tables[2]
ttsprov=ItemTS.tables[3]
ttscomu=ItemTS.tables[4]
#Locales
ttslocext=ItemTS.tables[6]
ttslocnac=ItemTS.tables[5]

ttsnation.manager.truncate()
ttspais.manager.truncate()
ttsregi.manager.truncate()
ttsprov.manager.truncate()
ttscomu.manager.truncate()
ttslocext.manager.truncate()
ttslocnac.manager.truncate()



