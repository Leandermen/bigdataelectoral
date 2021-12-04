import requests
import json

continentlist="https://www.servelelecciones.cl/data/mesas_instaladas/filters/continentes/all.json"
countrylist="https://www.servelelecciones.cl/data/mesas_instaladas/filters/paises/all.json"
reglist="https://www.servelelecciones.cl/data/elecciones_presidente/filters/regiones/all.json"
provlist="https://www.servelelecciones.cl/data/elecciones_presidente/filters/provincias/all.json"
comlist="https://www.servelelecciones.cl/data/elecciones_presidente/filters/comunas/all.json"
circelectoral="https://www.servelelecciones.cl/data/elecciones_presidente/filters/circ_electoral/allchile.json"

rcon = requests.request("GET", continentlist, headers={}, data={})
rpai = requests.request("GET", countrylist, headers={}, data={})
rreg = requests.request("GET", reglist, headers={}, data={})
rpro = requests.request("GET", provlist, headers={}, data={})
rcom = requests.request("GET", comlist, headers={}, data={})
rcce = requests.request("GET", circelectoral, headers={}, data={})

jcon=json.loads(rcon.text)
jpai=json.loads(rpai.text)
jreg=json.loads(rreg.text)
jpro=json.loads(rpro.text)
jcom=json.loads(rcom.text)
jcce=json.loads(rcce.text)

with open('elecciones/inputs/continentes.json', 'w', encoding='utf-8') as f:
    json.dump(jcon, f,ensure_ascii=False, indent=4)

with open('elecciones/inputs/pais.json', 'w', encoding='utf-8') as f:
    json.dump(jpai, f,ensure_ascii=False, indent=4)

with open('elecciones/inputs/regiones.json', 'w', encoding='utf-8') as f:
    json.dump(jreg, f,ensure_ascii=False, indent=4)

with open('elecciones/inputs/provincias.json', 'w', encoding='utf-8') as f:
    json.dump(jpro, f,ensure_ascii=False, indent=4)

with open('elecciones/inputs/comunas.json', 'w', encoding='utf-8') as f:
    json.dump(jcom, f,ensure_ascii=False, indent=4)

with open('elecciones/inputs/circelectoral.json', 'w', encoding='utf-8') as f:
    json.dump(jcce, f,ensure_ascii=False, indent=4)
