import requests
import json

evento='cc2023'
context_event='elecciones_consejo_gen'
folder='lookups'

#continentlist="https://www.servelelecciones.cl/data/{}/filters/continentes/all.json".format(context_event)
""" countrylist="https://www.servelelecciones.cl/data/{}/filters/paises/all.json".format(context_event) """
reglist="https://www.servelelecciones.cl/data/{}/filters/regiones/all.json".format(context_event)
provlist="https://www.servelelecciones.cl/data/{}/filters/provincias/all.json".format(context_event)
comlist="https://www.servelelecciones.cl/data/{}/filters/comunas/all.json".format(context_event)
circelectoral="https://www.servelelecciones.cl/data/{}/filters/circ_electoral/allchile.json".format(context_event)
circsenator="https://www.servelelecciones.cl/data/{}/filters/circ_senatorial/all.json".format(context_event)

#rcon = requests.request("GET", continentlist, headers={}, data={})
""" rpai = requests.request("GET", countrylist, headers={}, data={}) """
rreg = requests.request("GET", reglist, headers={}, data={})
rpro = requests.request("GET", provlist, headers={}, data={})
rcom = requests.request("GET", comlist, headers={}, data={})
rcce = requests.request("GET", circelectoral, headers={}, data={})
rccs = requests.request("GET", circsenator, headers={}, data={})

#jcon=json.loads(rcon.text)
""" jpai=json.loads(rpai.text) """
jreg=json.loads(rreg.text)
jpro=json.loads(rpro.text)
jcom=json.loads(rcom.text)
jcce=json.loads(rcce.text)
jccs=json.loads(rccs.text)

#with open('{}/{}/continentes.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    #json.dump(jcon, f,ensure_ascii=False, indent=4)

""" with open('{}/{}/pais.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jpai, f,ensure_ascii=False, indent=4) """

with open('{}/{}/regiones.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jreg, f,ensure_ascii=False, indent=4)

with open('{}/{}/provincias.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jpro, f,ensure_ascii=False, indent=4)

with open('{}/{}/comunas.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jcom, f,ensure_ascii=False, indent=4)

with open('{}/{}/circelectoral.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jcce, f,ensure_ascii=False, indent=4)

with open('{}/{}/circsenat.json'.format(evento,folder), 'w', encoding='utf-8') as f:
    json.dump(jccs, f,ensure_ascii=False, indent=4)