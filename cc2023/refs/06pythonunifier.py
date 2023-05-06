import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import json
import pandas as pd

evento='cc2023'
folder='lookups'

tabla=pd.DataFrame(columns=['oid','ids','amb','ext'])

reg=open('{}/{}/oidregiones.json'.format(evento,folder))
com=open('{}/{}/oidcomunas.json'.format(evento,folder))
lch=open('{}/{}/oidlocales.json'.format(evento,folder))

jreg=json.load(reg)
jcom=json.load(com)
jlch=json.load(lch)


for a in jreg:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'regiones',
        'ext':False
    }
    tabla=tabla.append(fila,ignore_index=True)

for a in jcom:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'comunas',
        'ext':False
    }
    tabla=tabla.append(fila,ignore_index=True)

for a in jlch:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'locales',
        'ext':False
    }
    tabla=tabla.append(fila,ignore_index=True)


tabla.to_json(path_or_buf='{}/{}/codigosfs.json'.format(evento,folder),orient='records')
reg.close()
com.close()
lch.close()