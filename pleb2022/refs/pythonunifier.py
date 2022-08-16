import json
import pandas as pd

evento='pleb2022'
folder='lookups'

tabla=pd.DataFrame(columns=['oid','ids','amb','ext'])

pai=open('{}/{}/oidpaises.json'.format(evento,folder))
reg=open('{}/{}/oidregiones.json'.format(evento,folder))
pro=open('{}/{}/oidprovincias.json'.format(evento,folder))
com=open('{}/{}/oidcomunas.json'.format(evento,folder))
lch=open('{}/{}/oidlocales.json'.format(evento,folder))
lex=open('{}/{}/oidlocalext.json'.format(evento,folder))

jpai=json.load(pai)
jreg=json.load(reg)
jpro=json.load(pro)
jcom=json.load(com)
jlch=json.load(lch)
jlex=json.load(lex)

for a in jpai:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'pais',
        'ext':False
    }
    tabla=tabla.append(fila,ignore_index=True)

for a in jreg:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'regiones',
        'ext':False
    }
    tabla=tabla.append(fila,ignore_index=True)

for a in jpro:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'provincias',
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

for a in jlex:
    fila={
        'oid':a['OBJECTID'],
        'ids':a['idservel'],
        'amb':'locales',
        'ext':True
    }
    tabla=tabla.append(fila,ignore_index=True)

tabla.to_json(path_or_buf='{}/{}/codigosfs.json'.format(evento,folder),orient='records')
pai.close()
reg.close()
pro.close()
com.close()
lch.close()
lex.close()