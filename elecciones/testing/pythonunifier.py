import json
import pandas as pd

tabla=pd.DataFrame(columns=['oid','ids','amb','ext'])

pai=open('elecciones/testing/oidpaises.json')
reg=open('elecciones/testing/oidregiones.json')
pro=open('elecciones/testing/oidprovincias.json')
com=open('elecciones/testing/oidcomunas.json')
lch=open('elecciones/testing/oidlocales.json')
lex=open('elecciones/testing/oidlocalext.json')

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

tabla.to_json(path_or_buf='elecciones/inputs/codigosfs.json',orient='records')
pai.close()
reg.close()
pro.close()
com.close()
lch.close()
lex.close()