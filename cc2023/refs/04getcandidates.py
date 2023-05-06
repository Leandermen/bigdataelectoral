import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import requests
import json
import pandas as pd

evento='cc2023'
context_event='elecciones_consejo_gen'
folder='lookups'

s = requests.Session()
ccs=open("{}/{}/circsenat.json".format(evento,folder),encoding='utf-8')
jccs=json.load(ccs)
tabla=pd.DataFrame(columns=['id','csen','pacto','rawpartido','cpartido','numero','genero','nombre','indpart'])

def candNumber(a):
    i=a.find('.')
    result = a[:i]
    return result

def clearName(a):
    i=a.find('.')
    e=a.rfind(' ')
    result = a[i+2:e]
    return result

def genderbase(a):
    ig=a.rfind(' ')
    base = a[ig+1:]
    base = base.replace('(','')
    base = base.replace(')','')
    return base

def pactoPolitico(a):
    base=a[0:2]
    if base[1] == '.':
        return base[0]
    else: return "IND"

for key in jccs:
    k=key['c']
    d=key['d']
    print(d)
    direccion="https://www.servelelecciones.cl/data/{}/computo/circ_senatorial/{}.json".format(context_event,k)
    payload={}
    headers={}
    response=s.get(direccion)
    datas=json.loads(response.text)
    for a in datas['data']:
        pp = pactoPolitico(a['a'])
        for b in a['sd']:
            cstr = b['a']
            cpp=b['b']
            cname= clearName(cstr) 
            cidx = candNumber(cstr)
            cgen = genderbase(cstr)
            indep = 0
            solopp = cpp
            if cpp[:4] == "IND-":
                indep = 1
                solopp = cpp[4:]
            record = {
                'id':str(k)+'-'+str(pp)+'-'+str(cidx),
                'csen':k,
                'pacto':pp,
                'rawpartido':cpp,
                'cpartido':solopp,
                'numero':cidx,
                'genero':cgen,
                'nombre':cname,
                'indpart':indep
            }
            tabla=tabla.append(record,ignore_index=True)

tabla.to_json(path_or_buf='{}/{}/candidatos.json'.format(evento,folder),orient='records',force_ascii=False)
tabla.to_csv(path_or_buf='{}/{}/tblcandidatos.csv'.format(evento,folder),index=False,encoding='utf-8')
ccs.close()
