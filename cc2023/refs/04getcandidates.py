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
tabla=pd.DataFrame(columns=['idcan','csen','lista','partido','cpartido','n_cartilla','genero','nombre','esind'])

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
                'idcan':str(k)+'-'+str(pp)+'-'+str(cidx),
                'csen':k,
                'lista':pp,
                'partido':cpp,
                'cpartido':solopp,
                'n_cartilla':cidx,
                'genero':cgen,
                'nombre':cname,
                'esind':indep
            }
            tabla=tabla.append(record,ignore_index=True)

tabla.to_json(path_or_buf='{}/{}/candidatos.json'.format(evento,folder),orient='records',force_ascii=False)
tabla.to_csv(path_or_buf='{}/{}/tblcandidatos.csv'.format(evento,folder),index=False,encoding='utf-8')

cand=open("{}/{}/candidatos.json".format(evento,folder),encoding='utf-8')
jcand=json.load(cand)
com=open("{}/{}/comunas.json".format(evento,folder),encoding='utf-8')
jcom=json.load(com)
loc=open("{}/{}/localesservel.json".format(evento,folder),encoding='utf-8')
jloc=json.load(loc)
tablac=pd.DataFrame(columns=['idcan','csen','lista','partido','cpartido','n_cartilla','genero','nombre','esind','idservel'])
tablal=pd.DataFrame(columns=['idcan','csen','lista','partido','cpartido','n_cartilla','genero','nombre','esind','idservel'])

for key in jccs:
    k=key['c']
    d=key['d']
    urlcom="https://www.servelelecciones.cl/data/{}/filters/comunas/bycirc_senatorial/{}.json".format(context_event,k)
    rpcom = requests.request("GET", urlcom, headers={}, data={})
    circelecs=json.loads(rpcom.text)
    for comunita in circelecs:
        ak=comunita['c']
        print (comunita['d'])
        direccion="https://www.servelelecciones.cl/data/{}/computo/comunas/{}.json".format(context_event,ak)
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
                    'idcan':str(ak)+'-'+str(k)+'-'+str(pp)+'-'+str(cidx),
                    'csen':k,
                    'lista':pp,
                    'partido':cpp,
                    'cpartido':solopp,
                    'n_cartilla':cidx,
                    'genero':cgen,
                    'nombre':cname,
                    'esind':indep,
                    'idservel':ak
                }
                tablac=tablac.append(record,ignore_index=True)

tablac.to_csv(path_or_buf='{}/{}/tblcomcandidatos.csv'.format(evento,folder),index=False,encoding='utf-8')

for key in jccs:
    k=key['c']
    d=key['d']
    urlcom="https://www.servelelecciones.cl/data/{}/filters/circ_electoral/bycirc_senatorial/{}.json".format(context_event,k)
    rpcom = requests.request("GET", urlcom, headers={}, data={})
    circelecs=json.loads(rpcom.text)
    for comunita in circelecs:
        ck=comunita['c']
        print (comunita['d'])
        urlloc="https://www.servelelecciones.cl/data/{}/filters/locales/bycirc_electoral/{}.json".format(context_event,ck)
        rplocs = requests.request("GET", urlloc, headers={}, data={})
        locales=json.loads(rplocs.text)
        for localitos in locales:
            ak=localitos['c']
            direccion="https://www.servelelecciones.cl/data/{}/computo/locales/{}.json".format(context_event,ak)
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
                        'idcan':str(ak)+'-'+str(k)+'-'+str(pp)+'-'+str(cidx),
                        'csen':k,
                        'lista':pp,
                        'partido':cpp,
                        'cpartido':solopp,
                        'n_cartilla':cidx,
                        'genero':cgen,
                        'nombre':cname,
                        'esind':indep,
                        'idservel':ak
                    }
                    tablal=tablal.append(record,ignore_index=True)
        
tablal.to_csv(path_or_buf='{}/{}/tblloccandidatos.csv'.format(evento,folder),index=False,encoding='utf-8')      
        
        
        
        

        


















ccs.close()
