import requests
import json
import pandas as pd

evento='ter2023'
context_event='elecciones_municipales'
folder='lookups'

url = "https://ciudadano.subdere.gov.cl/pentaho/plugin/cda/api/doQuery?"
headers = {
  'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Microsoft Edge";v="116"',
  'Accept': '*/*',
  'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8',
  'X-Requested-With': 'XMLHttpRequest',
  'sec-ch-ua-mobile': '?0',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36 Edg/116.0.1938.69',
  'sec-ch-ua-platform': '"Windows"',
  'Cookie': 'JSESSIONID=3401B7D17721E28240691527BA1A992E; session-flushed=true; server-time=1694113419677; session-expiry=1694120619677'
}

s = requests.Session()
cut=open("{}/{}/comdataine.json".format(evento,folder),encoding='utf-8')
jcut=json.load(cut)
outdata=[]

for key in jcut:
    k=key['codcom']
    d=key['nomcom']
    payload = r'dataAccessId=dsConsejales&outputIndexId=1&pageSize=0&pageStart=0&parampComuna={}&parampPeriodo=68&paramsearchBox=&path=%2Fpublic%2FSIM_PUBLICO%2FTABLEROS_MINI%2FDM02_Autoridades.cda&sortBy='.format(k)
    print (d)
    urlrequest=url
    rpcom=requests.request("POST",urlrequest,headers=headers,data=payload)
    datos=json.loads(rpcom.text)
    #print (datos)
    for concejal in datos["resultset"]:
        gc="Mujer"
        if concejal[2]==1:
            gc="Hombre"
        diccio={
            "nombre":concejal[0],
            "partido":concejal[1],
            "genero":gc,
            "rut":concejal[3],
            "comuna":d
        }
        outdata.append(diccio)
salida=pd.DataFrame(outdata)
salida.to_csv(path_or_buf='{}/{}/dataconcejales.csv'.format(evento,folder),index=False,encoding='utf-8')
