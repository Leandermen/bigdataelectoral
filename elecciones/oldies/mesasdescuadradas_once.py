import requests
import json
import pandas as pd
import time
from multiprocessing import Pool
import datetime
import arcgis

reg=open("elecciones/inputs/mesas_cl.json")
jreg=json.load(reg)
tablamesa=pd.DataFrame(columns=['cloc','cmesa','mesa','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vtc','vts','w1V','obs'])
#tablalocal=pd.DataFrame(columns=['cloc','local','c1','c2','c3','c4','c5','c6','c7','vv','vn','vb','vt','me','tm','cm','w1V'])
for mesa in jreg:
    val=mesa['md']
    if val:    
        print(mesa['c_mesa'])
        urlmesa="https://www.servelelecciones.cl/data/elecciones_presidente/computomesas/{}.json".format(mesa['c_mesa'])
        response = requests.request("GET", urlmesa, headers={}, data={})
        datas=json.loads(response.text)
        obs=''
        try:
            boric=int(datas['data'][0]['c'].replace(".",""))
        except:
            boric=0       
        try:
            kast=int(datas['data'][1]['c'].replace(".",""))
        except:
            kast=0
        try:
            provoste=int(datas['data'][2]['c'].replace(".",""))
        except:
            provoste=0  
        try:
            sichel=int(datas['data'][3]['c'].replace(".",""))
        except:
            sichel=0     
        try:
            artes=int(datas['data'][4]['c'].replace(".",""))
        except:
            artes=0
        try:
            enriquez=int(datas['data'][5]['c'].replace(".",""))
        except:
            enriquez=0      
        try:
            parisi=int(datas['data'][6]['c'].replace(".",""))
        except:
            parisi=0        
        comp={  'Boric':boric,
                'Kast':kast,
                'Provoste':provoste,
                'Sichel':sichel,
                'Artés':artes,
                'ME-O':enriquez,
                'Parisi':parisi}
        winner=max(comp,key=comp.get)
        vve=int(datas['resumen'][0]['c'].replace(".",""))
        try:
            vnu=int(datas['resumen'][1]['c'].replace(".",""))
        except:
            vnu=0
            obs='- Falla VNU '
        try:
            vbl=int(datas['resumen'][2]['c'].replace(".",""))    
        except:
            vbl=0
            obs=obs+'- Falla VBL'
        tv=vve+vnu+vbl
        try:
            vts=int(datas['resumen'][3]['c'].replace(".",""))
        except:
            vts=0  
        fila={  
                'cloc':mesa['c_loc'],
                'cmesa':mesa['c_mesa'],
                'mesa':mesa['n_mesa'],
                'c1':boric,
                'c2':kast,
                'c3':provoste,
                'c4':sichel,
                'c5':artes,
                'c6':enriquez,
                'c7':parisi,
                'vv':vve,
                'vn':vnu,
                'vb':vbl,
                'vtc':tv,
                'vts':vts,
                'w1V':winner,
                'obs':obs}
        tablamesa=tablamesa.append(fila,ignore_index=True)

outfn='elecciones/outputs/descuadra/totalvotos.csv'
tablamesa.to_csv(path_or_buf=outfn,index=False,encoding='utf-8')
reg.close()