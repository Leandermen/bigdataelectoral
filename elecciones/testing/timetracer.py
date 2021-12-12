#Fase 1: Instalación de Mesas
import requests
import json
import time
from datetime import datetime

masterurl="https://www.servelelecciones.cl/data/mesas_instaladas/computo/global/19001.json"
dato=0
update=0

def CheckNovedad(url):
    response = requests.request("GET", masterurl, headers={}, data={})
    r=json.loads(response.text)
    mesasinst=int(r['resumen'][0]['c'].replace(".",""))
    print (mesasinst)
    return mesasinst

def actualiza():
    print ("Actualizando...")

if __name__ == '__main__':
    while True:
        update=CheckNovedad(masterurl)
        if update!=dato:
            actualiza()
            dato=update
        else: print ("Todo Igual")
        time.sleep(5)