# -*- coding: utf-8 -*-
"""
Éditeur de Spyder

Ceci est un script temporaire.
"""

import mysql.connector
from mysql.connector import Error
import requests
import json

try:
    #connexion au serveur mysql
    connexion = mysql.connector.connect(host='localhost',
                                         database='dataengineer',
                                         user='root', password="****")
    #si la connexion réussit, on récupère l'ensemble de la table "address" en y ajoutant les colonnes latitude et longitude
    if connexion.is_connected():
        cursor = connexion.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("Vous êtes connectés à la base de données: ", record)
        cursor.execute("SELECT * from address")
        result=cursor.fetchall()
        #cursor.execute("alter table dataengineer.address add latitude float default null, add column longitude float default null;")
        
        #on crée une liste vide qui contiendra l'ensemble des coordonnées correspondants aux adresses    
        reponse=[]
        #on parcourt la table "address" pour réaliser les requêtes à l'API et récolter les coordonnées
        for address in result:
            id=address[0]
            adresse=address[1]+","+address[2]+","+address[3]
            api=requests.get("https://nominatim.openstreetmap.org/search?q="+adresse+"&format=json").text
            js=json.loads(api)
            if len(js)>0:
                reponse.append([id,js[0]["lat"],js[0]["lon"]])
            #else:
             #   reponse.append([id,0,0])
                
        #on ajoute les résultats récoltés à nos colonnes latitude et longitude dans sql
        for r in reponse:
            cursor.execute("update address set latitude="+str(r[1])+" where address_id="+str(r[0]))
            cursor.execute("update address set longitude="+str(r[2])+" where address_id="+str(r[0]))
            connexion.commit()

except Error as e:
    print("Error while connecting to MySQL", e)
    
    
finally:
    if connexion.is_connected():
        cursor.close()
        connexion.close()
        print("MySQL connection is closed")

