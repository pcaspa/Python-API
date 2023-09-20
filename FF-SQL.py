import time
import os
import requests
import json
import csv
import pathlib
import pyodbc

# python -m pip install

from datetime import datetime

s = requests.Session()
s.headers.update({'Content-type': 'application/json'})

client_id = "39_543bqpabl58oscok8g0wggo0kgkog8cow40sg400koc4o0ogw4"
client_secret = "1db321m491j40wc8oskg0084k8o0ogsk8ogk8404k8s0o0soko"
username = "calming.blanket.api@fulfilio.com.au"
password = "uFD]5bXf#hk4U<hM"


def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format("ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    return conn, cursor

def FF_login():
    formdata = {
	"client_id": client_id,
	"client_secret": client_secret,
	"username": username,
	"password": password
        }
    r = s.post('https://app.fulfilio.net/api/v1/login', data = json.dumps(formdata))

    if r.ok:
        print("Login Successful")
        access_token = r.json()['data']['access_token']
        refresh_token = r.json()['data']['refresh_token']
        return access_token, refresh_token
    else:
        print("HTTP %i - %s, Message %s" % (r.status_code, r.reason, r.text))

def FF_Stock():
    access_token, refresh_token = FF_login()
    s.headers.update({'Authorization': 'Bearer '+str(access_token),
                  'Content-Type': 'application/json'})
    conn, cursor = get_connection(user='###', Pass='###', IP='###', database='###')
    cursor = conn.cursor()
    skus = list(set([str(each['sku']).strip() for each in s.get('https://app.fulfilio.net/api/v1/products?order_by=externalId&sort=ASC&limit=1000&page=1').json()['data']]))
    new = 0
    for sku in skus:
        r = s.get('https://app.fulfilio.net/api/v1/inventories/?skus[0]='+str(sku)+'&page=1&limit=250')
        for x in json.loads(r.text)['data']:
            try:
                sku = x['sku']
            except:
                sku = ''
            try:
                quantity = x['quantity']
            except:
                quantity = ''
            try:
                warehouse_code = x['warehouse_code']
            except:
                warehouse_code = ''
            try:
                created = x['created']
            except:
                created = ''
            try:
                modified = x['modified']
            except:
                created = ''
            
            cursor.execute("""
                            INSERT INTO ###.dbo.Fulfilio_Inventory (sku, quantity,  warehouse_code,
                            created, modified)
                            VALUES
                            (?,?,?,?,?)
                            """,sku, quantity, warehouse_code, created, modified)
            new+=1
        conn.commit()
        
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print (current_time," Fulfilio Inventory Capture Complete  ",new," Rows")

  
def FFStock_Archive():

    conn, cursor = get_connection(user='###', Pass='###', IP='###', database='###')
    cursor = conn.cursor()
    cursor.execute("""
                INSERT INTO Fulfilio_Inventory_Hist  (sku, warehouse_code, quantity, Archived)
                SELECT  sku, warehouse_code, quantity, { fn NOW() } 
                FROM  Fulfilio_Inventory """)
    conn.commit()
    cursor.execute("DELETE FROM Fulfilio_Inventory")
    conn.commit()

if __name__ == "__main__":
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(current_time," Fulfilio API Connection Started")
    FFStock_Archive()
    FF_Stock()
