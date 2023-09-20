import requests
import pyodbc
import time
from random import randint


def get_three_pls_id(url):
    while True:
        try:
            r = x(url)
        except:
            r = None
        if r.status_code == 200:
            break
        else:
            time.sleep(randint(15, 29))
    return [[each['id'], each['company_name']] for each in r.json()['results']]


def parser(ID):
    new = 0
    while True:
        try:
            r = x(
                f'https://api.borderless360.com/api/v1/public_api/inventory/inventory_levels/?expand=product&format=json&limit=1000&offset=0&three_pl={ID[0]}')
        except:
            r = None
        if r.status_code == 200:
            break
        else:
            time.sleep(randint(15, 29))
    for y in r.json()['results']:
        id_ = y['id']
        three_pl = y['three_pl']
        product_id = y['product_id']
        stock_quantity = y['stock_quantity']
        client = y['product']['client']
        created = y['product']['created']
        title = y['product']['title']
        image = y['product']['image']
        product_type = y['product']['product_type']
        sku = y['product']['sku']
        barcode = y['product']['barcode']
        width = y['product']['width']
        length = y['product']['length']
        height = y['product']['height']
        weight = y['product']['weight']
        country_of_manufacture = y['product']['country_of_manufacture']
        country_of_manufacture_display = y['product']['country_of_manufacture_display']
        hs_code = y['product']['hs_code']
        tax_code = y['product']['tax_code']
        customs_description = y['product']['customs_description']
        local_customs_description = y['product']['local_customs_description']
        state = y['product']['state']
        state_display = y['product']['state_display']
        declared_price = y['product']['declared_price']
        wholesale_price = y['product']['wholesale_price']
        retail_price = y['product']['retail_price']
        customs_value = y['product']['customs_value']
        customs_value_source = y['product']['customs_value_source']
        customs_value_source_display = y['product']['customs_value_source_display']
        source = ", ".join(y['product']['source'])
        safety_stock = y['product']['safety_stock']
        #print (title)
        cursor.execute("""
                            INSERT INTO ###.dbo.BL360_Inventory (company_name, id, three_pl, product_id, stock_quantity, client, created, title, image, product_type, sku, barcode, width, length, height, weight,
                            country_of_manufacture, country_of_manufacture_display, hs_code, tax_code, customs_description, local_customs_description, state, state_display, declared_price,
                            wholesale_price, retail_price, customs_value, customs_value_source, customs_value_source_display, source, safety_stock)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            """, ID[1], id_, three_pl, product_id, stock_quantity, client, created, title, image, product_type[:50], sku[:50], barcode[:50], width[:50], length[:50], height[:50], weight[:50], country_of_manufacture[:50],
                       country_of_manufacture_display[:50], hs_code[:50], tax_code[:50], customs_description[:50], local_customs_description[:50], state[:50], state_display[:50], declared_price, wholesale_price, retail_price,
                       customs_value, customs_value_source, customs_value_source_display, source, safety_stock)
        new = new+1

    conn.commit()
    return new


def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
        "ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    return conn, cursor


if __name__ == "__main__":
    headers = {'Content-Type': 'application/json',
               'Authorization': '###'}

    def x(x): return requests.get(x, headers=headers)
    conn, cursor = get_connection(
        user='###', Pass='###', IP='###', database='###')

    cursor.execute("DELETE FROM ###.dbo.BL360_Inventory")
    conn.commit()
    ID_s = get_three_pls_id(
        'https://api.borderless360.com/api/v1/public_api/three_pl/three_pls/?limit=100')
    for ID in ID_s:
        time.sleep(4)
        ret = parser(ID)
        print(ID, ret)
