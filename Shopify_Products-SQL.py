import itertools
import requests
import csv
import json
import pyodbc
import html
import time

from datetime import datetime

def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format("ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    return conn, cursor

def get_cost_item(APIKey, APIPW, URLPart, item_id):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36'}
    r = requests.get(f'https://{APIKey}:{APIPW}@{URLPart}.myshopify.com/admin/api/2022-01/inventory_items/{item_id}.json', headers = headers)
    try:
        cost = r.json()['inventory_item']['cost']
    except:
        cost = None
    return cost

def get_products_data(APIKey, APIPW, URLPart, conn, cursor, Handles_p, Brand):
    headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.82 Safari/537.36'}
    r = requests.get(f'https://{APIKey}:{APIPW}@{URLPart}.myshopify.com/admin/api/2022-01/products.json?limit=250', headers = headers)
    new=0
    for x in json.loads(r.text)['products']:
        time.sleep(1)
        try:
            handle = x['handle']
        except:
            handle = None
        try:
            title = x['title']
        except:
            title = None
        try:
            body_html = html.escape(" ".join(["".join(each.strip().split(' ')) for each in " ".join(x['body_html'].split()).split()]))
        except:
            body_html = None
        try:
            vendor = x['vendor']
        except:
            vendor = None
        try:
            Type = None
        except:
            Type = None
        try:
            Tags = x['tags']
        except:
            Tags = None
        try:
            Published = 'TRUE' if 'T' in x['published_at'] else 'FALSE'
        except:
            Published = 'FALSE'
        try:
            Option1_Name = x['options'][0]['name']
        except:
            Option1_Name = None
        try:
            Option1_Value = ", ".join(x['options'][0]['values'])
        except:
            Option1_Value = None
        try:
            Option2_Name = x['options'][1]['name']
        except:
            Option2_Name = None
        try:
            Option2_Value = ", ".join(x['options'][1]['values'])
        except:
            Option2_Value = None
        try:
            Option3_Name = x['options'][2]['name']
        except:
            Option3_Name = None
        try:
            Option3_Value = ", ".join(x['options'][2]['values'])
        except:
            Option3_Value = None
        try:
            Variant_SKU = [each['sku'] for each in x['variants']]#list
        except:
            Variant_SKU = None
        try:
            Variant_Grams = [each['grams'] for each in x['variants']]#list
        except:
            Variant_Grams = None
        try:
            Variant_Inventory_Tracker = [each['inventory_management'] for each in x['variants']]#list
        except:
            Variant_Inventory_Tracker = None
        try:
            Variant_Inventory_Qty = [each['inventory_quantity'] for each in x['variants']]#list
        except:
            Variant_Inventory_Qty = None
        try:
            Variant_Inventory_Policy = [each['inventory_policy'] for each in x['variants']]#list
        except:
            Variant_Inventory_Policy = None
        try:
            Variant_Fulfillment_Service = [each['fulfillment_service'] for each in x['variants']]#list
        except:
            Variant_Fulfillment_Service = None
        try:
            Variant_Price = [each['price'] for each in x['variants']]#list
        except:
            Variant_Price = None
        try:
            Variant_Compare_At_Price = [each['compare_at_price'] for each in x['variants']]#list
        except:
            Variant_Compare_At_Price = None
        try:
            Variant_Requires_Shipping = [each['requires_shipping'] for each in x['variants']]#list
        except:
            Variant_Requires_Shipping = None
        try:
            Variant_Taxable = [each['taxable'] for each in x['variants']]#list
        except:
            Variant_Taxable = None
        try:
            Variant_Barcode = [each['barcode'] for each in x['variants']]#list
        except:
            Variant_Barcode = None
        try:
            Image_Src = [each['src'] for each in x['images']]#list
        except:
            Image_Src = None
        try:
            Image_Position = [each['position'] for each in x['images']]#list
        except:
            Image_Position = None
        try:
            Image_Alt_Text = [each['alt'] for each in x['images']]#list
        except:
            Image_Alt_Text = None
        try:
            Variant_Weight_Unit = [each['weight_unit'] for each in x['variants']]#list
        except:
            Variant_Weight_Unit = None
        try:
            inventory_item_ids = [each['inventory_item_id'] for each in x['variants']]#list
        except:
            inventory_item_ids = None
        try:
            Status = x['status']
        except:
            Status = None
        #print ("%r" % body_html)
        qwe = 1
        for a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p in itertools.zip_longest(Variant_SKU, Variant_Grams, Variant_Inventory_Tracker, Variant_Inventory_Qty, Variant_Inventory_Policy, Variant_Fulfillment_Service,
                                                             Variant_Price, Variant_Compare_At_Price, Variant_Requires_Shipping, Variant_Taxable, Variant_Barcode, Image_Src, Image_Position, Image_Alt_Text,
                                                             Variant_Weight_Unit, inventory_item_ids):
            
            cost_per_item = get_cost_item(APIKey, APIPW, URLPart, p)
            
            if qwe == 1:
                if handle not in Handles_p:
                    try:
                        cursor.execute("""
                                    INSERT INTO ###.dbo.Shopify_Products (Brand, Handle, Title,  Vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                    Variant_SKU, Variant_Grams, Variant_Inventory_Tracker, Variant_Inventory_Qty, Variant_Inventory_Policy, Variant_Fulfillment_Service, Variant_Cost, Variant_Price, Variant_Compare_At_Price,
                                    Variant_Requires_Shipping, Variant_Taxable, Variant_Barcode, Image_Src, Image_Position, Image_Alt_Text, Variant_Weight_Unit, Status)
                                    VALUES
                                    ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                                    """.format(Brand, handle, title,  vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                                a, b, c, d, e, f, cost_per_item, g, h, i, j, k, l, m, n, o, Status))
                    except:
                        
                        cursor.execute("""
                                    INSERT INTO ###.dbo.Shopify_Products (Brand, Handle, Title,  Vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                    Variant_SKU, Variant_Grams, Variant_Inventory_Tracker, Variant_Inventory_Qty, Variant_Inventory_Policy, Variant_Fulfillment_Service, Variant_Cost, Variant_Price, Variant_Compare_At_Price,
                                    Variant_Requires_Shipping, Variant_Taxable, Variant_Barcode, Image_Src, Image_Position, Image_Alt_Text, Variant_Weight_Unit, Status)
                                    VALUES
                                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """,Brand, handle, title,  vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                       a, b, c, d, e, f, cost_per_item, g, h, i, j, k, l, m, n, o, Status)
                
            else:

                if handle not in Handles_p:
                    try:
                        cursor.execute("""
                                    INSERT INTO ###.dbo.Shopify_Products (Brand, Handle, Title,  Vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                    Variant_SKU, Variant_Grams, Variant_Inventory_Tracker, Variant_Inventory_Qty, Variant_Inventory_Policy, Variant_Fulfillment_Service, Variant_Cost, Variant_Price, Variant_Compare_At_Price,
                                    Variant_Requires_Shipping, Variant_Taxable, Variant_Barcode, Image_Src, Image_Position, Image_Alt_Text, Variant_Weight_Unit, Status)
                                    VALUES
                                    ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                                    """.format(Brand, handle, None, None, None, None, None, None, None, None, None, None, None,
                                                a, b, c, d, e, f, cost_per_item, g, h, i, j, k, l, m, n, o, None))
                    except:
                        cursor.execute("""
                                    INSERT INTO ###.dbo.Shopify_Products (Brand, Handle, Title,  Vendor, Type, Tags, Published, Option1_Name, Option1_Value, Option2_Name, Option2_Value, Option3_Name, Option3_Value,
                                    Variant_SKU, Variant_Grams, Variant_Inventory_Tracker, Variant_Inventory_Qty, Variant_Inventory_Policy, Variant_Fulfillment_Service, Variant_Cost, Variant_Price, Variant_Compare_At_Price,
                                    Variant_Requires_Shipping, Variant_Taxable, Variant_Barcode, Image_Src, Image_Position, Image_Alt_Text, Variant_Weight_Unit, Status)
                                    VALUES
                                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                                    """,Brand, handle, None, None, None, None, None, None, None, None, None, None, None,
                                       a, b, c, d, e, f, cost_per_item, g, h, i, j, k, l, m, n, o, None)
                
            qwe+=1
            new+=1
            conn.commit()
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print (current_time," Shopify Products Added", len(json.loads(r.text)['products']))
    print (current_time," Shopify Lines Added", new)

conn, cursor = get_connection(user='###', Pass='###', IP='###', database='###')

Brands = ["###"]
APIKey = ["###"]
APIPW = ["###]
URLPart=["###]




now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print(current_time, " Start Update Products")



for i in range(11,12): #20

    cursor.execute("DELETE FROM Shopify_Products WHERE Brand='"+Brands[i]+"'")
    conn.commit()

    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(str(i)+".", current_time, "Brand",Brands[i],"=======================")  

    cursor.execute("SELECT Handle FROM Shopify_products WHERE Brand = '"+Brands[i]+"'")
    Handles_p = [each[0] for each in cursor.fetchall()]
    get_products_data(APIKey[i], APIPW[i], URLPart[i], conn, cursor, Handles_p,Brands[i])

    
