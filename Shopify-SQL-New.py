import itertools
import requests
import csv
import json
import pyodbc

from datetime import datetime

def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format("ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    return conn, cursor
           
            
def get_line_item(link, conn, cursor, Handles_l, Brand, URLPart, APIKey, APIPW):
    s = requests.Session()
    New=0
    s.headers.update({'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
    r = s.get(link)
    while True:
        for x in json.loads(r.text)['orders']:
            try:
                _ID = x['id']
            except:
                _ID = ''
            try:
                created_at = x['created_at'].replace('T',' ')
            except:
                created_at = None
            try:
                updated_at = x['updated_at'].replace('T',' ')
            except:
                updated_at = None
            for y in x['line_items']:
                try:
                    Lineitem_quantity = int(y['quantity'])
                except:
                    Lineitem_quantity = 0
                try:
                    Lineitem_name = y['name']
                except:
                    Lineitem_name = ''
                try:
                    Lineitem_price = float(y['price'])
                except:
                    Lineitem_price = 0
                try:
                    Lineitem_sku = y['sku']
                except:
                    Lineitem_sku = ''
                try:
                    Lineitem_requires_shipping = int(y['requires_shipping'])
                except:
                    Lineitem_requires_shipping = 0
                try:
                    Lineitem_taxable = int(y['taxable'])
                except:
                    Lineitem_taxable = 0
                try:
                    Lineitem_fulfillment_status = y['fulfillment_status']
                except:
                    Lineitem_fulfillment_status = 'unfulfilled'
                try:
                    Item_ID = y['id']
                except:
                    Item_ID = ''
                    
                if str(Item_ID) not in Handles_l:

                    cursor.execute("""
                                INSERT INTO ###.dbo.Shopify_lineitems (Brand, Order_ID, Lineitem_quantity, Lineitem_name, Lineitem_price, Lineitem_sku,
                                Lineitem_requires_shipping, Lineitem_taxable, Lineitem_fulfillment_status, Item_ID, Create_Date, Update_Date)
                                VALUES
                                (?,?,?,?,?,?,?,?,?,?,?,?)
                                """, Brand,_ID, Lineitem_quantity, Lineitem_name[:255], Lineitem_price, Lineitem_sku, Lineitem_requires_shipping, Lineitem_taxable, Lineitem_fulfillment_status, Item_ID,
                                           created_at, updated_at)
                    New=New+1
                cursor.commit()
        if 'link' not in r.headers.keys():
            return New
            break
        elif 'rel="next"' not in r.headers['link']:
            return New
            break
        else:
            link = [each.split('<')[1].split('>')[0] for each in r.headers['link'].split(',') if 'rel="next"' in each][0]
        r = s.get(link.replace('https://'+URLPart+'.myshopify.com','https://'+APIKey+':'+APIPW+'@'+ URLPart+'.myshopify.com'))
            

def get_orders_data(link, conn, cursor, Handles_c, Brand, URLPart, APIKey, APIPW):
    s = requests.Session()
    New=0
    s.headers.update({'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'})
    r = s.get(link)
    while True:
        for x in json.loads(r.text)['orders']:
            try:
                created_at = x['created_at'].replace('T',' ')
            except:
                created_at = None
            try:
                updated_at = x['updated_at'].replace('T',' ')
            except:
                updated_at = None
            try:
                Name = x['name']
            except:
                Name = ''
            try:
                Email = x['email']
            except:
                Email = ''
            try:
                Financial_Status = x['financial_status']
            except:
                Financial_Status = ''
            try:
                Paid_at = x['processed_at'].replace('T',' ')
            except:
                Paid_at = ''
            try:
                Fulfillment_status = x['fulfillment_status']
            except:
                Fulfillment_status = 'unfulfilled'
            try:
                Fulfilled_at = x['closed_at'].replace('T',' ')
            except:
                Fulfilled_at = ''
            try:
                Accepts_Marketing = int(x['buyer_accepts_marketing'])
            except:
                Accepts_Marketing = 0
            try:
                Currency = x['currency']
            except:
                Currency = ''
            try:
                Subtotal = float(x['current_subtotal_price_set']['shop_money']['amount'])
            except:
                Subtotal = 0
            try:
                Shipping = float(x['total_shipping_price_set']['shop_money']['amount'])
            except:
                Shipping = 0
            try:
                Taxes = float(x['total_tax'])
            except:
                Taxes = 0
            try:
                Total = x['total_price']
            except:
                Total = ''
            try:
                Discount_code = x['discount_codes'][0]['code']
            except:
                Discount_code = ''
            try:
                Discount_amount = float(x['discount_codes'][0]['amount'])
            except:
                Discount_amount = 0
                
            try:
                Billing_Name = x['billing_address']['name']
            except:
                Billing_Name = ''
            try:
                Billing_Street = x['billing_address']['address1']
            except:
                Billing_Street = ''
            try:
                Billing_Address1 = x['billing_address']['address1']
            except:
                Billing_Address1 = ''
            try:
                Billing_Address2 = x['billing_address']['address2']
            except:
                Billing_Address2 = ''
            try:
                Billing_Company = x['billing_address']['company']
            except:
                Billing_Company = ''
            try:
                Billing_City = x['billing_address']['city']
            except:
                Billing_City = ''
            try:
                Billing_Zip = x['billing_address']['zip']
            except:
                Billing_Zip = ''
            try:
                Billing_Province = x['billing_address']['province']
            except:
                Billing_Province = ''
            try:
                Billing_Country = x['billing_address']['country']
            except:
                Billing_Country = ''
            try:
                Billing_Phone = x['billing_address']['phone']
            except:
                Billing_Phone = ''

            try:
                Shipping_Name = x['shipping_address']['name']
            except:
                Shipping_Name = ''
            try:
                Shipping_Street = x['shipping_address']['address1']
            except:
                Shipping_Street = ''
            try:
                Shipping_Address1 = x['shipping_address']['address1']
            except:
                Shipping_Address1 = ''
            try:
                Shipping_Address2 = x['shipping_address']['address2']
            except:
                Shipping_Address2 = ''
            try:
                Shipping_Company = x['shipping_address']['company']
            except:
                Shipping_Company = ''
            try:
                Shipping_City = x['shipping_address']['city']
            except:
                Shipping_City = ''
            try:
                Shipping_Zip = x['shipping_address']['zip']
            except:
                Shipping_Zip = ''
            try:
                Shipping_Province = x['shipping_address']['province']
            except:
                Shipping_Province = ''
            try:
                Shipping_Country = x['shipping_address']['country']
            except:
                Shipping_Country = ''
            try:
                Shipping_Phone = x['shipping_address']['phone']
            except:
                Shipping_Phone = ''
                
            try:
                Cancelled_at = x['cancelled_at'].replace('T',' ')
            except:
                Cancelled_at = ''
            try:
                Payment_Method = x['gateway']
            except:
                Payment_Method = ''
            try:
                _ID = x['id']
            except:
                _ID = ''
 
            if str(_ID) not in Handles_c:
                cursor.execute("""
                            INSERT INTO ###.dbo.Shopify_orders (Brand, Order_ID, Name, Email, Financial_Status, Paid_at, Fulfillment_status, Fulfilled_at, Accepts_Marketing, Currency, Subtotal, Shipping, Taxes, Total,
                            Discount_code, Discount_amount, Billing_Name, Billing_Street, Billing_Address1, Billing_Address2, Billing_Company, Billing_City, Billing_Zip, Billing_Province, Billing_Country, Billing_Phone,
                            Shipping_Name, Shipping_Street, Shipping_Address1, Shipping_Address2, Shipping_Company, Shipping_City, Shipping_Zip, Shipping_Province, Shipping_Country, Shipping_Phone,
                            Cancelled_at, Payment_Method, Create_Date, Update_Date)
                            VALUES
                            (?,?,left(?,100),left(?,150),?,?,?,?,?,?,?,?,?,?,left(?,255),?,left(?,100),left(?,255),left(?,255),left(?,255),left(?,255),left(?,255),left(?,50),left(?,100),left(?,100),left(?,100),left(?,100),left(?,255),left(?,255),left(?,255),
                            left(?,255),left(?,255),left(?,50),left(?,100),left(?,100),left(?,100),?,?,?,?)
                            """, Brand, _ID, Name, Email, Financial_Status, Paid_at, Fulfillment_status, Fulfilled_at, Accepts_Marketing, Currency, Subtotal, Shipping, Taxes, Total,
                            Discount_code, Discount_amount, Billing_Name[:100], Billing_Street, Billing_Address1, Billing_Address2, Billing_Company, Billing_City, Billing_Zip, Billing_Province, Billing_Country, Billing_Phone,
                            Shipping_Name[:100], Shipping_Street, Shipping_Address1, Shipping_Address2, Shipping_Company, Shipping_City, Shipping_Zip, Shipping_Province, Shipping_Country, Shipping_Phone,
                            Cancelled_at, Payment_Method, created_at, updated_at)
                New=New+1
                
            cursor.commit()
        
        if 'link' not in r.headers.keys():
            return New
            break
        elif 'rel="next"' not in r.headers['link']:
            return New
            break
        else:
            link = [each.split('<')[1].split('>')[0] for each in r.headers['link'].split(',') if 'rel="next"' in each][0]
        r = s.get(link.replace('https://'+URLPart+'.myshopify.com','https://'+APIKey+':'+APIPW+'@'+ URLPart+'.myshopify.com'))


conn, cursor = get_connection(user='###', Pass='###', IP='###', database='###')

Brands = ["###"]
APIKey = ["###"]
APIPW = ["###"]
URLPart=["###"]




now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print(current_time, " Start New Orders")


for i in range(0, 24): #19
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(str(i)+".", current_time, "Brand",Brands[i],"=======================")  

     
    cursor.execute("SELECT Max(Order_ID)  FROM Shopify_Orders WHERE (Brand = '"+Brands[i]+"')")
    try:
        MaxOrdGet = cursor.fetchone()
        MaxOrdID=int(MaxOrdGet[0])
    except:        
        MaxOrdID= 0
    
    
    print("Adding Orders starting",str(MaxOrdID))
    
    cursor.execute("SELECT Order_ID FROM Shopify_Orders WHERE Brand = '"+Brands[i]+"' AND Order_ID > "+str(MaxOrdID)  )
    Handles_c = [each[0] for each in cursor.fetchall()]
    New=get_orders_data('https://'+APIKey[i]+':'+APIPW[i]+'@'+ URLPart[i]+'.myshopify.com/admin/api/2021-04/orders.json?status=any&limit=250&since_id='+str(MaxOrdID), conn, cursor, Handles_c,Brands[i],URLPart[i], APIKey[i], APIPW[i])

    print("New Orders",New)

    
    cursor.execute("SELECT Max(order_ID) FROM Shopify_lineitems WHERE (Brand = '"+Brands[i]+"')")
    try:
        MaxOrdGet = cursor.fetchone()
        MaxOrdID=int(MaxOrdGet[0])
    except:        
        MaxOrdID= 0


    print("Adding Items Starting",str(MaxOrdID))

    cursor.execute("SELECT Item_ID FROM Shopify_lineitems WHERE Brand = '"+Brands[i]+"' AND order_ID > "+str(MaxOrdID) )
    Handles_l = [each[0] for each in cursor.fetchall()]
    New=get_line_item('https://'+APIKey[i]+':'+APIPW[i]+'@'+URLPart[i]+'.myshopify.com/admin/api/2021-04/orders.json?status=any&limit=250&since_id='+str(MaxOrdID), conn, cursor, Handles_l,Brands[i],URLPart[i], APIKey[i], APIPW[i])
    
    print("New Items",New)

    

