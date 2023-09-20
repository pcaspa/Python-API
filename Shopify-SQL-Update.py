import requests
import time
import pyodbc
from datetime import datetime


def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
        "ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    return conn, cursor


def update_line_item(json_data, Brand, cursor, Handle_e):
    try:
        Update_Date = json_data['updated_at']
    except:
        Update_Date = None
    for y in json_data['order']['line_items']:
        try:
            Lineitem_quantity = float(y['quantity'])
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
            Lineitem_requires_shipping = y['requires_shipping']
        except:
            Lineitem_requires_shipping = ''
        try:
            Lineitem_taxable = y['taxable']
        except:
            Lineitem_taxable = ''
        try:
            Lineitem_fulfillment_status = y['fulfillment_status']
        except:
            Lineitem_fulfillment_status = 'pending'
        try:
            Item_ID = y['id']
        except:
            Item_ID = ''
        #print (f'Updating Line Item id = {Item_ID} under order ID {Handle_e[0]}')
        cursor.execute("""
                        UPDATE ###.dbo.Shopify_lineitems SET Brand = ?, Lineitem_quantity = ?, Lineitem_name = ?, Lineitem_price = ?, Lineitem_sku = ?,
                        Lineitem_requires_shipping = ?, Lineitem_taxable = ?, Lineitem_fulfillment_status = ?,
                        Update_Date = ? WHERE Item_ID = ?""", Brand,  Lineitem_quantity, Lineitem_name, Lineitem_price, Lineitem_sku, Lineitem_requires_shipping,
                       Lineitem_taxable, Lineitem_fulfillment_status, Update_Date, str(Item_ID))
        conn.commit()


def update_orders_data(Handle_e, Brand, APIKey, APIPW, URLPart, cursor):

    headers = {
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36'}
    for qwert in range(1, 11, 1):
        try:
            r = requests.get('https://{APIKey}:{APIPW}@{URLPart}.myshopify.com/admin/api/2021-07/orders/{id_}.json'.format(
                APIKey=APIKey, APIPW=APIPW, URLPart=URLPart, id_=Handle_e[0]), headers=headers)
        except:
            r = None
        if r != None:
            if r.status_code == 200:
                break
            else:
                time.sleep(5)

    if r.status_code == 404:
        print(f'Order ID - {Handle_e[0]} = "Not Found"')
        return 0
    try:
        x = r.json()['order']
    except:
        return 0
    try:
        Update_Date = x['updated_at']
    except:
        Update_Date = None
    if Handle_e[1] == Update_Date:
        return 0
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
        Paid_at = x['processed_at'].replace('T', ' ')
    except:
        Paid_at = ''
    try:
        Fulfillment_status = x['fulfillment_status']
    except:
        Fulfillment_status = ''
    if Fulfillment_status == None:
        Fulfilled_at = None
    else:
        try:
            Fulfilled_at = x['fulfillments'][0]['created_at'].replace('T', ' ')
        except:
            Fulfilled_at = ''
    try:
        Accepts_Marketing = x['buyer_accepts_marketing']
    except:
        Accepts_Marketing = ''
    try:
        Currency = x['currency']
    except:
        Currency = ''
    try:
        Subtotal = float(x['current_subtotal_price_set']
                         ['shop_money']['amount'])
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
        Total = float(x['total_price'])
    except:
        Total = 0
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

    Shipping_Phone = ''

    try:
        Cancelled_at = x['cancelled_at'].replace('T', ' ')
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
    print(Name)
    cursor.execute("""
                        UPDATE ###.dbo.Shopify_orders SET Brand = ?,Name = ?, Email = ?, Financial_Status = ?, Paid_at = ?, Fulfillment_status = ?, Fulfilled_at = ?,
                        Accepts_Marketing = ?, Currency = ?, Subtotal = ?, Shipping = ?, Taxes = ?, Total = ?,
                        Discount_code = ?, Discount_amount = ?, Billing_Name = ?, Billing_Street = ?, Billing_Address1 = ?, Billing_Address2 = ?, Billing_Company = ?,
                        Billing_City = ?, Billing_Zip = ?, Billing_Province = ?, Billing_Country = ?, Billing_Phone = ?,
                        Shipping_Name = ?, Shipping_Street = ?, Shipping_Address1 = ?, Shipping_Address2 = ?, Shipping_Company = ?, Shipping_City = ?, Shipping_Zip = ?,
                        Shipping_Province = ?, Shipping_Country = ?, Shipping_Phone = ?, Cancelled_at = ?, Payment_Method = ?,
                        Update_Date = ? WHERE Order_ID = ?""", Brand, Name, Email, Financial_Status, Paid_at, Fulfillment_status, Fulfilled_at, Accepts_Marketing,
                   Currency, Subtotal, Shipping, Taxes, Total, Discount_code, Discount_amount, Billing_Name, Billing_Street,
                   Billing_Address1, Billing_Address2, Billing_Company, Billing_City, Billing_Zip, Billing_Province, Billing_Country, Billing_Phone,
                   Shipping_Name, Shipping_Street, Shipping_Address1[:255], Shipping_Address2, Shipping_Company, Shipping_City, Shipping_Zip,
                   Shipping_Province, Shipping_Country, Shipping_Phone, Cancelled_at, Payment_Method, Update_Date, str(_ID))
    conn.commit()
    print(Name)
    update_line_item(r.json(), Brand, cursor, Handle_e)
    return 1


conn, cursor = get_connection(
    user='###', Pass='###', IP='###', database='###')

Brands = ["###"]
APIKey = ["###"]
APIPW = ["###"]
URLPart = ["###"]


now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print(current_time, " Start Update Orders")

for i in range(2, 24):  # 20
    if i == 1:
        i = 8
    new = 0
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    print(
        f'{str(i)}. {current_time} Brand {Brands[i]} =======================')

    cursor.execute("SELECT Order_ID, Update_Date FROM qryShopify_Orders1 WHERE Brand = '" +
                   Brands[i]+"'  AND (Fulfilled_at IS NULL OR Fulfilled_at = N'') AND (Cancelled_at IS NULL OR Cancelled_at = N'') AND (DATEDIFF(d, { fn NOW() }, Create_Date2) > - 10) ")
    Handles_c = [[each[0], each[1]] for each in cursor.fetchall()]
    for Handle_e in Handles_c:
        ret = update_orders_data(
            Handle_e, Brands[i], APIKey[i], APIPW[i], URLPart[i], cursor)
        new = new+ret

    print(new, " Orders Updated")
