import requests
from requests.auth import HTTPBasicAuth
import pyodbc


def get_connection(user, Pass, IP, database):
    conn = pyodbc.connect("DRIVER={};SERVER={};DATABASE={};UID={};PWD={}".format(
        "ODBC Driver 17 for SQL Server", IP, database, user, Pass))
    cursor = conn.cursor()
    conn.autocommit = True
    return conn, cursor


def parser(user, password, existing_order_ids):
    i = 0
    j = 10000
    New = 0
    r = requests.get(f'https://###-group.willdoo.it/api/v1/custom/SalesOrders?offset={i}&limit={j}',
                     auth=HTTPBasicAuth(user, password))
    while True:
        if r.json()['result'] == []:
            break
        i += 10000
        for x in r.json()['result']:
            try:
                activity_state = x['activity_state']
            except:
                activity_state = None
            try:
                amount_tax = x['amount_tax']
            except:
                amount_tax = None
            try:
                amount_total = x['amount_total']
            except:
                amount_total = None
            try:
                commitment_date = x['commitment_date']
            except:
                commitment_date = None
            try:
                create_date = x['create_date']
            except:
                create_date = None
            try:
                date_order = x['date_order']
            except:
                date_order = None
            try:
                delivery_message = x['delivery_message']
            except:
                delivery_message = None
            try:
                delivery_rating_success = x['delivery_rating_success']
            except:
                delivery_rating_success = None
            try:
                display_name = x['display_name']
            except:
                display_name = None
            try:
                is_presale_order = x['is_presale_order']
            except:
                is_presale_order = None
            try:
                name = x['name']
            except:
                name = None
            #try:
            #  order_line = x['order_line']
            #except:
            #    order_line = None
            try:
                state = x['state']

            except:
                state = None
            try:
                id_ = x['id']
            except:
                id_ = None
            if str(id_) not in existing_order_ids:
                cursor.execute("""
                INSERT INTO BL360.dbo.Odoo_SalesOrder (activity_state,amount_tax,amount_total,commitment_date,create_date,date_order,delivery_message, delivery_rating_success, display_name,id,is_presale_order,name,state)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                               activity_state, amount_tax, amount_total, commitment_date, create_date, date_order, delivery_message, delivery_rating_success, display_name, id_, is_presale_order, name,state)

                New += 1
                
        conn.commit()
        
        r = requests.get(f'https://###-group.willdoo.it/api/v1/custom/SalesOrders?offset={i}&limit={j}',
                         auth=HTTPBasicAuth(user, password))
    print(f'records added {New}')

conn, cursor = get_connection(
    user='###', Pass='###', IP='103.1.205.55\SQLEXPRESS,1141', database='###')


sql_query = """
DELETE FROM xqryOdoo_SalesOrder
FROM            qryShopify_Orders1 INNER JOIN
                         xqryOdoo_SalesOrder ON qryShopify_Orders1.Name = xqryOdoo_SalesOrder.name
WHERE        (qryShopify_Orders1.Create_Date2 > CONVERT(DATETIME, '2023-03-01 00:00:00', 102)) AND (LEFT(qryShopify_Orders1.Brand, 2) = 'OO' OR
                         LEFT(qryShopify_Orders1.Brand, 2) = 'ZT') AND (ISNULL(qryShopify_Orders1.Cancelled_at, N'') <> N'') AND (xqryOdoo_SalesOrder.state = N'Sale')
                         """
cursor.execute(sql_query)


conn, cursor = get_connection(
    user='###', Pass='###', IP='###', database='BL360')
cursor.execute("SELECT id from Odoo_SalesOrder;")
existing_order_ids = [str(each[0]) for each in cursor.fetchall()]

parser('TheAdministrator', '6XusHFF0X7tJaR6cHPn7WLvSK', existing_order_ids)
