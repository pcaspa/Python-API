from Google import Create_Service # link to source code is in the description
import pypyodbc as odbc # pip install pypyodbc
import pandas as pd # pip install pandas
import os

"""
Step 1.1 Connect to MS SQL Server Database System
"""
DRIVER_NAME = 'ODBC Driver 17 for SQL Server' #'SQL Server'
SERVER_NAME = '103.1.205.55\SQLEXPRESS,1141'
DATABASE_NAME = '###'
USER_NAME = '###'
PASSWORD = '###'


def connection_string(username,password,driver_name, server_name, database_name):
    conn_string = f"""
        uid={username};
        pwd={password};
        DRIVER={{{driver_name}}};
        SERVER={server_name};
        DATABASE={database_name};
        Trust_Connection=yes;        
    """
    return conn_string

try:
    conn = odbc.connect(connection_string(USER_NAME,PASSWORD,DRIVER_NAME, SERVER_NAME, DATABASE_NAME))
    #conn, cursor = get_connection(user='###', Pass='###', IP='103.1.205.55\SQLEXPRESS,1141', database='###')
    print('Connection Created')
except odbc.DatabaseError as e:
    print('Database Error:')
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error:')
    print(str(e.value[1]))
else:
    # sql_query = """
    # SELECT TOP 2000 Traffic_Report_Id, Issue_Reported, Published_Date, Issue_Reported
    # FROM Austin_Traffic_Incident
    # """

    sql_query = """
SELECT     Company AS warehouse_id, Customer AS owner_id, Name AS Order_ID, '' as cust_id,  Order_type, 'UNITRANSHD' AS carrier_ID, CONVERT(varchar(10), Create_Date2, 103) AS date_ordered, '' AS delivery_date, row_number() OVER (PARTITION BY Name ORDER BY Create_Date2) AS order_line, 
                         CAST(Lineitem_sku AS nvarchar(20)) AS Item_ID, Lineitem_quantity AS pieces_ordered, '' AS pieces_to_pick, '' AS cust_code, Shipping_Name AS s_company, Shipping_Address1 AS s_address1, 
                         Shipping_Address2 AS s_address2, '' as s_address3, '' as s_address4, Shipping_City as s_address5, 
                         replace(replace(replace(replace(replace(replace(replace(Shipping_Province,'New South Wales','NSW'),'Victoria','VIC'),'Queensland','QLD'),'Tasmania','TAS'),'Northern Territory','NT'),'South Australia','SA'),'Western Australia','WA') AS s_state, 
                         CAST(Shipping_Zip AS nvarchar(10)) AS s_zip, '' AS [customer's PO Number], '' AS Other, 'N' AS kit, 
                         RIGHT(REPLACE(REPLACE('+' + REPLACE(REPLACE(REPLACE(Shipping_Phone, '+', ''), '+61', '0'), ' ', ''), '+61', '0'), '+', '0'), 10) AS Phone, Email, '' AS DeliveryNote, 
                         RIGHT(REPLACE(REPLACE('+' + REPLACE(REPLACE(REPLACE(Billing_Phone, '+', ''), '+61', '0'), ' ', ''), '+61', '0'), '+', '0'), 10) AS Mobile
FROM            qryUnitTrans_Unfulfilled
ORDER BY Create_Date2
        
    """
    
    #, CAST(lic AS NVARCHAR(10)) AS lic, CAST(age AS NVARCHAR(10)) AS age,

    cursor = conn.cursor()
    # cursor.execute(sql_query)
    cursor.execute(sql_query)

    """
    Step 1.2 Retrieve Dataset from SQL Server
    """
    recordset = cursor.fetchall()

    columns = [col[0].upper() for col in cursor.description]

    #pd.set_option('display.width',1000)

    df = pd.DataFrame(recordset, columns=columns)

    if 'Date' in df.columns:
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    recordset = df.values.tolist()


    """
    Step 2. Export Dataset to Google Spreadsheets
    """

    #https://docs.google.com/spreadsheets/d/1Myg5YqTTIYMntzODsuk88i0mCzhAfsuSOaB-WMvM46o/edit#gid=1284573425
    #https://docs.google.com/spreadsheets/d/1K2LbPFFPaDSU_eFxg8ybRWEtLI5hg7-RWQdXEIJIchg/edit#gid=651506013

    gs_sheet_id = '1rU3jagomesU3gu13amQnI5m53_xztmWKKspr9VOlegU'
    #tab_id = '1284573425'
    
    FOLDER_PATH = r'C:\Users\Windows PC\Dropbox\###\Python'
    CLIENT_SECRET_FILE = os.path.join(FOLDER_PATH, 'client_secret_889752874664-7v5kfcu435tt36td5a064o84c7ahs68i.apps.googleusercontent.com.json')
    
    API_NAME = 'sheets'
    API_VERSION = 'v4'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

    # create spreadsheets reference object
    mySpreadsheets = service.spreadsheets().get(
        spreadsheetId=gs_sheet_id
    ).execute()

    recordset

    tab_name = 'Sheet1'
    #[sheet['properties']['title'] for sheet in mySpreadsheets['sheets'] if sheet['properties']['sheetId'] == tab_id][0]
    
    """
    Clear workshete content
    """
    service.spreadsheets().values().clear(
        spreadsheetId=gs_sheet_id,
        range=tab_name
    ).execute()


    """
    Insert dataset
    """
    def construct_request_body(value_array, dimension: str='ROWS') -> dict:
        try:
            request_body = {
                'majorDimension': dimension,
                'values': value_array
            }
            return request_body
        except Exception as e:
            print(e)
            return {}

    """
    Insert column names
    """

    request_body_columns = construct_request_body([columns])
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A1',
        body=request_body_columns
    ).execute()
    


    """
    Insert rows
    """
    request_body_values = construct_request_body(recordset)
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A2',
        body=request_body_values
    ).execute()

    print('Task is complete')

    exit()

    #+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

    sql_query = """
SELECT        Brand, CAST(Lineitem_sku AS NVARCHAR(10)) AS SKU, CAST(ROUND(LastMoSales, 0) AS NVARCHAR(10)) AS July_Sales, CAST(ROUND(M0, 0) AS NVARCHAR(10)) AS Aug_21, CAST(ROUND(M1, 0) AS NVARCHAR(10)) AS Sep_21, 
                         CAST(ROUND(M2, 0) AS NVARCHAR(10)) AS Oct_21, CAST(ROUND(M3, 0) AS NVARCHAR(10)) AS Nov_21, CAST(ROUND(M4, 0) AS NVARCHAR(10)) AS Dec_21, CAST(ROUND(M5, 0) AS NVARCHAR(10)) AS Jan_22, CAST(ROUND(M6, 
                         0) AS NVARCHAR(10)) AS Mar_22, CAST(ROUND(M8, 0) AS NVARCHAR(10)) AS Apr_22, CAST(ROUND(M9, 0) AS NVARCHAR(10)) AS May_22, CAST(ROUND(M10, 0) AS NVARCHAR(10)) AS Jun_22, CAST(ROUND(M11, 0) 
                         AS NVARCHAR(10)) AS Jul_22
FROM            ForeCast_Proj2
ORDER BY LastMoSales DESC
    """
    

    cursor.execute(sql_query)

    """
    Step 1.2 Retrieve Dataset from SQL Server
    """
    recordset = cursor.fetchall()

    columns = [col[0].upper() for col in cursor.description]

    #pd.set_option('display.width',1000)

    df = pd.DataFrame(recordset, columns=columns)


    """
    Step 2. Export Dataset to Google Spreadsheets
    """

    #https://docs.google.com/spreadsheets/d/1Myg5YqTTIYMntzODsuk88i0mCzhAfsuSOaB-WMvM46o/edit#gid=1284573425
    #https://docs.google.com/spreadsheets/d/1K2LbPFFPaDSU_eFxg8ybRWEtLI5hg7-RWQdXEIJIchg/edit#gid=651506013

        
    tab_name = 'AU - Forecast-PC'
    
    """
    Clear workshete content
    """
    service.spreadsheets().values().clear(
        spreadsheetId=gs_sheet_id,
        range=tab_name
    ).execute()


    """
    Insert dataset
    """
    def construct_request_body(value_array, dimension: str='ROWS') -> dict:
        try:
            request_body = {
                'majorDimension': dimension,
                'values': value_array
            }
            return request_body
        except Exception as e:
            print(e)
            return {}

    """
    Insert column names
    """
    request_body_columns = construct_request_body([columns])
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A1',
        body=request_body_columns
    ).execute()

    """
    Insert rows
    """
    request_body_values = construct_request_body(recordset)
    service.spreadsheets().values().update(
        spreadsheetId=gs_sheet_id,
        valueInputOption='USER_ENTERED',
        range=f'{tab_name}!A2',
        body=request_body_values
    ).execute()

    print('Task is complete')


    exit()
  
