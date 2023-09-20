import os
import pandas as pd # pip install pandas
import pypyodbc as odbc # pip install pypyodbc 
from Google import Create_Service

"""
Getting Dataset from Google Sheets
"""
FOLDER_PATH = r'###'
CLIENT_SECRET_FILE = os.path.join(FOLDER_PATH, '###.json')
API_NAME = 'sheets'
API_VERSION = 'v4'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

#https://docs.google.com/spreadsheets/d/1K2LbPFFPaDSU_eFxg8ybRWEtLI5hg7-RWQdXEIJIchg/edit#gid=0
#https://docs.google.com/spreadsheets/d/1K2LbPFFPaDSU_eFxg8ybRWEtLI5hg7-RWQdXEIJIchg/edit#gid=2136824790

google_sheets_id = '1Bq0hkGaXmDDcW9BC73ZHYDOxaShmRCCKxiCmf2r8tRM'

response = service.spreadsheets().values().get(
    spreadsheetId=google_sheets_id,
    majorDimension='ROWS',
    range='Input-SOH_BY_BIN'
).execute()

rows = response['values'][1:]


"""
Push Dataset to SQL Server (database system)
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

# connection object creation
try:    
    conn = odbc.connect(connection_string(USER_NAME,PASSWORD,DRIVER_NAME, SERVER_NAME, DATABASE_NAME))
    print('Connection created')    
except odbc.DatabaseError as e:
    print('Database Error:')
    print(str(e.value[1]))
except odbc.Error as e:
    print('Connection Error')
    print(str(e.value[1]))
else:
    cursor = conn.cursor()
    cursor.execute('DELETE FROM Fulfilio_SOH_BY_BIN')
    sql_insert = """
        INSERT INTO Fulfilio_SOH_BY_BIN
        VALUES(?,?,?,?,?,?)
    """    
    try:
        cursor.executemany(sql_insert, rows)
        cursor.commit()
        print('Data import complete')
    except Exception as e:
        print(str(e.value[1]))
        cursor.rollback()
    finally:
        cursor.close()
        conn.close()
