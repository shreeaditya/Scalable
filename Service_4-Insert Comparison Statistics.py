import pandas as pd
import cx_Oracle as orcCon
from cx_Oracle import DatabaseError
import os
cwd = os.getcwd()
import time

Loadresults = pd.read_excel(os.path.join(cwd, r'Data\Result_Statistics.xlsx'))
print(list(Loadresults))
Loadresults = Loadresults.drop('Unnamed: 0', axis=1)
# print(Loadresults.head)
print("4th Service ----> Data insertion to push the Statistics of Data Analysis into Database is started")

try:
    #orcCon.connect('username/password@localhost')
    conn = orcCon.connect('testuser/testuser@dataanalysis.ccqua7310dhr.us-east-1.rds.amazonaws.com:1521/DATABASE')
    if conn:
        print("cx_Oracle version:", orcCon.version)
        print("Database version:", conn.version)
        print("Client version:", orcCon.clientversion())
        cursor = conn.cursor()
        print("You're connected: ")
        print('Inserting this data\'s statistics into table....')
        for i,row in Loadresults.iterrows():
            sql = "INSERT INTO STATISTICS(col1, col2, col3,col4,col5,col6,col7) VALUES (:1,:2,:3,:4,:5,:6,:7)"
            cursor.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our changes
        conn.commit()
        print("Records after data analysis are inserted succesfully")
        cursor.close()
        conn.close()
except DatabaseError as e:
    err, = e.args
    print("Oracle-Error-Code:", err.code)
    print("Oracle-Error-Message:", err.message)
finally:
    print("closing connection")
    print("4th Service ----> Data insertion to push the Statistics of Data Analysis into Database is completed")
    time.sleep(2)
    print("Below services called and executed..")
    time.sleep(2)
    print("1. Data Comparison Service")
    time.sleep(2)
    print("2. Data Analysis Service")
    time.sleep(2)
    print("3. Comparison Data Transfer Service")
    time.sleep(2)
    print("4. Stats data transfer Service")
