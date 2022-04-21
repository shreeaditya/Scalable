import pandas as pd
import cx_Oracle as orcCon
from cx_Oracle import DatabaseError
import os
cwd = os.getcwd()

print("3rd Service ----> Data insertion to push the compared data into Database is started")
Loadresults = pd.read_csv(os.path.join(cwd, r'Data\_Comparison_Result.csv'), nrows=100)
Loadresults = Loadresults.drop('Index', axis=1)

try:
    #orcCon.connect('username/password@localhost')
    conn = orcCon.connect('testuser/testuser@dataanalysis.ccqua7310dhr.us-east-1.rds.amazonaws.com:1521/DATABASE')
    if conn:
        print("cx_Oracle version:", orcCon.version)
        print("Database version:", conn.version)
        print("Client version:", orcCon.clientversion())
        cursor = conn.cursor()
        print("You're connected: ")
        print('Inserting the analysed data into table....')
        for i,row in Loadresults.iterrows():
            # print(i)
            # print(row)
            sql = "INSERT INTO comparison_table(col1, col2, col3,col4,col5,col6,col7,col8,col9,col10,col11,col12,col13,col14,col15,col16,col17,col18," \
                  "col19,col20,col21,col22,col23,col24,col25,col26,col27,col28,col29)" \
                  "VALUES (:2,:3,:4,:5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, " \
                  ":26, :27, :28, :29, :30)"
            cursor.execute(sql, tuple(row))
            #print(cursor)
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
    print("4th Service ----> Data insertion to push the Statistics of Data Analysis into Database is being called..")