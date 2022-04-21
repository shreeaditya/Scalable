import pandas as pd
import os
import csv
import numpy as np
import gc
import time

cwd = os.getcwd()

Pre = os.path.join(cwd, r'Data\breast-cancer-wisconsin.data')
Post = os.path.join(cwd, r'Data\breast-cancer-wisconsin.data1')
df1 = pd.read_csv(Pre, delimiter=',', header=0, dtype=str)
df2 = pd.read_csv(Post, delimiter=',', header=0, dtype=str)
#--------------------------------------------------Ignore Columns in df1 & df2 - ------------------------------------------------------

time.sleep(3)
print("2nd service ----> data analysis of the compared results has started ..")
time.sleep(1)
print("Analysis of the compared data in progress..")
time.sleep(3)
ignore_attributes = ['marginal_adhesion']
for i in ignore_attributes:
    df1 = df1.drop(i, axis=1)
    df2 = df2.drop(i, axis=1)


#-------------------------------------------------- Comparison --------------------------------------------------------------------------
def comparison(df1, df2, key_cols):

    df3 = df1.merge(df2, on=key_cols, how='inner', suffixes=['_Data1', '_Data2'], indicator=True)
    #df3.to_csv(r'F:\Machine_Learning\Scalability_Services\dummy.csv')
    l1 = []
    l2 = []
    l3 = list(df3)

    #Data1 - Old Data
    #Data2 - New Data
    for i in l3:
        if'_Data1' in i:
            l1.append(i)
        if '_Data2' in i:
            l2.append(i)
    myzip = zip(l1, l2)

    df3 = df3.replace(np.nan, '', regex=True)

    l_1 = list(df1)
    l_2 = list(df2)

    if len(l_1) != len(l_2):
        for col in list(df1):
            if col not in list(df2):
                df2[col + '_File2'] = 'Column missing in File2'

        for col in list(df2):
            if col not in list(df1):
                df1[col + '_File1'] = 'Column missing in File1'

    for (x, y) in myzip:
        df3[x + "_" + y + "_Result"] = df3[x] == df3[y]
    df3.rename(columns = {i : i + '_key_col' for i in key_cols}, inplace = True)

    #df3 = df3.sort_index(axis = 1)
    df3 = df3.reindex(sorted(df3.columns), axis=1)
    #df3.to_csv(r'F:\Machine_Learning\Scalability_Services\dummy.csv')
    gc.collect()
    del myzip
    del df1
    del df2

    list_header = list(df3)
    list_header.remove('_merge')
    list_header.insert(0, '_merge')
    list_header.insert(0, 'Index')
    print('******************************************')
    print(list_header)
    #df3.to_csv(r'F:\Machine_Learning\Scalability_Services\dummy.csv')
    list_merge =['both', 'left_only', 'right_only']

    #************************************************* Write comparison results *************************************************

    with open(os.path.join(cwd, r'Data\_Comparison_Result.csv'), 'w', newline='', encoding='utf-8') as resultFile:

        wr = csv.writer(resultFile)
        wr.writerows([list_header])
        for row in df3.itertuples(index=True, name='pandas'):
            row_list = list(row)
            wr.writerows([row_list])
            # print(row_list)
            # for i in list_merge:
            #     for i in row_list:
            #         row_list.remove(i)
            #         row_list.insert(1, i)
            #         wr.writerows([row_list])

    # print("completed writing results..")
    print('***************************************************************************')
    gc.collect()

    #print(df3.head)
    return df3

df3 = comparison(df1, df2, key_cols=['id'])
time.sleep(3)
print('..')
print('parameters are being analysed')
time.sleep(3)
#------------------------------------------------------------------------------------------------------------------------------------------

df_stats = pd.DataFrame(columns = ['Data_Feed_Name', 'Columns_Names_Check', 'Columns_Order_Check', 'row_count_check', 'Data_Validation', 'columns_count',
                                   'Columns_Failed'])
#----------------------------> Write Statistics <------------------------------------------------------

''' Check Col name and Col order'''

old_attr = list(df1)
new_attr = list(df2)
if old_attr == new_attr:
    x = 'Pass'
else:
    x = 'Fail'

'''Check record count '''

if len(df1) == len(df2):
    if len(df1) == 0:
        y = 'Fail - Not Compared' + ' Old_Data_Count = ' + str(len(df1)) + ' New_Data_Count = ' + str(len(df2))
    else:
        y = ' Pass' + ': Old_Data_Count = ' + str(len(df1)) + ' New_Data_Count = ' + str(len(df2))

else:
    if len(df1) == 0:
        y = 'Fail. Old Data file having 0 records - Not compared' + ':Old_Data_Count = ' + str(len(df1)) + ' New_Data_Count = ' + str(len(df2))
    elif len(df2) == 0:
        y = 'Fail. New Data file having 0 records - Not compared' + ':Old_Data_Count = ' + str(len(df1)) + ' New_Data_Count = ' + str(len(df2))
    else:
        y = 'Fail' + ':Old_Data_Count = ' + str(len(df1)) + 'New_Data_Count = ' + str(len(df2))

'''check Data validation'''

booleandf = df3.select_dtypes(include=[bool])
booleanDictionary = {True: 'TRUE', False: 'FALSE'}
for column in booleandf:
    df3[column] = df3[column].map(booleanDictionary)

if len(df3) > 0:
    df3_returned_1 = df3.loc[df3['_merge'] == 'both']
    if len(df3_returned_1) > 0:
        if 'FALSE' in df3_returned_1.values:
            x_val = 'Fail'
        else:
            x_val = 'Pass'
    else:
        x_val = 'No records to compare'
else:
    x_val = 'No records to compare'

'''check false columns and count'''
False_cols = []
False_cols_strip = []

if len(df3) > 0:
    df3_returned_1 = df3.loc[df3['_merge'] == 'both']
    if len(df3_returned_1) > 0:
        col_false_index = df3_returned_1.columns[df3_returned_1.eq('FALSE').any()]
        for i in col_false_index:
            False_cols.append(i)

        False_cols_strip.append([col_nam.split('_Old')[0] for col_nam in False_cols])
        #print(False_cols_strip)

    elif len(df3_returned_1) == 0:
        False_cols_strip = ['Files not compared as either of the files or both of the files have 0 records']

else:
    False_cols_strip = ['Files not compared as either of the files or both of the files have 0 records']

'''publish record counts with false'''

False_cols_row_count = []
for i in False_cols:
    i1 = i.split('_Data1')[0] + ' - ' + str(len(df3_returned_1[df3_returned_1[i] == 'FALSE']))
    False_cols_row_count.append(i1)

# last_char_index = file_set[0].rfind("_")
# new_string = file_set[0][:last_char_index]

if False_cols_strip[0] == 'Files not compared as either of the files or both of the files have 0 records':
    col_count = 'NOT COMPARED since no records'
    False_cols_row_count = ['Files not compared as either of the files or both of the files have 0 records']

else:
    col_count = str(len(False_cols_strip[0])) + '/' + str(len(old_attr)) + ' Failed in comparison'
z = 0
#feed1 = Pre[:Pre.rfind("/")]
feed1 = 'breast-cancer-wisconsin.data'
df_stats.loc[z] = [feed1, x, x, y, x_val, col_count, False_cols_row_count]
z += 1

#print(df_stats)
print("Dataframe to compute statistics is being written.")
time.sleep(2)
print('Data Analysis completed. Results of Data Analysis written successfully')
time.sleep(2)
print('3rd Service to insert data into Database is being called ---->')
# path = r'F:\Machine_Learning\Scalability_Services\Result_Statistics.xlsx'
df_stats.to_excel(os.path.join(cwd, 'Data\Result_Statistics.xlsx'), sheet_name = 'statistics')