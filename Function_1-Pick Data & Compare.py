import csv
import pandas as pd
import os
import numpy as np
import gc
import time
# import _socket

cwd = os.getcwd()
Pre = os.path.join(cwd, r'Data\breast-cancer-wisconsin.data')
Post = os.path.join(cwd, r'Data\breast-cancer-wisconsin.data1')

df1 = pd.read_csv(Pre, delimiter=',', header=0, dtype=str)
df2 = pd.read_csv(Post, delimiter=',', header=0, dtype=str)

#--------------------------------------------------Ignore Columns in df1 & df2 - ------------------------------------------------------


ignore_attributes = ['marginal_adhesion']
for i in ignore_attributes:
    df1 = df1.drop(i, axis=1)
    df2 = df2.drop(i, axis=1)


#-------------------------------------------------- Comparison --------------------------------------------------------------------------
def comparison(df1, df2, key_cols):

    df3 = df1.merge(df2, on=key_cols, how='inner', suffixes=['_Data1', '_Data2'], indicator=True)
    #df3.to_csv(r'F:\Machine_Learning\Scalability_Services\dummy.csv')
    print("1st service ----> data comparison has started ..")
    time.sleep(2)
    print("..")
    time.sleep(2)
    print("Comparison in progress..")
    time.sleep(2)
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
    # print(list_header)
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

    print("completed writing results..")
    print('***************************************************************************')
    gc.collect()

    # print(df3.head)
    return df3

df3 = comparison(df1, df2, key_cols=['id'])
print("Data Comparison completed. Results written successfully")
time.sleep(2)
print("2nd Service ----> To perform Data Analysis is being called..")