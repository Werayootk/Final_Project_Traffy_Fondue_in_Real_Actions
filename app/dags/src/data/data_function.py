import json
import pandas as pd
import numpy as np
import os
from sklearn.cluster import KMeans

def save_data_kmean(**kwargs):
    try:
        df = pd.read_excel(open(os.getcwd()+kwargs['path_read']+"raw.xlsx", "rb"))
        df = df.dropna(subset = ['type'])
        df2 = df.set_index(['Unnamed: 0', 'message_id', 'type_id', 'org', 'comment', 'ticket_id', 'coords', 'photo', 'after_photo', 'address', 'district', 'subdistrict', 'province', 'timestamp', 'problem_type_abdul', 'status', 'star', 'count_reopen', 'state']).apply(lambda x: x.str.split(',').explode()).reset_index()
        df2[["long", "lat"]] = df2["coords"].str.strip(r"[[]]").str.replace("'","").str.split(",", expand=True).astype(np.str)
        df2 = df2.astype({'lat':'float','long':'float'})
        no_type = ['ร้องเรียน','สอบถาม','เสนอแนะ']
        df3 = df2[~df2['type'].isin(no_type)]
        df3['date']= pd.to_datetime(df3['timestamp']).apply(lambda x: x.date())
        drop_list = ['Unnamed: 0', 'message_id', 'type_id', 'org', 'comment', 'coords', 'photo','after_photo', 'address', 'district', 'subdistrict', 'province', 'timestamp', 'problem_type_abdul', 'star', 'count_reopen', 'state']
        df3 = df3.drop(drop_list, axis=1)
        df3 = df3[df3['status'] != 'finish']

        df4 = df3[df3['type'] == 'ทางเท้า']
        x4 = df4.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x4)
        identified_clusters_x4 = kmeans.fit_predict(x4)
        data_with_clusters_x4 = df4.copy()
        data_with_clusters_x4['Cluster'] = identified_clusters_x4
        data_with_clusters_x4.to_excel(open(os.getcwd()+kwargs['path_save']+"ทางเท้า.xlsx", "wb"))

        df5 = df3[df3['type'] == 'ความสะอาด']
        x5 = df5.iloc[:,3:5]
        kmeans = KMeans(8)
        kmeans.fit(x5)
        identified_clusters_x5 = kmeans.fit_predict(x5)
        data_with_clusters_x5 = df5.copy()
        data_with_clusters_x5['Cluster'] = identified_clusters_x5
        data_with_clusters_x5.to_excel(open(os.getcwd()+kwargs['path_save']+"ความสะอาด.xlsx", "wb"))

        df6 = df3[df3['type'] == 'ถนน']
        x6 = df6.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x6)
        identified_clusters_x6 = kmeans.fit_predict(x6)
        data_with_clusters_x6 = df6.copy()
        data_with_clusters_x6['Cluster'] = identified_clusters_x6
        data_with_clusters_x6.to_excel(open(os.getcwd()+kwargs['path_save']+"ถนน.xlsx", "wb"))

        df7 = df3[df3['type'] == 'คลอง']
        x7 = df7.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x7)
        identified_clusters_x7 = kmeans.fit_predict(x7)
        data_with_clusters_x7 = df7.copy()
        data_with_clusters_x7['Cluster'] = identified_clusters_x7
        data_with_clusters_x7.to_excel(open(os.getcwd()+kwargs['path_save']+"คลอง.xlsx", "wb"))

        df8 = df3[df3['type'] == 'สะพาน']
        x8 = df8.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x8)
        identified_clusters_x8 = kmeans.fit_predict(x8)
        data_with_clusters_x8 = df8.copy()
        data_with_clusters_x8['Cluster'] = identified_clusters_x8
        data_with_clusters_x8.to_excel(open(os.getcwd()+kwargs['path_save']+"สะพาน.xlsx", "wb"))

        df9 = df3[df3['type'] == 'น้ำท่วม']
        x9 = df9.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x9)
        identified_clusters_x9 = kmeans.fit_predict(x9)
        data_with_clusters_x9 = df9.copy()
        data_with_clusters_x9['Cluster'] = identified_clusters_x9
        data_with_clusters_x9.to_excel(open(os.getcwd()+kwargs['path_save']+"น้ำท่วม.xlsx", "wb"))

        df10 = df3[df3['type'] == 'แสงสว่าง']
        x10 = df10.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x10)
        identified_clusters_x10 = kmeans.fit_predict(x10)
        data_with_clusters_x10 = df10.copy()
        data_with_clusters_x10['Cluster'] = identified_clusters_x10
        data_with_clusters_x10.to_excel(open(os.getcwd()+kwargs['path_save']+"แสงสว่าง.xlsx", "wb"))

        df11 = df3[df3['type'] == 'ห้องน้ำ']
        x11 = df11.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x11)
        identified_clusters_x11 = kmeans.fit_predict(x11)
        data_with_clusters_x11 = df11.copy()
        data_with_clusters_x11['Cluster'] = identified_clusters_x11
        data_with_clusters_x11.to_excel(open(os.getcwd()+kwargs['path_save']+"ห้องน้ำ.xlsx", "wb"))

        df12 = df3[df3['type'] == 'เสียงรบกวน']
        x12 = df12.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x12)
        identified_clusters_x12 = kmeans.fit_predict(x12)
        data_with_clusters_x12 = df12.copy()
        data_with_clusters_x12['Cluster'] = identified_clusters_x12
        data_with_clusters_x12.to_excel(open(os.getcwd()+kwargs['path_save']+"เสียงรบกวน.xlsx", "wb"))

        df13 = df3[df3['type'] == 'ความปลอดภัย']
        x13 = df13.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x13)
        identified_clusters_x13 = kmeans.fit_predict(x13)
        data_with_clusters_x13 = df13.copy()
        data_with_clusters_x13['Cluster'] = identified_clusters_x13
        data_with_clusters_x13.to_excel(open(os.getcwd()+kwargs['path_save']+"ความปลอดภัย.xlsx", "wb"))

        df14 = df3[df3['type'] == 'กีดขวาง']
        x14 = df14.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x14)
        identified_clusters_x14 = kmeans.fit_predict(x14)
        data_with_clusters_x14 = df14.copy()
        data_with_clusters_x14['Cluster'] = identified_clusters_x14
        data_with_clusters_x14.to_excel(open(os.getcwd()+kwargs['path_save']+"กีดขวาง.xlsx", "wb"))
        
        df15 = df3[df3['type'] == 'ป้าย']
        x15 = df15.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x15)
        identified_clusters_x15 = kmeans.fit_predict(x15)
        data_with_clusters_x15 = df15.copy()
        data_with_clusters_x15['Cluster'] = identified_clusters_x15
        data_with_clusters_x15.to_excel(open(os.getcwd()+kwargs['path_save']+"ป้าย.xlsx", "wb"))

        df16 = df3[df3['type'] == 'จราจร']
        x16 = df16.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x16)
        identified_clusters_x16 = kmeans.fit_predict(x16)
        data_with_clusters_x16 = df16.copy()
        data_with_clusters_x16['Cluster'] = identified_clusters_x16
        data_with_clusters_x16.to_excel(open(os.getcwd()+kwargs['path_save']+"จราจร.xlsx", "wb"))

        df17 = df3[df3['type'] == 'สายไฟ']
        x17 = df17.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x17)
        identified_clusters_x17 = kmeans.fit_predict(x17)
        data_with_clusters_x17 = df17.copy()
        data_with_clusters_x17['Cluster'] = identified_clusters_x17
        data_with_clusters_x17.to_excel(open(os.getcwd()+kwargs['path_save']+"สายไฟ.xlsx", "wb"))

        df18 = df3[df3['type'] == 'สัตว์จรจัด']
        x18 = df18.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x18)
        identified_clusters_x18 = kmeans.fit_predict(x18)
        data_with_clusters_x18 = df18.copy()
        data_with_clusters_x18['Cluster'] = identified_clusters_x18
        data_with_clusters_x18.to_excel(open(os.getcwd()+kwargs['path_save']+"สัตว์จรจัด.xlsx", "wb"))

        df19 = df3[df3['type'] == 'ต้นไม้']
        x19 = df19.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x19)
        identified_clusters_x19 = kmeans.fit_predict(x19)
        data_with_clusters_x19 = df19.copy()
        data_with_clusters_x19['Cluster'] = identified_clusters_x19
        data_with_clusters_x19.to_excel(open(os.getcwd()+kwargs['path_save']+"ต้นไม้.xlsx", "wb"))

        df20 = df3[df3['type'] == 'ท่อระบายน้ำ']
        x20 = df20.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x20)
        identified_clusters_x20 = kmeans.fit_predict(x20)
        data_with_clusters_x20 = df20.copy()
        data_with_clusters_x20['Cluster'] = identified_clusters_x20
        data_with_clusters_x20.to_excel(open(os.getcwd()+kwargs['path_save']+"ท่อระบายน้ำ.xlsx", "wb"))

        df21 = df3[df3['type'] == 'ป้ายจราจร']
        x21 = df21.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x21)
        identified_clusters_x21 = kmeans.fit_predict(x21)
        data_with_clusters_x21 = df21.copy()
        data_with_clusters_x21['Cluster'] = identified_clusters_x21
        data_with_clusters_x21.to_excel(open(os.getcwd()+kwargs['path_save']+"ป้ายจราจร.xlsx", "wb"))

        df22 = df3[df3['type'] == 'PM2.5']
        x22 = df22.iloc[:,3:5]
        kmeans = KMeans(2)
        kmeans.fit(x22)
        identified_clusters_x22 = kmeans.fit_predict(x22)
        data_with_clusters_x22 = df22.copy()
        data_with_clusters_x22['Cluster'] = identified_clusters_x22
        data_with_clusters_x22.to_excel(open(os.getcwd()+kwargs['path_save']+"PM2.5.xlsx", "wb"))

        df23 = df3[df3['type'] == 'การเดินทาง']
        x23 = df23.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x23)
        identified_clusters_x23 = kmeans.fit_predict(x23)
        data_with_clusters_x23 = df23.copy()
        data_with_clusters_x23['Cluster'] = identified_clusters_x23
        data_with_clusters_x23.to_excel(open(os.getcwd()+kwargs['path_save']+"การเดินทาง.xlsx", "wb"))

        df24 = df3[df3['type'] == 'คนจรจัด']
        x24 = df24.iloc[:,3:5]
        kmeans = KMeans(6)
        kmeans.fit(x24)
        identified_clusters_x24 = kmeans.fit_predict(x24)
        data_with_clusters_x24 = df24.copy()
        data_with_clusters_x24['Cluster'] = identified_clusters_x24
        data_with_clusters_x24.to_excel(open(os.getcwd()+kwargs['path_save']+"คนจรจัด.xlsx", "wb"))

    except Exception as e:
        print(e)


