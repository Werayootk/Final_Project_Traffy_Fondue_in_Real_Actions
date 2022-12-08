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
        kmeans = KMeans(4)
        kmeans.fit(x4)
        identified_clusters_x4 = kmeans.fit_predict(x4)
        data_with_clusters_x4 = df4.copy()
        data_with_clusters_x4['Cluster'] = identified_clusters_x4
        data_with_clusters_x4.to_excel(open(os.getcwd()+kwargs['path_save']+"ทางเท้า.xlsx", "wb"))


    except Exception as e:
        print(e)


