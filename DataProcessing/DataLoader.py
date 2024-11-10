import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://svbk:dmHUST@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))
db = client['VietNameseRealEstateData']
collection_bds_com_vn = db['BDS_com_vn']
collection_bds_so = db['BDS_So']
collection_alonhadat = db['Alo_nha_dat']
final_collection = db['Final_Real_Estate']
#print(collection_bds_so)


final_data = list(final_collection.find())
df = pd.DataFrame(final_data)
# df = df.dropna(subset=['Mã tin'])
df_tmp = df.copy()
# df_tmp = df.drop(columns=['_id', 'Mã tin', 'Ngày', 'Tháng', 'Năm'])
df_tmp.drop_duplicates(inplace=True, keep = 'first')
print(df_tmp.head(10))
print(df_tmp.info())
# print(bds_so_data)

# for item in bds_so_data:
#     print(item)