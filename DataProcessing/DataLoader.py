import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://svbk:<pass>@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))
db = client['VietNameseRealEstateData']
collection_bds_com_vn = db['BDS_com_vn']
collection_bds_so = db['BDS_So']
collection_alonhadat = db['Alo_nha_dat']

#print(collection_bds_so)

bds_so_data = list(collection_bds_so.find())
alo_nha_dat_data = list(collection_alonhadat.find())
bds_com_vn_data = list(collection_bds_com_vn.find())

print(len(alo_nha_dat_data))
print(len(bds_com_vn_data))
print(len(bds_so_data))
df = pd.DataFrame(alo_nha_dat_data)
df = df.dropna(subset=['Mã tin'])
print(df.head(10))
print(df.info())
# print(bds_so_data)

# for item in bds_so_data:
#     print(item)