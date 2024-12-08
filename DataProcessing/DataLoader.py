import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

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

df_alonhadat = pd.DataFrame(list(collection_alonhadat.find()))
df_bdsso = pd.DataFrame(list(collection_bds_so.find()))
df_bds_com_vn = pd.DataFrame(list(collection_bds_com_vn.find()))

websites = ['Alonhadat', 'BDS.so', 'BDS.com.vn']
statistics = [df_alonhadat.shape[0] * 2, df_bdsso.shape[0] * 2, df_bds_com_vn.shape[0]]
plt.figure(figsize=(8, 5))
plt.bar(websites, statistics)  # Optional: Adjust color for better visuals
plt.title('Website Statistics')
plt.xlabel('Websites')
plt.ylabel('Number of raw data crawled')
plt.show()