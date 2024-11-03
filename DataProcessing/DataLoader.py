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

#print(collection_bds_so)

bds_so_data = list(collection_bds_so.find())
print(len(bds_so_data))
df = pd.DataFrame(bds_so_data)

print(df.head(10))
# print(bds_so_data)

# for item in bds_so_data:
#     print(item)