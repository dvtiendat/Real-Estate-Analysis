import logging
import pandas as pd
import yaml
import json
import os

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from WebCrawler.BDS_com_vn_Webcrawler import BDSWebCrawler
from WebCrawler.Alonhadat_Webcrawler import AlonhadatWebCrawler
from WebCrawler.BDS_So_Webcrawler import BDS_SoWebCrawler
from DataProcessing.transform import *      
from pipeline import *

os.makedirs('logs', exist_ok=True)
log_filename = './WebCrawler/logs/scraper.log'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(filename=log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

if __name__ == "__main__":
    pipeline = Pipeline(password='dmHUST')
    #pipeline.run(['BDS_SoCrawler', 'BDSWebCrawler', 'AlonhadatWebCrawler'])
    pipeline.run(['BDS_SoCrawler', 'BDSWebCrawler'], only_integrate=True)