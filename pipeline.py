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

os.makedirs('logs', exist_ok=True)
log_filename = './WebCrawler/logs/scraper.log'
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(filename=log_filename, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

def get_config(path):
    '''
    Load content of final columns in dataframe
    '''
    with open(path, 'r', encoding='utf8') as f:
        return yaml.safe_load(f)
    
def update_config(updated_config, path):
    '''
    Update the new config after scraping to config.yaml
    '''
    try:
        with open(path, 'w', encoding='utf8') as f:
            yaml.dump(updated_config, f, allow_unicode=True)
        logging.info("config file updated successfully!")
    except Exception as e:
        logging.info(f"config file could not be updated, caught {e}")

def load_data(db, web):
    assert web in ['BDS_So', 'BDS_com_vn', 'Alo_nha_dat'], "Web must be either ..."
    return_data_frame = pd.DataFrame(list(db[web].find()))
    return return_data_frame

class Pipeline():
    def __init__(self, 
                 password: str = "",
                 config_path: str = 'WebCrawler/config.yaml', 
                 crawler_names: list = ['BDS_SoCrawler' ,'BDSWebCrawler', 'AlonhadatWebCrawler'], 
                 web: list = ['BDS_So', 'BDS_com_vn', 'Alo_nha_dat'],
                 integrated_columns: list = ['Diện tích', 'Đường trước nhà', 'Mặt tiền','Số tầng', 'Số toilet','Số phòng ngủ', 'Loại', 'Hướng', 'Mức giá']):
        self.config_path = config_path
        self.crawler_names = crawler_names
        self.web = web
        self.config = get_config(self.config_path)
        self.crawler_to_web_names = {self.crawler_names[i]: self.web[i] for i in range(3)}
        self.uri = "mongodb+srv://svbk:<password>@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.uri = self.uri.replace("<password>", password) 
        self.client = MongoClient(self.uri, server_api = ServerApi('1'))
        self.db = self.client['VietNameseRealEstateData']
        self.integrated_columns = integrated_columns
        self.final_data = pd.DataFrame(columns=self.integrated_columns)
    def run_crawler(self, crawler_names):
        '''
        Begin scraping procedures
        '''
        config = self.config
        updated_config = config
        for crawler in crawler_names:
            assert crawler in self.crawler_names, "crawler_names must be a subset in ['BDS_SoCrawler' ,'BDSWebCrawler', 'AlonhadatWebCrawler']"
        try:
            # crawler_names = ['BDS_SoCrawler' ,'BDSWebCrawler', 'AlonhadatWebCrawler'] # CHANGE THIS
            # crawler_names = ['BDS_SoCrawler', 'BDSWebCrawler']
            # crawler_names = ['BDSWebCrawler']
            # crawler_names = ['AlonhadatWebCrawler', 'BDS_SoCrawler']
            for crawler in crawler_names:
                logger.info(f'Starting crawler {crawler}')
                if crawler == 'BDSWebCrawler':
                    final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
                    for property_type, settings in config[crawler]['property_types'].items():
                        try:
                            scraper = BDSWebCrawler(num_pages=settings['num_pages'], base_url=settings['base_url'], start_page=settings['start_page'])
                            df = scraper.multithread_extract(max_workers=1) # CHANGE MAX_WORKER

                            df['Loại'] = property_type
                            # df = scraper.transform(df)
                            final_df = pd.concat([final_df,df],ignore_index=True)

                            logger.info(f'Completed scraping {property_type} of {crawler}')
                            scraper.load_to_mongo(final_df, self.client)
                            logging.info(f'{crawler} data successfully pushed to DB')
                            updated_config[crawler]['property_types'][property_type]['start_page'] += updated_config[crawler]['property_types'][property_type]['num_pages']
                        except Exception as e:
                            logger.error(f"Error occurred while scraping {property_type}: {str(e)}")
                elif crawler == 'AlonhadatWebCrawler':
                    final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
                    try:
                        scraper = AlonhadatWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'], start_page=config[crawler]['start_page'])
                        df = scraper.multithread_extract(max_workers=1)
                        # df = scraper.transform(df)
                        final_df = pd.concat([final_df,df],ignore_index=True)
                        scraper.load_to_mongo(final_df, self.client)
                        updated_config[crawler]['start_page'] += updated_config[crawler]['num_pages']
                        logger.info(f'Completed scraping {crawler}')
                        logging.info(f'{crawler} data successfully pushed to DB')
                    except Exception as e:
                        logger.error(f"Error occurred while scraping: {str(e)}")
                elif crawler == 'BDS_SoCrawler':
                    final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
                    try:
                        scraper = BDS_SoWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'], start_page=config[crawler]['start_page'])
                        df = scraper.multithread_extract(max_workers=1)
                        # df = scraper.transform(df)
                        final_df = pd.concat([final_df,df],ignore_index=True)
                        updated_config[crawler]['start_page'] += updated_config[crawler]['num_pages']
                        logger.info(f'Completed scraping {crawler}')
                        scraper.load_to_mongo(final_df, self.client)
                        logging.info(f'{crawler} data successfully pushed to DB')
                    except Exception as e:
                        logger.error(f"Error occurred while scraping: {str(e)}")
            self.config = updated_config
            update_config(updated_config, self.config_path)
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
    
    def run_tranform_and_integrate(self, web_names: list):
        final_df = pd.DataFrame(columns=self.integrated_columns)
        for web in web_names:
            assert web in self.web, "web_names element must be in ['BDS_So', 'BDS_com_vn', 'Alo_nha_dat']"
        for web in web_names:
            data = tranform(web)
            added_data = data.loc[:, self.integrated_columns]
            final_df = pd.concat([final_df, added_data], axis = 0)
        self.final_data = pd.concat([self.final_data, final_df], axis = 0)

    def run(self, crawler_names, only_integrate = False):
        if only_integrate:
            web_names = [self.crawler_to_web_names[_] for _ in self.crawler_names]
        else:
            web_names = [self.crawler_to_web_names[_] for _ in crawler_names]
            self.run_crawler(crawler_names)
        self.run_tranform_and_integrate(web_names)
        self.push_to_db()
    
    def push_to_db(self):
        collection = self.db['Final_Real_Estate']
        collection.delete_many({})
        records = self.final_data.to_dict(orient='records')
        collection.insert_many(records)
        print("Data pushed to DB Successfully!!")

if __name__ == "__main__":
    pipeline = Pipeline(password='dmHUST')
    #pipeline.run(['BDS_SoCrawler', 'BDSWebCrawler', 'AlonhadatWebCrawler'])
    pipeline.run(['BDS_SoCrawler', 'BDSWebCrawler'])