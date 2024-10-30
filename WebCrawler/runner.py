from BDS_com_vn_Webcrawler import BDSWebCrawler
from Alonhadat_Webcrawler import AlonhadatWebCrawler
from BDS_So_Webcrawler import BDS_SoWebCrawler

import logging
import pandas as pd
import yaml
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://svbk:<PASSSWORD>@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))

# setup logger
os.makedirs('logs', exist_ok=True)
log_filename = 'logs/bds_com.log'
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
    

def run_crawler(config):
    '''
    Begin scraping procedure
    '''
    # crawler_names = ['BDS_SoCrawler'] # CHANGE THIS
    crawler_names = ['AlonhadatWebCrawler']
    for crawler in crawler_names:
        logger.info(f'Starting crawler {crawler}')

        if crawler == 'BDSWebCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            for property_type, settings in config[crawler]['property_types'].items():
                try:
                    scraper = BDSWebCrawler(num_pages=settings['num_pages'], base_url=settings['base_url'])
                    df = scraper.multithread_extract(max_workers=1) # CHANGE MAX_WORKER

                    df['Loại'] = property_type
                    df = scraper.transform(df)
                    final_df = pd.concat([final_df,df],ignore_index=True)

                    logger.info(f'Completed scraping {property_type} of {crawler}')
                except Exception as e:
                    logger.error(f"Error occurred while scraping {property_type}: {str(e)}")
        elif crawler == 'AlonhadatWebCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            try:
                scraper = AlonhadatWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'])
                df = scraper.multithread_extract(max_workers=1)
                #df = scraper.transform(df)
                final_df = pd.concat([final_df,df],ignore_index=True)
                logger.info(f'Completed scraping {crawler}')
            except Exception as e:
                logger.error(f"Error occurred while scraping: {str(e)}")
        elif crawler == 'BDS_SoCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            try:
                scraper = BDS_SoWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'])
                df = scraper.multithread_extract(max_workers=1)
                df = scraper.transform(df)
                final_df = pd.concat([final_df,df],ignore_index=True)
                logger.info(f'Completed scraping {crawler}')
            except Exception as e:
                logger.error(f"Error occurred while scraping: {str(e)}")
    return final_df

def main():
    try:
        config = get_config('WebCrawler\config.yaml')
        final_df = run_crawler(config)
            
        output_path = f"alonhadat_data.json" # CHANGE THIS 
        final_df.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
