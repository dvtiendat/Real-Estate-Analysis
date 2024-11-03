from BDS_com_vn_Webcrawler import BDSWebCrawler
from Alonhadat_Webcrawler import AlonhadatWebCrawler
from BDS_So_Webcrawler import BDS_SoWebCrawler

import logging
import pandas as pd
import yaml
import json
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://svbk:<password>@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))

# setup logger
os.makedirs('logs', exist_ok=True)
log_filename = 'logs/scraper.log'
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

def run_crawler(config):
    '''
    Begin scraping procedure
    '''
    updated_config = config
    #crawler_names = ['BDS_SoCrawler' ,'BDSWebCrawler', 'AlonhadatWebCrawler'] # CHANGE THIS
    crawler_names = ['BDS_SoCrawler']
    # crawler_names = ['BDSWebCrawler']
    # crawler_names = ['AlonhadatWebCrawler']
    for crawler in crawler_names:
        logger.info(f'Starting crawler {crawler}')
        if crawler == 'BDSWebCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            for property_type, settings in config[crawler]['property_types'].items():
                try:
                    scraper = BDSWebCrawler(num_pages=settings['num_pages'], base_url=settings['base_url'])
                    df = scraper.multithread_extract(max_workers=1) # CHANGE MAX_WORKER

                    df['Loại'] = property_type
                    # df = scraper.transform(df)
                    final_df = pd.concat([final_df,df],ignore_index=True)

                    logger.info(f'Completed scraping {property_type} of {crawler}')
                    scraper.load_to_mongo(final_df, client)
                    logging.info(f'{crawler} data successfully pushed to DB')
                    updated_config[crawler]['property_types'][property_type]['start_page'] += updated_config[crawler]['property_types'][property_type]['num_pages']
                except Exception as e:
                    logger.error(f"Error occurred while scraping {property_type}: {str(e)}")
        elif crawler == 'AlonhadatWebCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            try:
                scraper = AlonhadatWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'])
                df = scraper.multithread_extract(max_workers=1)
                # df = scraper.transform(df)
                final_df = pd.concat([final_df,df],ignore_index=True)
                scraper.load_to_mongo(final_df, client)
                updated_config[crawler]['start_page'] += updated_config[crawler]['num_pages']
                logger.info(f'Completed scraping {crawler}')
                logging.info(f'{crawler} data successfully pushed to DB')
            except Exception as e:
                logger.error(f"Error occurred while scraping: {str(e)}")
        elif crawler == 'BDS_SoCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['final_columns'])
            try:
                scraper = BDS_SoWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'])
                df = scraper.multithread_extract(max_workers=1)
                # df = scraper.transform(df)
                final_df = pd.concat([final_df,df],ignore_index=True)
                updated_config[crawler]['start_page'] += updated_config[crawler]['num_pages']
                logger.info(f'Completed scraping {crawler}')
                # scraper.load_to_mongo(final_df, client)
                logging.info(f'{crawler} data successfully pushed to DB')
            except Exception as e:
                logger.error(f"Error occurred while scraping: {str(e)}")
    return updated_config, final_df

def main():
    try:
        # Get config
        config_path = 'config.yaml'
        config = get_config(config_path)
        
        # Run crawler
        updated_config, final_df = run_crawler(config)
        
        # Update config and save file 
        update_config(updated_config, config_path)
        output_path = f"../SampleData/bds_so_data.json" # CHANGE THIS 
        tmp = final_df.to_dict(orient="records")
        with open(output_path, 'w', encoding='utf8') as f:
            json.dump(tmp, f, indent = 2, ensure_ascii= False)
        # final_df.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
