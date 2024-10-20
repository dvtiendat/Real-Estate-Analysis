from BDS_com_vn_Webcrawler import BDSWebCrawler
from Alonhadat_Webcrawler import AlonhadatWebCrawler

import logging
import pandas as pd
import yaml
import traceback

logging.basicConfig(
    filename='logs/alonhadat_logs',
    encoding='utf-8',
    filemode='a',
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)

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
    crawler_names = ['AlonhadatWebCrawler']
    
    for crawler in crawler_names:
        logger.info(f'Starting crawler {crawler_names}')

        if crawler == 'BDSWebCrawler':
            final_df = pd.DataFrame(columns=config[crawler]['property_types'])
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
                final_df = pd.DataFrame()
                try:
                    scraper = AlonhadatWebCrawler(num_pages=config[crawler]['num_pages'], base_url=config[crawler]['base_url'])
                    df = scraper.multithread_extract(max_workers=1)
                    df = scraper.transform(df)
                    final_df = pd.concat([final_df,df],ignore_index=True)
                    logger.info(f'Completed scraping {crawler}')
                except Exception as e:
                    logger.error(f"Error occurred while scraping: {str(e)}")
                    logger.error("Traceback details:", exc_info=True)

    return final_df

def main():
    try:
        config = get_config('WebCrawler\config.yaml')
        final_df = run_crawler(config)
            
        output_path = f"data/alonhadat_data.csv"
        final_df.to_csv(output_path, index=False)
        logger.info(f"Data saved to {output_path}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
