from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from fake_useragent import UserAgent
import undetected_chromedriver as uc
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential
import time
from tqdm import tqdm
import pandas as pd
import numpy as np
from .WebCrawler import WebCrawler
from .utils import dms_to_decimal, scroll_shim

import logging
import datetime


cur_datetime = datetime.datetime.now()
cur_date = cur_datetime.day
cur_year = cur_datetime.year
cur_month = cur_datetime.month

previous_day = cur_datetime - datetime.timedelta(days=1)
prev_day = previous_day.day
prev_month = previous_day.month
prev_year = previous_day.year

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlonhadatWebCrawler(WebCrawler):
    def __init__(self, base_url, num_pages = None, start_page = 1) : 
        self.num_pages = num_pages
        self.base_url = base_url
        self.start_page = start_page
        if self.num_pages:
            logger.info(f"Initialized batdongsan.com.vn from page {start_page} to page {start_page + num_pages - 1 } and base URL: {base_url}")
        else :
            logger.info(f"Initialized batdongsan.com.vn from page {start_page} with base URL: {base_url}")

    def init_driver(self):
        
        options = Options()
        # options.add_argument("--headless")
        ua = UserAgent()
        user_agent = ua.random

        options.add_argument(f'--user-agent={user_agent}')
        driver = uc.Chrome(options=options)

        # opt = Options()
        # opt.add_argument("--headless")
        # driver = webdriver.Chrome(opt)
        driver.implicitly_wait(10)
        actions = ActionChains(driver)
        wait = WebDriverWait(driver, 10)

        return driver, actions, wait
    
        
    def get_pages(self,driver):
        pages = []
        page = self.start_page
        try :
            while True : 
                if page == 1 :
                    url = self.base_url
                else :
                    url = self.base_url[:-5] + '/trang--' + str(page) + '.html'
                # time.sleep(60)
                driver.get(url)
                driver.implicitly_wait(0.5) 
                links =  driver.find_elements(By.XPATH , value="//div[@class='thumbnail']//a") 
        
                for link in links:
                    pages.append(link.get_attribute('href'))
                page += 1 
                if self.num_pages:
                    if page >= self.start_page + self.num_pages:
                        logger.info(f'Process ended with total of {self.num_pages} pages')
                        break
        except Exception as e :
            logger.error(f'Process ended with total of {page-self.start_page} pages : {e}')
            return pages
        finally:
            driver.quit()
        return pages
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def extract(self, page):
        '''
        Extract data from a single page
        '''
        driver, actions, wait = self.init_driver()

        logger.info(f'Extracting from: {page}')

        columns = ['Mã tin', 'Ngày', 'Tháng', 'Năm','Phường','Quận','Thành phố','Mức giá','Diện tích','Hướng', 
           'Phòng ăn','Đường trước nhà','Nhà bếp', 'Loại BDS','Pháp lý',
           'Sân thượng', 'Chiều ngang' , 'Số lầu','Chổ để xe hơi','Chiều dài', 'Số phòng ngủ', 'Chính chủ']

        house_data = {col: np.nan for col in columns}

        if 'vaymuanha' in page:
            driver.quit()
            return None
        
        try:
            # time.sleep(90)
            driver.get(page)
            
            address = driver.find_element(By.XPATH, "//div[@class='address']//span[@class='value']").text.split(',')
            t  = driver.find_element(By.XPATH , "//span[@class='date']").text
            
            if 'Hôm nay' in t :
                house_data['Ngày'] = cur_date
                house_data['Tháng'] = cur_month
                house_data['Năm'] = cur_year
            elif 'Hôm qua' in t:
                house_data['Ngày'] = prev_day
                house_data['Tháng'] = prev_month
                house_data['Năm'] = prev_year
            else :
                date , month , year  = t.split()[1].split("/")
                house_data['Ngày'] = date
                house_data['Tháng'] = month
                house_data['Năm'] = year
            
            price = driver.find_element(By.XPATH, "//span[@class='price']//span[@class='value']").text
            area = driver.find_element(By.XPATH, "//span[@class='square']//span[@class='value']").text
            
            house_data['Mức giá'] = price
            house_data['Diện tích'] = area
                
                
            p = address[-3].strip().split(' ')
            if 'Phường' in p or 'Xã' in p or 'Thị trấn' in p :
                p = ' '.join(p[1:])
            else :
                p = ' '.join(p)
                
            q = address[-2].strip().split(' ')
            if 'Huyện' in q or 'Quận' in q :
                q = ' '.join(q[1:])
            else :
                q = ' '.join(q)
                    
            house_data['Phường'] = p
            house_data['Quận'] = q
            house_data['Thành phố'] = address[-1].strip()

            values = driver.find_elements(By.XPATH , "//td")
            for i in range(0,len(values),2):
                if i == 6  :
                    continue
                else : 
                    if len(values[i+1].text) == 0  : 
                        house_data[values[i].text] = 'Có'
                    elif values[i+1].text == '---' or values[i+1].text == '_' :
                        house_data[values[i].text] = np.nan
                    else:
                        house_data[values[i].text] = values[i+1].text
        except Exception as e:
            logger.error(f'Error occurred while extracting data from page {page}: {e}') 
            raise
        finally:
            driver.quit()
            return house_data
  

    def multithread_extract(self, max_workers=4):
        driver, _, _ = self.init_driver()
        pages = self.get_pages(driver)
        final_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_page  = {executor.submit(self.extract, page): page for page in pages}
            for f in concurrent.futures.as_completed(future_to_page):
                url = future_to_page[f]
                try:
                    data = f.result()
                    if data is not None:
                        final_data.append(data)
                except Exception as e:
                    logger.error(f'Fail to extract: {url} - {e}')
        
        df = pd.DataFrame(final_data)
        return df
    
    def load_to_mongo(self, df, client):
        db = client['VietNameseRealEstateData']
        collection = db['Alo_nha_dat']
        records = df.to_dict(orient='records')
        collection.insert_many(records)
        logger.info("Data loaded successfully")  
    
    