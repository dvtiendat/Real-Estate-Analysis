from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import concurrent.futures
from tenacity import retry, stop_after_attempt, wait_exponential
import time
from tqdm import tqdm
import pandas as pd
import numpy as np
from WebCrawler import WebCrawler
from utils import dms_to_decimal, scroll_shim
import logging
import datetime


cur_datetime = datetime.datetime.now()
cur_date = cur_datetime.day
cur_year = cur_datetime.year
cur_month = cur_datetime.month
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlonhadatWebCrawler(WebCrawler):
    def __init__(self, base_url, num_pages = None) : 
        self.num_pages = num_pages
        self.base_url = base_url
        if self.num_pages:
            logger.info(f"Initialized batdongsan.com.vn with {num_pages} pages and base URL: {base_url}")
        else :
            logger.info(f"Initialized batdongsan.com.vn base URL: {base_url}")

    def init_driver(self):
        opt = Options()
        # opt.add_argument("--headless")
        driver = webdriver.Chrome(opt)
        driver.implicitly_wait(10)
        actions = ActionChains(driver)
        wait = WebDriverWait(driver, 10)

        return driver, actions, wait
    
        
    def get_pages(self,driver):
        pages = []
        page = 1 
        try :
            while True : 
                if page == 1 :
                    url = self.base_url
                else :
                    url = self.base_url[:-5] + '/trang--' + str(page) + '.html'
                time.sleep(5)
                driver.get(url)
                driver.implicitly_wait(0.5) 
                links =  driver.find_elements(By.XPATH , value="//div[@class='thumbnail']//a") 
        
                for link in links:
                    pages.append(link.get_attribute('href'))
                page += 1 
                if self.num_pages:
                    if page >= self.num_pages:
                        logger.info(f'Process ended with total of {self.num_pages} pages')
                        break
        except Exception as e :
            logger.error(f'Process ended with total of {page} pages : {e}')
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
                time.sleep(5)
                driver.get(page)
                
                address = driver.find_element(By.XPATH, "//div[@class='address']//span[@class='value']").text.split(',')
                t  = driver.find_element(By.XPATH , "//span[@class='date']").text
                if 'Hôm nay' in t :
                    house_data['Ngày'] = cur_date
                    house_data['Tháng'] = cur_month
                    house_data['Năm'] = cur_year
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
                        if values[i+1] == '': 
                            house_data[values[i].text] = 'Có'
                        elif values[i+1] == '---':
                            house_data[values[i].text] = np.nan
                        else:
                            house_data[values[i].text] = values[i+1].text

                return house_data
                
        except Exception as e:
            logger.error(f'Error occurred while extracting data from page {page}: {e}') 
            raise
        # finally:
        #     driver.quit()
  

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
            
    
    def transform(self,df):
        
        df['Mã tin'] = [int(x) for x in (df['Mã tin'].to_list()) ]
        df['Số phòng ngủ'] = [float(x)  if  not isinstance(x,float) else np.nan for x in (df['Số phòng ngủ'].to_list()) ]
        df['Chiều ngang'] = [float(x[:-1].replace(',','.'))  if  not isinstance(x,float) else np.nan for x in (df['Chiều ngang'].to_list())  ] 
        df['Chiều dài'] = [float(x[:-1].replace(',','.'))  if  not isinstance(x,float) else np.nan for x in (df['Chiều dài'].to_list())   ] 
        df['Diện tích'] = [float(x.split()[0]) for x in (df['Diện tích'].to_list())]
        
    
        flag = df['Mức giá'].to_list()
        new_attribute = [] 
        for index,s in enumerate(flag) :           
            s = s.split()
            if 'thỏa' in s or 'Thỏa' in s :
                new_attribute.append(np.nan)
                continue 
            elif 'triệu' in s :
                new_attribute.append(float(s[0].replace(',','.')) / 1000)
            elif 'triệu/m²' in s :
                new_attribute.append( (float(s[0].replace(',','.')) * df.iloc[index]['Diện tích']) / 1000 )
            else :
                new_attribute.append(float(s[0].replace(',','.')))
        df['Mức giá'] = new_attribute
    
        date_cols = ['Ngày', 'Tháng', 'Năm']
        for col in date_cols:
            flag = df[col].to_list()
            new_attribute = []
            for x in flag :
                try:
                    if isinstance(x,str):
                        new_attribute.append(float(x))
                    else :
                        new_attribute.append(x)
                except Exception as e :
                    new_attribute.append(np.nan)
                    print(e)
            df[col] = new_attribute
        
        return df
    
    def load_to_mongo(self,df,client):
        db = client['VietNameseRealEstateData']
        collection = db['Alo_nha_dat']
        records = df.to_dict(orient='records')
        collection.insert_many(records)
        logger.info("Data loaded successfully")  
    
    def load_to_csv(self,df, csv_path):
        df = df.to_csv(csv_path)
    