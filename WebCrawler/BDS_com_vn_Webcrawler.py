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

from tqdm import tqdm
import pandas as pd
import numpy as np
from WebCrawler.WebCrawler import WebScraper
from utils import dms_to_decimal, scroll_shim
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BDSWebCrawler(WebScraper):
    def __init__(self, num_pages, base_url) : 
        self.num_pages = num_pages
        self.base_url = base_url
        logger.info(f"Initialized batdongsan.com.vn with {num_pages} pages and base URL: {base_url}")

    def init_driver(self):
        opt = Options()
        # opt.add_argument("--headless")
        driver = webdriver.Chrome(opt)
        driver.implicitly_wait(10)
        actions = ActionChains(driver)
        wait = WebDriverWait(driver, 10)

        return driver, actions, wait
    
    def get_pages(self, driver):
        
        pages = []
        
        for page in tqdm(range(1,self.num_pages+1)):
            if page == 1 :
                url = self.base_url
            else :
                url = self.base_url + '/p' + str(page)
            driver.get(url)
            driver.implicitly_wait(0.5) 
            links =  driver.find_elements(By.XPATH , value="//a[@class='js__product-link-for-product-id']") 
    
            for link in links:
                pages.append(link.get_attribute('href'))
        
        driver.quit()
        return pages
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def extract(self, page):
        '''
        Extract data from a single page
        '''
        driver, actions, wait = self.init_driver()

        logger.info(f'Extracting from: {page}')

        columns = ['Ngày', 'Tháng', 'Năm', 'Kinh độ', 'Vĩ độ','Phường','Quận','Thành phố','Mức giá','Diện tích','Mặt tiền' ,'Hướng nhà', 
           'Số tầng','Số toilet','Đường vào', 'Hướng ban công','Số phòng ngủ',
           'Pháp lý', 'Nội thất']

        house_data = {col: np.nan for col in columns}

        if 'vaymuanha' in page:
            driver.quit()
            return None
        
        try:
                driver.get(page)
                
                address = driver.find_element(By.XPATH, "//span[@class='re__pr-short-description js__pr-address']").text.split(',')
                date, month, year = driver.find_element(By.XPATH , "//div[@class='re__pr-short-info re__pr-config js__pr-config']//div[1]").text.splitlines()[1].split("/")

                iframe = driver.find_elements(By.XPATH, '//iframe[@class="lazyload"]')[0]
                scroll_shim(driver,iframe)
                actions.move_to_element(iframe).perform()
                driver.implicitly_wait(10)
                iframe = driver.find_element(By.XPATH, '//iframe[@class=" lazyloaded"]')    
                driver.switch_to.frame(frame_reference=iframe)
                longitude , latitude = driver.find_element(By.XPATH , "//div[@class='place-name']").text.split(' ')
                driver.switch_to.default_content()

                    
                house_data['Kinh độ'] = longitude
                house_data['Vĩ độ'] = latitude
                house_data['Ngày'] = date
                house_data['Tháng'] = month
                house_data['Năm'] = year
                    
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

                keys = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='re__pr-specs-content-item-title']")))
                values = wait.until(EC.presence_of_all_elements_located((By.XPATH, "//span[@class='re__pr-specs-content-item-value']")))
                for index, key in enumerate(keys):
                    house_data[key.text] = values[index].text

                return house_data
                
        except Exception as e:
            logger.error(f'Error occurred while extracting data from page {page}: {e}') 
            raise
        finally:
            driver.quit()
  

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
        cols_to_change = ['Diện tích','Mặt tiền' , 'Số tầng','Số toilet','Số phòng ngủ', 'Đường vào']

        for col in cols_to_change:
            flag = df[col].to_list()
            new_attribute = []

            for s in flag:
                try:
                    if s != np.nan and  (not isinstance(s,float)):
                        new_attribute.append(float(s.split()[0].replace(',','.')))
                    else :
                        new_attribute.append(np.nan)
                except ValueError:
                    new_attribute.append(np.nan)

            df[col] = new_attribute
    
        flag = df['Mức giá'].to_list()
        new_attribute = [] 
        for index,s in enumerate(flag) :
            s = s.split()
            
            try :
                if 'triệu/m²' in s :
                    new_attribute.append( (float(s[0].replace(',','.')) * df.iloc[index]['Diện tích']) / 1000 )
                else :
                    new_attribute.append(float(s[0].replace(',','.')))
            except ValueError:
                new_attribute.append(np.nan)
        df['Mức giá'] = new_attribute
        
        flag = df['Kinh độ'].to_list()
        flag = [dms_to_decimal(x) for x in flag ]
        df['Kinh độ'] = flag
        
        flag = df['Vĩ độ'].to_list()
        flag = [dms_to_decimal(x) for x in flag ]
        df['Vĩ độ'] = flag 
    
        date_cols = ['Ngày', 'Tháng', 'Năm']
        for col in date_cols:
            flag = df[col].to_list()
            flag = [float(x) for x in flag]
            df[col] = flag
        
        
        return df
    
    def load(self,df, csv_path):
        df = df.to_csv(csv_path)