from abc import ABC, abstractmethod
import json
class WebCrawler(ABC):
    @abstractmethod
    def get_pages(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    def load_to_csv(self,df,csv_path):
        df.to_csv(csv_path)
        pass
    
    def load_to_json(self,df,json_path):
        data = df.to_dict(orient="records")
        with open(json_path,'w', encoding='utf-8') as f:
            json.dump(data,f,indent=4,ensure_ascii=False)
    
    @abstractmethod
    def load_to_mongo(self,df):
        pass