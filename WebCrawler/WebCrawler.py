from abc import ABC, abstractmethod

class WebCrawler(ABC):
    @abstractmethod
    def get_pages(self):
        pass

    @abstractmethod
    def extract(self):
        pass

    @abstractmethod
    def transform(self,df):
        pass

    @abstractmethod
    def load_to_csv(self,df):
        pass
    
    @abstractmethod
    def load_to_mongo(self,df):
        pass