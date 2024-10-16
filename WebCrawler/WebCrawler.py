from abc import ABC, abstractmethod
class WebScraper(ABC):
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
    def load(self,df):
        pass