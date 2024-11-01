import json 
import yaml

path = 'config.yaml'
with open(path, 'r', encoding='utf8') as f:
    tmp = yaml.safe_load(f)
print(tmp['BDSWebCrawler']['property_types']['Chung cư'])
# print(json.dumps(tmp, indent = 2, ensure_ascii=False))