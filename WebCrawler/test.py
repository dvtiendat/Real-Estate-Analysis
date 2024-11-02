import json 
import yaml

with open("../data/bds_com_vn_data.json", "r", encoding='utf8') as f:
    tmp = json.load(f)
print(len(tmp))
# path = 'config.yaml'
# with open(path, 'r', encoding='utf8') as f:
#     tmp = yaml.safe_load(f)
# print(tmp['BDSWebCrawler']['property_types']['Chung cư'])
# print(json.dumps(tmp, indent = 2, ensure_ascii=False))