import json

data_path = 'data/alonhatdat.json'

with open(data_path, "r", encoding = 'utf8') as f:
    data = json.load(f)
print('\n'.join([str(x) for x in data]))