import json 

test = [
    {
        "Tôi": "Tạ Tuấn Hải",
        "Giới tính": "Nam",
        "Sở thích": "Chơi game", 
    }
]

with open("./test.json", "w", encoding = 'utf8') as f:
    json.dump(test, f, indent = 2, ensure_ascii=False)