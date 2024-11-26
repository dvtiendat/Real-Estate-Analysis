import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from unidecode import unidecode
uri = "mongodb+srv://svbk:dmHUST@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))
db = client['VietNameseRealEstateData']
# Are common attributes
def tranform(web):
    data = load_data(web)
    if web == 'Alo_nha_dat':
        transfomred_data = transform_alonhadat(data)
    elif web == 'BDS_com_vn':
        transfomred_data = transform_bdscomvn(data)
    else:
        transfomred_data = transform_bdsso(data)
    return transfomred_data
    
def load_data(web):
    assert web in ['BDS_So', 'BDS_com_vn', 'Alo_nha_dat'], "Web must be either ..."
    global db
    collection_bds_com_vn = db['BDS_com_vn']
    collection_bds_so = db['BDS_So']
    collection_alonhadat = db['Alo_nha_dat']
    return_data_frame = pd.DataFrame(list(db[web].find()))
    return return_data_frame

def convert_to_city(address):
    vietnam_provinces_cities = [
    # Northern Vietnam
    # Red River Delta
    "hà nội", "hải phòng", "bắc ninh", "hà nam", "hưng yên", 
    "nam định", "ninh bình", "thái bình", "vĩnh phúc", 
    "quảng ninh", "hải dương",
    
    # Northern Midlands and Mountains
    "hà giang", "cao bằng", "bắc kạn", "lạng sơn", 
    "tuyên quang", "thái nguyên", "phú thọ", "bắc giang", 
    "điện biên", "lai châu", "sơn la", "hòa bình", 
    "yên bái", "lào cai",

    # Central Vietnam
    # North Central Coast
    "thanh hóa", "nghệ an", "hà tĩnh", "quảng bình", 
    "quảng trị", "thừa thiên huế",
    
    # South Central Coast
    "đà nẵng", "quảng nam", "quảng ngãi", "bình định", 
    "phú yên", "khánh hòa", "ninh thuận", "bình thuận",

    # Central Highlands
    "kon tum", "gia lai", "đắk lắk", "đắk nông", "lâm đồng",

    # Southern Vietnam
    # Southeast
    "hồ chí minh", "bình dương", "đồng nai", 
    "bà rịa vũng tàu", "tây ninh", "bình phước",
    
    # Mekong River Delta
    "cần thơ", "long an", "tiền giang", "bến tre", 
    "trà vinh", "vĩnh long", "đồng tháp", "an giang", 
    "kiên giang", "hậu giang", "sóc trăng", "bạc liêu", "cà mau"
    ]

    district_mapping = {}
    for province in vietnam_provinces_cities:
        district_mapping[province] = [province]

    district_mapping['hồ chí minh'] = ["q4", "tân phong", "phường 5", "long trường", "đông hưng thuận", "lê văn quới", "thạnh lộc", "long thạnh mỹ", "phước long b", "bình trị đông a", "tăng nhơn phú a", "bình chánh" , "q10", "q6", "q9", "hóc môn", "bình hưng hòa", "thới an", "hcm", "củ chi","nhà bè" ,"hồ chí minh", "bình thạnh", "thủ đức", "gò vấp", "quận 1", "quận 2","quận 4", "quận 5","quận 6" ,"quận 7", "q7","quận 9", "q11","quận 12", "quận 3", "quận 8", "tân bình", "phú nhuận", "bình tân", "tân phú"]
    district_mapping['hà nội'] = ['nguyên hồng', 'ngô quyền quang trung', 'hồ đắc di', 'hồ tây', 'phú xuyên', "kcn quang minh", "văn quán", "bùi xưowng trạch", "mộ lao", "khương trung", "trương đinh", "bùi xuân trạch", "thanh trì", "triều khúc", "hoàn kiếm", "cổ linh", "chương mỹ", 'tân mai', "khương đình", "nguyễn trãi", "trương định", "thanh oai", "thanh trì","trung văn", "hoài đức", "trường chinh" , "hà nội", "đông anh", "long biên", "cầu giấy", "đống đa", "thanh xuân", "hai bà trưng", "bắc từ liêm", "nam từ liêm", "ba đình", "hoàng mai", "tây hồ", "gia lâm", "hà đông", "giáp bát", "phương liệt", "hoàng quốc việt", ]
    district_mapping['thừa thiên huế'] = ['thừa thiên huế', 'huế']
    district_mapping['đồng nai'] = ['đồng nai', 'biên hòa', 'bình đa', 'an bình', "tân hiệp", "tam hiệp", 'nhơn trạch', 'phường trảng dài']
    district_mapping['bình dương'] = ['bình dương', 'dĩ an', 'thuận an']
    district_mapping['quảng ninh'] = ['quảng ninh', 'hạ long', ]
    district_mapping['đà nẵng'] = ['an hải đông', 'hòa thọ đông', 'đà nẵng', 'hải châu', 'cẩm lệ', 'sơn trà']
    district_mapping['khánh hòa'] = ['khánh hòa', 'khánh hoà', 'nha trang']
    district_mapping['đắk lắk'] = ['đắk lắk', 'buôn ma thuột']
    district_mapping['bà rịa vũng tàu'] = ['bà rịa vũng tàu', 'vũng tàu', 'bà rịa']
    district_mapping['hải phòng'] = ['hải phòng', 'hải an']
    district_mapping['thanh hóa'] = ['thanh hóa', 'tân sơn'] 
    district_mapping['kiên giang'] = ['kiên giang', 'phú quốc']
    district_mapping['quảng nam'] = ['quảngt nam', 'quảng nam']
    if type(address) is float:
        return address
    tmp = address.lower().strip()
    for province in vietnam_provinces_cities:
        for t in district_mapping[province]:
            if tmp.find(t.strip()) != -1:
                return province
            if unidecode(tmp).find(unidecode(t)) != -1:
                return province
    with open("tmp.txt", "a", encoding = "utf8") as f:
        f.write(tmp + "\n")
    print(tmp)
    return 'KXĐ'

def transform_bdsso(data: pd.DataFrame) -> pd.DataFrame:
    data_modified = data.copy()
    data_modified = data_modified.dropna(subset=['Mã tin'])
    data_modified = data_modified.drop(columns=['_id', 'Mã tin', 'Ngày', 'Tháng', 'Năm'])
    data_modified.drop_duplicates(inplace=True, keep = 'first')
    data_modified['Thành phố'] = data_modified['Địa chỉ'].apply(convert_to_city)
    def convert_area(area):
        if pd.isnull(area):
            return None
        area = str(area).replace('\n', '').replace(',', '.').replace('m2', '').strip()
        try:
            return float(area)  
        except ValueError:
            return None
    data_modified['Diện tích'] = data_modified['Diện tích'].apply(convert_area)
    
    # Convert columns 'Mặt tiền' to numerical values, keep NaN as is
    numeric_columns = ['Mặt tiền','Lộ giới','Số tầng', 'Số phòng ngủ', 'Số toilet']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace(',', '.').replace('-', '.').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore') 
    
    # Replace full names with abbreviated direction names
    direction_mapping = {
        'đông': 'Đ',
        'tây': 'T',
        'nam': 'N',
        'bắc': 'B',
        'đông bắc': 'ĐB',
        'đông nam': 'ĐN',
        'tây bắc': 'TB',
        'tây nam': 'TN',
        'không xác định': 'KXĐ',
        'Đông - Nam': 'ĐN',
        'Tây - Bắc': 'TB',
        'Đông - Bắc': 'ĐB',
        'Đông - Nam': 'ĐN',
        'Tây - Nam': 'TN',
        'Đông': 'Đ',
        'Bắc': 'B',
        'Nam': 'N',
        'Tây': 'T'
    }
    data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(' '.join(x.replace('-', '').strip().lower().split()), 'KXĐ') if type(x) is not float else x)
    # data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: ' '.join(x.replace('-', '').strip().lower().split()) if type(x) is not float else x)

    # Hàm chuyển đổi giá trị cột mức giá
    def convert_price_to_billion(price_str, area):
        try:
            price_str = str(price_str).lower().replace(' ', '').replace(',', '.')
            if float(price_str)<100:
                return float(price_str)
            elif float(price_str)>=100:
                return (float(price_str) * area) / 1000 if area else None
        except ValueError:
            return None
        return None
    
    data_modified['Mức giá'] = data_modified.apply(lambda row: convert_price_to_billion(row['Mức giá'], row['Diện tích']), axis=1)
    data_modified['Loại'] = 'N'
    data_modified.rename(columns = {"Lộ giới": "Đường trước nhà"}, inplace = True)
    return data_modified

def transform_bdscomvn(data: pd.DataFrame) -> pd.DataFrame:
    data_modified = data.copy()
    data_modified = data_modified.dropna(subset=['Mã tin'])
    data_modified = data_modified.drop(columns=['_id', 'Mã tin', 'Ngày', 'Tháng', 'Năm'])
    data_modified.drop_duplicates(inplace=True, keep = 'first')
    # Hàm chuyển đổi giá trị cột diện tích
    def convert_area(area):
        if pd.isnull(area):
            return None
        area = str(area).replace('\n', '').replace('.','').replace(',', '.').replace('m²', '').strip()
        try:
            return float(area)  
        except ValueError:
            return None
    data_modified['Diện tích'] = data_modified['Diện tích'].apply(convert_area)
    
    # Replace nhà
    bds_mapping = {
        'Đất nền': 'Đ',
        'Nhà đất': 'N',
        'Chung cư': 'CC',
        'Căn hộ chung cư': 'CC',
        'Biệt thự, nhà liền kề': 'N',
        'Đất nền, liền kề, đất dự án': 'Đ'
    }
    data_modified['Loại'] = data_modified['Loại'].apply(lambda x: bds_mapping.get(x, x))
    data_modified['Thành phố'] = data_modified['Thành phố'].apply(convert_to_city)
    # Replace full names with abbreviated direction names
    direction_mapping = {
        'đông': 'Đ',
        'tây': 'T',
        'nam': 'N',
        'bắc': 'B',
        'đông bắc': 'ĐB',
        'đông nam': 'ĐN',
        'tây bắc': 'TB',
        'tây nam': 'TN',
        'không xác định': 'KXĐ',
        'Đông - Nam': 'ĐN',
        'Tây - Bắc': 'TB',
        'Đông - Bắc': 'ĐB',
        'Đông - Nam': 'ĐN',
        'Tây - Nam': 'TN',
        'Đông': 'Đ',
        'Bắc': 'B',
        'Nam': 'N',
        'Tây': 'T'
    }
    data_modified['Hướng ban công'] = data_modified['Hướng ban công'].apply(lambda x: direction_mapping.get(' '.join(x.replace('-', '').strip().lower().split()), 'KXĐ') if type(x) is not float else x)
    data_modified['Hướng nhà'] = data_modified['Hướng nhà'].apply(lambda x: direction_mapping.get(' '.join(x.replace('-', '').strip().lower().split()), 'KXĐ') if type(x) is not float else x)
    # data_modified['Hướng ban công'] = data_modified['Hướng ban công'].apply(lambda x: ' '.join(x.replace('-', '').strip().lower().split()) if type(x) is not float else x)
    
    # Convert columns 'Đường vào', 'Mặt tiền' to numerical values, keep NaN as is
    numeric_columns = ['Đường vào', 'Mặt tiền']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('m', '').replace(',', '.').replace('-', '.').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore')   
        
    # Xử lý cột tầng   
    numeric_columns = ['Số tầng']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('tầng', '').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore')   

    # Hàm chuyển đổi giá trị cột mức giá
    def convert_price_to_billion(price_str, area):
        try:
            price_str = str(price_str).lower().replace(' ', '').replace(',', '.')
            if 'tỷ' in price_str:
                return float(price_str.replace('tỷ', ''))
            elif 'triệu' in price_str:
                return (float(price_str.replace('triệu/m²', '')) * area) / 1000 if area else None
        except ValueError:
            return None
        return None
    
    data_modified['Mức giá'] = data_modified.apply(lambda row: convert_price_to_billion(row['Mức giá'], row['Diện tích']), axis=1)

    # Xử lý cột tầng   
    numeric_columns = ['Số toilet','Số phòng ngủ']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('phòng', '').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore')       
   
    data_modified.rename(columns = {"Hướng nhà": "Hướng", "Đường vào": "Đường trước nhà"}, inplace = True)
    # print(f"BDScomvn: {data_modified['Hướng'].unique().tolist()}")
    return data_modified
    
def transform_alonhadat(data: pd.DataFrame) -> pd.DataFrame:
    data_modified = data.copy()
    data_modified = data_modified.dropna(subset=['Mã tin'])
    data_modified = data_modified.drop(columns=['_id', 'Mã tin', 'Ngày', 'Tháng', 'Năm'])
    data_modified.drop_duplicates(inplace=True, keep = 'first')
    # Hàm chuyển đổi giá trị cột diện tích
    def convert_area(area):
        if pd.isnull(area):
            return None
        area = area.replace('\n', '').replace(',', '.').replace('.', '').replace('m2', '').strip()
        try:
            return float(area.split()[0])  
        except ValueError:
            return None

    # Hàm chuyển đổi giá trị cột mức giá
    def convert_price_to_billion(price_str, area):
        try:
            price_str = str(price_str).lower().replace(' ', '').replace(',', '.')
            if 'tỷ' in price_str:
                return float(price_str.replace('tỷ', ''))
            elif 'triệu' in price_str:
                return (float(price_str.replace('triệu', '')) * area) / 1000 if area else None
        except ValueError:
            return None
        return None
    
    # Hàm chuyển đổi giá trị cột mức giá
    data_modified['Diện tích'] = data_modified['Diện tích'].apply(convert_area)
    data_modified['Mức giá'] = data_modified.apply(lambda row: convert_price_to_billion(row['Mức giá'], row['Diện tích']), axis=1)
    data_modified['Thành phố'] = data_modified['Thành phố'].apply(convert_to_city)
    # Xử lý các cột phòng
    columns_to_process = ['Phòng ăn', 'Nhà bếp', 'Sân thượng', 'Chỗ để xe hơi', 'Chính chủ', 'Chổ để xe hơi']
    for col in columns_to_process:
        data_modified[col] = data_modified[col].fillna(0)
        data_modified[col] = data_modified[col].apply(lambda x: 1 if x == 'Có' else x).astype(int)
        
    # Convert columns 'Đường trước nhà', 'Số lầu', 'Số phòng ngủ' to numerical values, keep NaN as is
    numeric_columns = ['Đường trước nhà', 'Số lầu', 'Số phòng ngủ','Chiều ngang', 'Chiều dài']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('m', '').replace(',', '.').replace('-', '.').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore')   

    # Replace full names with abbreviated direction names
    # direction_mapping = {
    #     'đông': 'Đ',
    #     'tây': 'T',
    #     'nam': 'N',
    #     'bắc': 'B',
    #     'đông bắc': 'ĐB',
    #     'đông nam': 'ĐN',
    #     'tây bắc': 'TB',
    #     'tây nam': 'TN',
    #     'không xác định': 'KXĐ'
    # }
    # # data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(x.replace('-', '').strip().lower(), 'KXĐ') if type(x) is not float else x)
    # data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: ' '.join(x.replace('-', '').strip().lower().split()) if not pd.isnull(x) else x)

    # Replace nhà
    bds_mapping = {
        'Đất thổ cư, đất ở': 'Đ',
        'Nhà mặt tiền': 'N',
        'Nhà trong hẻm': 'N',
        'Căn hộ chung cư': 'CC',
        'Biệt thự, nhà liền kề': 'N',
        'Đất nền, liền kề, đất dự án': 'Đ'
    }
    data_modified['Loại BDS'] = data_modified['Loại BDS'].apply(lambda x: bds_mapping.get(x, x))

    # Hàm chuyển đổi giá trị cột diện tích
    def convert_area(area):
        if pd.isnull(area):
            return None
        area = str(area).replace('\n', '').replace(',', '.').replace('.', '').replace('m2', '').strip()
        try:
            return float(area.split()[0])  
        except ValueError:
            return None

    # Hàm chuyển đổi giá trị cột mức giá
    def convert_price_to_billion(price_str, area):
        try:
            price_str = price_str.lower().replace(' ', '').replace(',', '.')
            if 'tỷ' in price_str:
                return float(price_str.replace('tỷ', ''))
            elif 'triệu' in price_str:
                return (float(price_str.replace('triệu', '')) * area) / 1000 if area else None
        except Exception as e:
            print(price_str)
            return None
        return None
    
    # Hàm chuyển đổi giá trị cột mức giá
    data_modified['Diện tích'] = data_modified['Diện tích'].apply(convert_area)
    data_modified['Mức giá'] = data_modified.apply(lambda row: convert_price_to_billion(row['Mức giá'], row['Diện tích']), axis=1)

    # Xử lý các cột phòng
    columns_to_process = ['Phòng ăn', 'Nhà bếp', 'Sân thượng', 'Chỗ để xe hơi', 'Chính chủ', 'Chổ để xe hơi']
    for col in columns_to_process:
        data_modified[col] = data_modified[col].fillna(0)
        data_modified[col] = data_modified[col].apply(lambda x: 1 if x == 'Có' else x).astype(int)
        
    # Convert columns 'Đường trước nhà', 'Số lầu', 'Số phòng ngủ' to numerical values, keep NaN as is
    numeric_columns = ['Đường trước nhà', 'Số lầu', 'Số phòng ngủ','Chiều ngang', 'Chiều dài']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('m', '').replace(',', '.').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore')   
        

    # Replace full names with abbreviated direction names
    direction_mapping = {
        'đông': 'Đ',
        'tây': 'T',
        'nam': 'N',
        'bắc': 'B',
        'đông bắc': 'ĐB',
        'đông nam': 'ĐN',
        'tây bắc': 'TB',
        'tây nam': 'TN',
        'không xác định': 'KXĐ',
        'Đông - Nam': 'ĐN',
        'Tây - Bắc': 'TB',
        'Đông - Bắc': 'ĐB',
        'Đông - Nam': 'ĐN',
        'Tây - Nam': 'TN',
        'Đông': 'Đ',
        'Bắc': 'B',
        'Nam': 'N',
        'Tây': 'T'
    }
    data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(' '.join(x.replace('-', '').strip().lower().split()), 'KXĐ') if type(x) is not float else x)
    # data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: ' '.join(x.replace('-', '').strip().lower().split()) if type(x) is not float else x)

    # Replace nhà
    bds_mapping = {
        'Đất thổ cư, đất ở': 'Đ',
        'Nhà mặt tiền': 'N',
        'Nhà trong hẻm': 'N',
        'Căn hộ chung cư': 'CC'
    }
    data_modified['Loại BDS'] = data_modified['Loại BDS'].apply(lambda x: bds_mapping.get(x, x))
    data_modified.rename(columns = {"Số lầu": "Số tầng", "Đường vào": "Đường trước nhà", "Loại BDS": "Loại"}, inplace = True)
    return data_modified

if __name__ == "__main__":
    bds_com_vn_data = load_data('BDS_com_vn')
    bds_so_data = load_data('BDS_So')
    alo_nha_dat_data = load_data('Alo_nha_dat')
    # processed = transform_bdscomvn(data)
    bds_com_vn_data_processed = transform_bdscomvn(bds_com_vn_data)
    alo_nha_dat_data_processed = transform_alonhadat(alo_nha_dat_data)
    bds_so_data_processed = transform_bdsso(bds_so_data)
    final_df = pd.concat([bds_com_vn_data_processed, alo_nha_dat_data_processed, bds_so_data_processed], axis = 0)
    # print(alo_nha_dat_data_processed.columns)
    # print(bds_com_vn_data_processed.columns)
    # print(bds_so_data_processed.columns)
    print(final_df.columns.tolist())
    print(final_df.info())