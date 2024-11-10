import pandas as pd
import numpy as np
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

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

def transform_bdsso(data: pd.DataFrame) -> pd.DataFrame:
    global integrated_columns
    data_modified = data.copy()
    data_modified = data_modified.dropna(subset=['Mã tin'])
    data_modified = data_modified.drop(columns=['_id', 'Mã tin', 'Ngày', 'Tháng', 'Năm'])
    data_modified.drop_duplicates(inplace=True, keep = 'first')
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
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace(',', '.').strip() if pd.notnull(x) else x)
        data_modified[col] = pd.to_numeric(data_modified[col], errors='ignore') 
        
    # Replace full names with abbreviated direction names
    direction_mapping = {
        'Đông': 'Đ',
        'Tây': 'T',
        'Nam': 'N',
        'Bắc': 'B',
        'Đông Bắc': 'ĐB',
        'Đông Nam': 'ĐN',
        'Tây Bắc': 'TB',
        'Tây Nam': 'TN',
        'Không xác định': 'KXĐ'
    }
    data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(x.strip(), x.strip()) if type(x) is not float else x)

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
    global integrated_columns
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
        'Chung cư': 'CC'  
    }
    data_modified['Loại'] = data_modified['Loại'].apply(lambda x: bds_mapping.get(x, x))

    # Replace full names with abbreviated direction names
    direction_mapping = {
        'Đông': 'Đ',
        'Tây': 'T',
        'Nam': 'N',
        'Bắc': 'B',
        'Đông - Bắc': 'ĐB',
        'Đông - Nam': 'ĐN',
        'Tây - Bắc': 'TB',
        'Tây - Nam': 'TN'
    }
    data_modified['Hướng ban công'] = data_modified['Hướng ban công'].apply(lambda x: direction_mapping.get(x.strip(), x.strip()) if type(x) is not float else x)

    # Convert columns 'Đường vào', 'Mặt tiền' to numerical values, keep NaN as is
    numeric_columns = ['Đường vào', 'Mặt tiền']
    for col in numeric_columns:
        data_modified[col] = data_modified[col].fillna(np.nan)
        data_modified[col] = data_modified[col].apply(lambda x: str(x).replace('m', '').replace(',', '.').strip() if pd.notnull(x) else x)
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
    return data_modified
    
def transform_alonhadat(data: pd.DataFrame) -> pd.DataFrame:
    global integrated_columns
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
        'Đông': 'Đ',
        'Tây': 'T',
        'Nam': 'N',
        'Bắc': 'B',
        'Đông Bắc': 'ĐB',
        'Đông Nam': 'ĐN',
        'Tây Bắc': 'TB',
        'Tây Nam': 'TN',
        'Không xác định': 'KXĐ'
    }
    data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(x.strip(), x.strip()) if type(x) is not float else x)

    # Replace nhà
    bds_mapping = {
        'Đất thổ cư, đất ở': 'Đ',
        'Nhà mặt tiền': 'N',
        'Nhà trong hẻm': 'N',
        'Căn hộ chung cư': 'CC'
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
        'Đông': 'Đ',
        'Tây': 'T',
        'Nam': 'N',
        'Bắc': 'B',
        'Đông Bắc': 'ĐB',
        'Đông Nam': 'ĐN',
        'Tây Bắc': 'TB',
        'Tây Nam': 'TN',
        'Không xác định': 'KXĐ'
    }
    data_modified['Hướng'] = data_modified['Hướng'].apply(lambda x: direction_mapping.get(x.strip(), x.strip()) if type(x) is not float else x)

    # Replace nhà
    bds_mapping = {
        'Đất thổ cư, đất ở': 'Đ',
        'Nhà mặt tiền': 'N',
        'Nhà trong hẻm': 'N',
        'Căn hộ chung cư': 'CC'
    }
    data_modified['Loại BDS'] = data_modified['Loại BDS'].apply(lambda x: bds_mapping.get(x, x))
    data_modified['Số toilet'] = np.nan
    data_modified['Mặt tiền'] = np.nan
    data_modified.rename(columns = {"Số lầu": "Số tầng", "Đường vào": "Đường trước nhà", "Loại BDS": "Loại"}, inplace = True)
    return data_modified

if __name__ == "__main__":
    bds_com_vn_data = load_data('BDS_com_vn')
    bds_so_data = load_data('BDS_So')
    alo_nha_dat_data = load_data('Alo_nha_dat')
    # processed = transform_bdscomvn(data)
    bds_com_vn_data_processed = transform_bdscomvn(bds_com_vn_data)
    alo_nha_dat_data_processed = tranform_alonhadat(alo_nha_dat_data)
    bds_so_data_processed = tranform_bdsso(bds_so_data)
    final_df = pd.concat([bds_com_vn_data_processed, alo_nha_dat_data_processed, bds_so_data_processed], axis = 0)
    # print(alo_nha_dat_data_processed.columns)
    # print(bds_com_vn_data_processed.columns)
    # print(bds_so_data_processed.columns)
    print(final_df.columns.tolist())
    print(final_df.info())