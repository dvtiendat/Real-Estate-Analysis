import streamlit as st
import pandas as pd 
import json
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import pickle
import numpy as np
import xgboost as xg
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sklearn.preprocessing import StandardScaler

st.set_page_config(layout="wide")
sys.stdout.reconfigure(encoding='utf-8')

uri = "mongodb+srv://svbk:dmHUST@cluster0.h5ef7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0" #change password to access to the database
client = MongoClient(uri, server_api = ServerApi('1'))
db = client['VietNameseRealEstateData']
collection = db['Final_Real_Estate']
final_data = list(collection.find())
df = pd.DataFrame(final_data)
columns = []
scaler = pickle.load(open('./SavedModels/scaler.pkl', 'rb'))
with open('./SavedModels/columns.json', 'r') as f:
    columns = json.load(f)
xgboost_model = pickle.load(open('./SavedModels/xgb_model.pkl', 'rb'))
    
# Custom CSS to increase font size
st.markdown(f""" 
    <style> 
    .appview-container .main .block-container{{{"max-width: {70}%;"}}}
    </style>    
    """, 
    unsafe_allow_html=True,
)
provinces = [
    "An Giang", "Bà Rịa Vũng Tàu", "Bạc Liêu", "Bắc Giang", "Bắc Kạn", "Bến Tre", "Bình Dương", "Bình Phước", "Bình Thuận", 
    "Cà Mau", "Cần Thơ", "Cao Bằng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang", 
    "Hà Nam", "Hà Nội", "Hà Tĩnh", "Hải Dương", "Hải Phòng", "Hậu Giang", "Hoà Bình", "Hồ Chí Minh", "Hưng Yên", "Khánh Hòa", 
    "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", "Ninh Bình", 
    "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", 
    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa", "Thừa Thiên Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang", 
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
]
directions = ["East", "West", "North", "South", "North East", "South East", "South West", "North West", "Any"]
direction_to_column = {
    "East": "Đ",
    "West": "W",
    "North": "B",
    "South": "N",
    "North East": "ĐB", 
    "South East": "ĐN", 
    "South West": "TN", 
    "North West": "TB", 
    "Any": "KXĐ"
}
area_selections_house_apartment = ["<20m2", "20-50m2", "50-100m2", "100-200m2", ">200m2"]
select_to_area_house_apartment = {
    "<20m2": 15.,
    "20-50m2": 35.,
    "50-100m2": 75.,
    "100-200m2": 150.,
    ">200m2": 300. 
}
area_selections_land = ["<1000m2", "1000-5000m2", "5000-10000m2", ">10000m2"]
select_to_area_land = {
    "<1000m2": 500.,
    "1000-5000m2": 3000.,
    "5000-10000m2": 7500.,
    ">10000m2": 15000. 
}
select_to_frontage_size_or_front_road_width = {
    "1-5m": 3., 
    "5-10m": 7.5, 
    "10-20m": 15., 
    ">20m": 25.
}
st.sidebar.title("DDDHM's Vietnamese Real-estate Price Predictor")
st.write("")
option = st.sidebar.selectbox("Choose landing page" , ['Dashboard' , "Prediction tool"])

def total_by_type():
        # Number of houses by each Type

    type_counts = df['Loại'].value_counts()

    ordered_types = ['N', 'CC', 'Đ']
    type_counts = type_counts[ordered_types]

    color_map = {
        'Đ': '#ffb6c1',
        'N': '#a3c9d1',
        'CC': '#f9d372'
    }

    colors = [color_map.get(x, 'gray') for x in type_counts.index]

    plt.figure(figsize=(6, 3))
    bars = plt.barh(type_counts.index, type_counts.values, color=colors)

    plt.title('Number of Properties by Types')
    plt.xlabel('Counts')
    plt.ylabel('Types')

    legend_labels = {
        'Đ': 'Land',
        'CC': 'Apartments',
        'N': 'Houses'
    }

    for bar in bars:
        plt.text(bar.get_width() + 20, bar.get_y() + bar.get_height() / 2,
                f'{bar.get_width()}', va='center', ha='left', fontsize=10)

    plt.grid(axis='x', linestyle='--', alpha=0.3)
    plt.subplots_adjust(right=1)

    handles = [plt.Line2D([0], [0], color=color_map[key], lw=6, label=legend_labels[key]) for key in ['Đ', 'CC', 'N']]
    plt.legend(handles=handles)
    
    return plt

def top5_high_avg_price():
    # Create a temporary DataFrame to perform operations without modifying the original 'df'
    temp_df = df[~((df['Loại'] == 'N') & (df['Thành phố'].str.lower() == 'thái nguyên'))]

    # Calculate price per square meter in temp_df
    temp_df['Giá/m²'] = (temp_df['Mức giá']*1000) / temp_df['Diện tích']

    # Filter and calculate the top 5 cities for each type
    top_5_cc = temp_df[temp_df['Loại'] == 'CC'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nlargest(5, 'Giá/m²').reset_index()
    top_5_n = temp_df[temp_df['Loại'] == 'N'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nlargest(5, 'Giá/m²').reset_index()
    top_5_d = temp_df[temp_df['Loại'] == 'Đ'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nlargest(5, 'Giá/m²').reset_index()

    # Plotting section
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # Plot for Apartment
    sns.barplot(
        data=top_5_cc,
        x='Giá/m²',
        y=top_5_cc['Thành phố'].str.title(),
        palette='flare_r',
        ax=axes[0]
    )
    axes[0].set_title('Apartment', fontsize=14)
    axes[0].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[0].set_ylabel('City', fontsize=12)
    axes[0].grid(axis='x', linestyle='--', alpha=0.3)

    # Plot for House
    sns.barplot(
        data=top_5_n,
        x='Giá/m²',
        y=top_5_n['Thành phố'].str.title(),
        palette='flare_r',
        ax=axes[1]
    )
    axes[1].set_title('House', fontsize=14)
    axes[1].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[1].set_ylabel('City', fontsize=12)
    axes[1].grid(axis='x', linestyle='--', alpha=0.3)

    # Plot for Land
    sns.barplot(
        data=top_5_d,
        x='Giá/m²',
        y=top_5_d['Thành phố'].str.title(),
        palette='flare_r',
        ax=axes[2]
    )
    axes[2].set_title('Land', fontsize=14)
    axes[2].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[2].set_ylabel('City', fontsize=12)
    axes[2].grid(axis='x', linestyle='--', alpha=0.3)

    # Add value labels to each bar
    for ax, top_5_data in zip(axes, [top_5_cc, top_5_n, top_5_d]):
        for index, value in enumerate(top_5_data['Giá/m²']):
            ax.text(
                value + 0.001,
                index,
                f'{value:.2f}',
                va='center',
                fontsize=10
            )

    fig.suptitle('Top 5 Cities With The Highest Price per m² By Type', fontsize=20)

    plt.tight_layout()
    return plt

def top5_low_avg_price():
    temp_df = df[~((df['Loại'] == 'N') & (df['Thành phố'].str.lower() == 'thái nguyên'))]

    # Calculate price per square meter in temp_df
    temp_df['Giá/m²'] = (temp_df['Mức giá'] * 1000) / temp_df['Diện tích']

    # Filter and calculate the top 5 cities with the lowest price per square meter for each type
    top_5_cc = temp_df[temp_df['Loại'] == 'CC'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nsmallest(5, 'Giá/m²').reset_index()
    top_5_n = temp_df[temp_df['Loại'] == 'N'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nsmallest(5, 'Giá/m²').reset_index()
    top_5_d = temp_df[temp_df['Loại'] == 'Đ'][['Thành phố', 'Giá/m²']].groupby('Thành phố').mean().nsmallest(5, 'Giá/m²').reset_index()

    # Plotting section
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))

    # Plot for Apartment
    sns.barplot(
    data=top_5_cc,
    x='Giá/m²',
    y=top_5_cc['Thành phố'].str.title(),
    palette='crest_r',
    ax=axes[0]
    )
    axes[0].set_title('Apartment', fontsize=14)
    axes[0].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[0].set_ylabel('City', fontsize=12)
    axes[0].grid(axis='x', linestyle='--', alpha=0.3)

    # Plot for House
    sns.barplot(
    data=top_5_n,
    x='Giá/m²',
    y=top_5_n['Thành phố'].str.title(),
    palette='crest_r',
    ax=axes[1]
    )
    axes[1].set_title('House', fontsize=14)
    axes[1].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[1].set_ylabel('City', fontsize=12)
    axes[1].grid(axis='x', linestyle='--', alpha=0.3)

    # Plot for Land
    sns.barplot(
    data=top_5_d,
    x='Giá/m²',
    y=top_5_d['Thành phố'].str.title(),
    palette='crest_r',
    ax=axes[2]
    )
    axes[2].set_title('Land', fontsize=14)
    axes[2].set_xlabel('Price per m² (Million VND/m²)', fontsize=12)
    axes[2].set_ylabel('City', fontsize=12)
    axes[2].grid(axis='x', linestyle='--', alpha=0.3)

    # Add value labels to each bar
    for ax, top_5_data in zip(axes, [top_5_cc, top_5_n, top_5_d]):
        for index, value in enumerate(top_5_data['Giá/m²']):
            ax.text(
                value + 0.001,
                index,
                f'{value:.2f}',
                va='center',
                fontsize=10
            )

    fig.suptitle('Top 5 Cities With The Lowest Price per m² By Type', fontsize=20)

    plt.tight_layout()
    return plt 

def dashboard():
    global df
    st.title("Number of Properties by Type")
    plt3 = total_by_type()
    st.pyplot(plt3,use_container_width=True) 
    
    st.markdown("")
    
    st.title("Top 5 Cities With The Highest Average Price By Type")
    plt1 = top5_high_avg_price()
    st.pyplot(plt1,use_container_width=True)

    st.markdown("")

    st.title("Top 5 Cities With The Lowest Average Price By Type")
    plt2 = top5_low_avg_price()
    st.pyplot(plt2,use_container_width=True)
       
    
def property_type_selection():
    st.title("DDDHM's Vietnamese Real-estate Price Predictor")
    st.header("Choose the real-estate type to continue")
    st.write("")
    
    property_type = st.radio("Select Property Type", ["House", "Apartment", "Land"])

    if 'property_type' not in st.session_state:
        st.session_state.property_type = property_type

    if st.button("Next"):
        st.session_state.page = property_type
        st.rerun()  

@st.dialog("Predicted price")
def predict_price(property_type, **kwargs): 
    estate_info = {column: 0. for column in columns[:-1]}
    direction = kwargs.get('direction')
    city = kwargs.get('city')
    area = kwargs.get('area')
    estate_info['Hướng_' + direction_to_column[direction]] = 1.
    # estate_info['Diện tích'] = area_selections_house_apartment[area]
    
    found_city = False
    for column in columns:
        if city.lower() in column:
            estate_info[column] = 1.
            found_city = True
            break  
    if not found_city:
        estate_info['Thành phố_KXĐ'] = 1.
    if property_type == "House":
        estate_info['Diện tích'] = float(scaler['Diện tích'].transform([[np.log(select_to_area_house_apartment[area])]]).reshape(-1).squeeze(0))
        estate_info['Loại_N'] = 1.
        bedrooms = kwargs.get('bedrooms')
        toilets = kwargs.get('toilets')
        floors = kwargs.get('floors')
        frontage_size = kwargs.get('frontage_size') 
        front_road_width = kwargs.get('front_road_width')
        if bedrooms == "10+":
            estate_info['Số phòng ngủ'] = 11. 
        else:
            estate_info['Số phòng ngủ'] = float(bedrooms) 
        estate_info['Số phòng ngủ'] = float(scaler['Số phòng ngủ'].transform([[estate_info['Số phòng ngủ']]]).reshape(-1).squeeze(0))
        if toilets == "10+":
            estate_info['Số toilet'] = 11. 
        else:
            estate_info['Số toilet'] = float(toilets)
        estate_info['Số toilet'] = float(scaler['Số toilet'].transform([[estate_info['Số toilet']]]).reshape(-1).squeeze(0))
        if floors == "10+":
            estate_info['Số tầng'] = 11. 
        else:
            estate_info['Số tầng'] = float(floors)
        estate_info['Số tầng'] = float(scaler['Số tầng'].transform([[estate_info['Số tầng']]]).reshape(-1).squeeze(0))
        estate_info['Mặt tiền'] =  float(scaler['Mặt tiền'].transform([[np.log(select_to_frontage_size_or_front_road_width[frontage_size])]]).reshape(-1).squeeze(0))
        estate_info['Đường trước nhà'] = float(scaler['Đường trước nhà'].transform([[np.log(select_to_frontage_size_or_front_road_width[front_road_width])]]).reshape(-1).squeeze(0))
        
        print(json.dumps(estate_info, indent = 2, ensure_ascii=False))
        # return f"Predicted House Price: {kwargs.get('bedrooms', 'N/A')} bedrooms, {kwargs.get('toilets', 'N/A')} toilets, {kwargs.get('floors', 'N/A')} floors"
    elif property_type == "Apartment":
        estate_info['Diện tích'] = float(scaler['Diện tích'].transform([[np.log(select_to_area_house_apartment[area])]]).reshape(-1).squeeze(0))
        estate_info['Loại_CC'] = 1.
        estate_info['Số tầng'] = 1.
        bedrooms = kwargs.get('bedrooms')
        toilets = kwargs.get('toilets')
        frontage_size = kwargs.get('frontage_size') 
        front_road_width = kwargs.get('front_road_width')
        if bedrooms == "10+":
            estate_info['Số phòng ngủ'] = 11. 
        else:
            estate_info['Số phòng ngủ'] = float(bedrooms)
        estate_info['Số phòng ngủ'] = float(scaler['Số phòng ngủ'].transform([[estate_info['Số phòng ngủ']]]).reshape(-1).squeeze(0))    
        if toilets == "10+":
            estate_info['Số toilet'] = 11. 
        else:
            estate_info['Số toilet'] = float(toilets)
        estate_info['Số toilet'] = float(scaler['Số toilet'].transform([[estate_info['Số toilet']]]).reshape(-1).squeeze(0))
        estate_info['Mặt tiền'] = -10.
        estate_info['Mặt tiền missing'] = 1.
        estate_info['Đường trước nhà missing'] = 1.
        estate_info['Đường trước nhà'] = -10.
        # print(json.dumps(estate_info, indent = 2, ensure_ascii=False))
        # return f"Predicted Apartment Price: {kwargs.get('bedrooms', 'N/A')} bedrooms, {kwargs.get('toilets', 'N/A')} toilets"
    elif property_type == "Land":
        estate_info['Diện tích'] = float(scaler['Diện tích'].transform([[select_to_area_land[area]]]).reshape(-1).squeeze(0))
        estate_info['Loại_Đ'] = 1
        directions = kwargs.get('direction')
        frontage_size = kwargs.get('frontage_size')
        front_road_width = kwargs.get('front_road_width')
        estate_info['Mặt tiền'] =  float(scaler['Mặt tiền'].transform([[np.log(select_to_frontage_size_or_front_road_width[frontage_size])]]).reshape(-1).squeeze(0))
        estate_info['Đường trước nhà'] = float(scaler['Đường trước nhà'].transform([[np.log(select_to_frontage_size_or_front_road_width[front_road_width])]]).reshape(-1).squeeze(0))
        estate_info['Số phòng ngủ'] = -10.
        estate_info['Số phòng ngủ missing'] = 1.
        estate_info['Số tầng'] = -10.
        estate_info['Số tầng missing'] = 1.
        estate_info['Số toilet'] = -10.
        estate_info['Số toilet missing'] = 1.
        print(directions, frontage_size, front_road_width, city, area)
        # print(json.dumps(estate_info, indent = 2, ensure_ascii=False))
        # return f"Predicted Land Price: {kwargs.get('area', 'N/A')} square meters, {kwargs.get('location', 'N/A')}"
    print(json.dumps(estate_info, indent = 2, ensure_ascii=False))
    x_input = np.array([v for k, v in estate_info.items()]).reshape(1, -1)
    output = np.exp(scaler["Mức giá"].inverse_transform([[xgboost_model['best_model'].predict(x_input)[0]]]).reshape(-1).squeeze(0))
    st.write(f"The predicted price is: {output:.2f} billion VND")
    return "LOL!"

def page_house():
    st.title("Predicting for house-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_house_apartment, value = "20-50m2")
    front_road_width = st.selectbox("Front Road Size", ["1-5m", "5-10m", "10-20m", ">20m"])
    frontage_size = st.selectbox("Frontage size", ["1-5m", "5-10m", "10-20m", ">20m"])
    city = st.selectbox("City/Province", provinces)
    bedrooms = st.select_slider("Number of Bedrooms", options = list(range(1, 11)) + ["10+"], value = 3)
    toilets = st.select_slider("Number of Toilets", options = list(range(1, 11)) + ["10+"], value = 2)
    floors = st.select_slider("Number of Floors", options = list(range(1, 11)) + ["10+"], value = 1)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Any"))

    button2 = st.button("Predict House Price")
    if button2:
        prediction = predict_price("House", 
                                   bedrooms=bedrooms, 
                                   toilets=toilets, 
                                   floors=floors,
                                   direction=direction,
                                   area=area, 
                                   city=city, 
                                   frontage_size=frontage_size,
                                   front_road_width=front_road_width)

    

def page_apartment():
    st.title("Predicting for apartment-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_house_apartment, value = "20-50m2")
    city = st.selectbox("City/Province", provinces)
    bedrooms = st.select_slider("Number of Bedrooms", options = list(range(1, 11)) + ["10+"], value = 3)
    toilets = st.select_slider("Number of Toilets", options = list(range(1, 11)) + ["10+"], value = 2)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Any"))

    st.write(f"Area: {area}")
    st.write(f"City/Province: {city}")
    st.write(f"Number of Bedrooms: {bedrooms}")
    st.write(f"Number of Toilets: {toilets}")
    st.write(f"Direction: {direction}")
    col1, col2, col3 = st.columns(3, gap = "large")
    with col3:
        button2 = st.button("Predict House Price")
    if button2:
        prediction = predict_price("Apartment", 
                                   bedrooms=bedrooms, 
                                   toilets=toilets,
                                   direction=direction,
                                   area=area, 
                                   city=city)
        st.write(prediction)

def page_land():
    st.title("Predicting for land-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_land, value = "<1000m2")
    front_road_size = st.selectbox("Front Road Size", ["1-5m", "5-10m", ">10m", "Others"])
    frontage_size = st.selectbox("Frontage size", ["1-5m", "5-10m", ">10m", "Others"])
    city = st.selectbox("City/Province", provinces)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Any"))

    st.write(f"Area: {area}")
    st.write(f"Front Road Size: {front_road_size}")
    st.write(f"Frontage Size: {frontage_size}")
    st.write(f"City/Province: {city}")
    st.write(f"Direction: {direction}")
    col1, col2, col3 = st.columns(3, gap = "large")
    with col3:
        button2 = st.button("Predict House Price")
    if button2:
        prediction = predict_price("Land",  
                                   direction=direction,
                                   area=area, 
                                   city=city, 
                                   frontage_size=frontage_size,
                                   front_road_width=front_road_width)
        st.write(prediction)
            

def main():
    if option == "Dashboard":
        dashboard()
    elif option == "Prediction tool":
        st.sidebar.header("Choose the real-estate type to continue")
        property_type = st.sidebar.radio("Select Property Type", ["House", "Apartment", "Land"])
        if property_type == "House":
            page_house()
        elif property_type == "Apartment":
            page_apartment()
        elif property_type == "Land":
            page_land()

if __name__ == "__main__":
    main()
