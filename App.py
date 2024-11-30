import streamlit as st

# Custom CSS to increase font size
st.markdown(f""" 
    <style> 
    .appview-container .main .block-container{{{"max-width: {70}%;"}}}
    </style>    
    """, 
    unsafe_allow_html=True,
)
provinces = [
    "An Giang", "Bà Rịa-Vũng Tàu", "Bạc Liêu", "Bắc Giang", "Bắc Kạn", "Bến Tre", "Bình Dương", "Bình Phước", "Bình Thuận", 
    "Cà Mau", "Cần Thơ", "Cao Bằng", "Đắk Lắk", "Đắk Nông", "Điện Biên", "Đồng Nai", "Đồng Tháp", "Gia Lai", "Hà Giang", 
    "Hà Nam", "Hà Nội", "Hà Tĩnh", "Hải Dương", "Hải Phòng", "Hậu Giang", "Hoà Bình", "Hồ Chí Minh", "Hưng Yên", "Khánh Hòa", 
    "Kiên Giang", "Kon Tum", "Lai Châu", "Lâm Đồng", "Lạng Sơn", "Lào Cai", "Long An", "Nam Định", "Nghệ An", "Ninh Bình", 
    "Ninh Thuận", "Phú Thọ", "Phú Yên", "Quảng Bình", "Quảng Nam", "Quảng Ngãi", "Quảng Ninh", "Quảng Trị", "Sóc Trăng", 
    "Sơn La", "Tây Ninh", "Thái Bình", "Thái Nguyên", "Thanh Hóa", "Thừa Thiên-Huế", "Tiền Giang", "Trà Vinh", "Tuyên Quang", 
    "Vĩnh Long", "Vĩnh Phúc", "Yên Bái"
]
directions = ["East", "West", "North", "South", "North East", "South East", "South West", "North West", "Unknown"]
area_selections_house_apartment = ["<20m2", "20-50m2", "50m2-100m2", ">100m2"]
area_selections_land = ["<1000m2", "1000-5000m2", "5000-10000m2", ">10000m2"]

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

def predict_price(property_type, **kwargs):
    if property_type == "House":
        # Replace with actual prediction logic for House
        return f"Predicted House Price: {kwargs.get('bedrooms', 'N/A')} bedrooms, {kwargs.get('toilets', 'N/A')} toilets, {kwargs.get('floors', 'N/A')} floors"
    elif property_type == "Apartment":
        # Replace with actual prediction logic for Apartment
        return f"Predicted Apartment Price: {kwargs.get('bedrooms', 'N/A')} bedrooms, {kwargs.get('toilets', 'N/A')} toilets"
    elif property_type == "Land":
        # Replace with actual prediction logic for Land
        return f"Predicted Land Price: {kwargs.get('area', 'N/A')} square meters, {kwargs.get('location', 'N/A')}"
    else:
        return "No prediction available"

def page_house():
    st.title("Predicting for house-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_house_apartment, value = "20-50m2")
    front_road_size = st.selectbox("Front Road Size", ["1-5m", "5-10m", ">10m", "Others"])
    frontage_size = st.selectbox("Frontage size", ["1-5m", "5-10m", ">10m", "Others"])
    city = st.selectbox("City/Province", provinces)
    bedrooms = st.select_slider("Number of Bedrooms", options = list(range(1, 11)) + ["10+"], value = 3)
    toilets = st.select_slider("Number of Toilets", options = list(range(1, 11)) + ["10+"], value = 2)
    floors = st.select_slider("Number of Floors", options = list(range(1, 11)) + ["10+"], value = 1)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Unknown"))

    # st.write(f"Area: {area}")
    # st.write(f"Front Road Size: {front_road_size}")
    # st.write(f"Frontage Size: {frontage_size}")
    # st.write(f"City/Province: {city}")
    # st.write(f"Number of Bedrooms: {bedrooms}")
    # st.write(f"Number of Toilets: {toilets}")
    # st.write(f"Number of Floors: {floors}")
    # st.write(f"Direction: {direction}")
    col1, col2, col3 = st.columns(3, gap = "large")
    with col1: 
        button1 = st.button("Back to Selection")
    with col3:
        button2 = st.button("Predict House Price")
    if button1:
        st.session_state.page = None
        st.rerun()  
    if button2:
        prediction = predict_price("House", bedrooms=bedrooms, toilets=toilets)
        st.write(prediction)

    

def page_apartment():
    st.title("Predicting for apartment-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_house_apartment, value = "20-50m2")
    city = st.selectbox("City/Province", provinces)
    bedrooms = st.select_slider("Number of Bedrooms", options = list(range(1, 11)) + ["10+"], value = 3)
    toilets = st.select_slider("Number of Toilets", options = list(range(1, 11)) + ["10+"], value = 2)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Unknown"))

    st.write(f"Area: {area}")
    st.write(f"City/Province: {city}")
    st.write(f"Number of Bedrooms: {bedrooms}")
    st.write(f"Number of Toilets: {toilets}")
    st.write(f"Direction: {direction}")
    col1, col2, col3 = st.columns(3, gap = "large")
    with col1: 
        button1 = st.button("Back to Selection")
    with col3:
        button2 = st.button("Predict House Price")
    if button1:
        st.session_state.page = None
        st.rerun()  
    if button2:
        prediction = predict_price("House", bedrooms=bedrooms, toilets=toilets)
        st.write(prediction)

def page_land():
    st.title("Predicting for land-type real-estate")
    st.write("Now choose your estate properties for price prediction")
    area = st.select_slider("Area", options = area_selections_land, value = "<1000m2")
    front_road_size = st.selectbox("Front Road Size", ["1-5m", "5-10m", ">10m", "Others"])
    frontage_size = st.selectbox("Frontage size", ["1-5m", "5-10m", ">10m", "Others"])
    city = st.selectbox("City/Province", provinces)
    direction = st.selectbox("Direction of the House", directions, index=directions.index("Unknown"))

    st.write(f"Area: {area}")
    st.write(f"Front Road Size: {front_road_size}")
    st.write(f"Frontage Size: {frontage_size}")
    st.write(f"City/Province: {city}")
    st.write(f"Direction: {direction}")
    col1, col2, col3 = st.columns(3, gap = "large")
    with col1: 
        button1 = st.button("Back to Selection")
    with col3:
        button2 = st.button("Predict House Price")
    if button1:
        st.session_state.page = None
        st.rerun()  
    if button2:
        prediction = predict_price("House", area=area)
        st.write(prediction)
            

def main():
    if "page" not in st.session_state:
        st.session_state.page = None

    # Check if there's a page to navigate to
    if st.session_state.page is None:
        property_type_selection()
    elif st.session_state.page == "House":
        page_house()
    elif st.session_state.page == "Apartment":
        page_apartment()
    elif st.session_state.page == "Land":
        page_land()

if __name__ == "__main__":
    main()
