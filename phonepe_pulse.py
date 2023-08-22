import os
import json
import pandas as pd
from PIL import Image
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
import streamlit as st
import plotly.express as px
from streamlit_option_menu import option_menu

#Cloning the Data directly from the Github or it can be clone by git desktop app:
#copy the github code to be cloned and paste it in the VS source control and click open:

img = Image.open("E:\downloads\PhonePe-Logo.wine.png")
my_pic = Image.open("E:\downloads\pic.jpg")
st.set_page_config(page_title='PhonePe Pulse', page_icon=img, layout='wide')
st.title(' PhonePe Pulse Data Visualization ')
st.write("\n\n\n\n\n\n\n\n\n\n")

#read from aggreated transaction path
path1 = r"E:\ML\pulse\data\aggregated\transaction\country\india\state"

# Agg_state_list--> to get the list of states in India
Agg_state_list = os.listdir(path1)

#                 <---------extracting the datas to create a dataframe as agg<----->TRANSACTION--------->

col_1 = {'State': [], 'Year': [], 'Quater': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in Agg_state_list:
    aa = os.path.join(path1, i)  # Use os.path.join for path concatenation
    Agg_yr = os.listdir(aa)
        
    for j in Agg_yr:
        bb = os.path.join(aa, j)  # Use os.path.join for path concatenation
        Agg_yr_list = os.listdir(bb)
                
        for k in Agg_yr_list:
            cc = os.path.join(bb, k)  # Use os.path.join for path concatenation
            Data = open(cc, 'r')
            D = json.load(Data)
            
            for z in D['data']['transactionData']:
                Name = z['name']
                count = z['paymentInstruments'][0]['count']
                amount = z['paymentInstruments'][0]['amount']
                col_1['Transaction_type'].append(Name)
                col_1['Transaction_count'].append(count)
                col_1['Transaction_amount'].append(amount)
                col_1['State'].append(i)
                col_1['Year'].append(j)
                col_1['Quater'].append(int(k.strip('.json')))

# Created DataFrame Successfully
df_aggregated_transaction = pd.DataFrame(col_1)

#                 <---------extracting the datas to create a dataframe as agg<----->USER--------->

path2 = r"E:\ML\pulse\data\aggregated\user\country\india\state"
user_list = os.listdir(path2)

col2 = {'State': [], 'Year': [], 'Quater': [], 'brands': [], 'Count': [],
        'Percentage': []}

for i in user_list:
    aa = os.path.join(path2, i)
    Agg_yr = os.listdir(aa)
        
    for j in Agg_yr:
        bb = os.path.join(aa,j)
        Agg_yr_list = os.listdir(bb)
           
        for k in Agg_yr_list:
            cc = os.path.join(bb,k)
            Data = open(cc, 'r')
            B = json.load(Data)
            try:
                for w in B["data"]["usersByDevice"]:
                    brand_name = w["brand"]
                    count_ = w["count"]
                    ALL_percentage = w["percentage"]
                    col2["brands"].append(brand_name)
                    col2["Count"].append(count_)
                    col2["Percentage"].append(ALL_percentage)
                    col2["State"].append(i)
                    col2["Year"].append(j)
                    col2["Quater"].append(int(k.strip('.json')))
            except:
                pass

# CReated Dataframe Successfully
df_aggregated_user = pd.DataFrame(col2)

#                 <---------extracting the datas to create a dataframe as map<----->TRANSACTION--------->

path3 = r"E:\ML\pulse\data\map\transaction\hover\country\india\state"
hover_list = os.listdir(path3)

col3 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'count': [],
        'amount': []}
for i in hover_list:
    aa = os.path.join(path3,i)
    Agg_yr = os.listdir(aa)

    for j in Agg_yr:
        bb = os.path.join(aa,j)
        Agg_yr_list = os.listdir(bb)

        for k in Agg_yr_list:
            cc = os.path.join(bb,k)
            Data = open(cc, 'r')
            C = json.load(Data)
            
            for x in C["data"]["hoverDataList"]:
                District = x["name"]
                count = x["metric"][0]["count"]
                amount = x["metric"][0]["amount"]
                col3["District"].append(District)
                col3["count"].append(count)
                col3["amount"].append(amount)
                col3['State'].append(i)
                col3['Year'].append(j)
                col3['Quater'].append(int(k.strip('.json')))

# CReated Dataframe Successfully
df_map_transaction = pd.DataFrame(col3)

#                 <---------extracting the datas to create a dataframe as map<----->USER--------->

path4 = r"E:\ML\pulse\data\map\user\hover\country\india\state"
map_list = os.listdir(path4)

col4 = {"State": [], "Year": [], "Quater": [], "District": [],
        "RegisteredUser": []}
for i in map_list:
    aa = os.path.join(path4,i)
    Agg_yr = os.listdir(aa)

    for j in Agg_yr:
        bb = os.path.join(aa,j)
        Agg_yr_list = os.listdir(bb)

        for k in Agg_yr_list:
            cc = os.path.join(bb,k)
            Data = open(cc, 'r')
            D = json.load(Data)

            for u in D["data"]["hoverData"].items():
                district = u[0]
                registereduser = u[1]["registeredUsers"]
                col4["District"].append(district)
                col4["RegisteredUser"].append(registereduser)
                col4['State'].append(i)
                col4['Year'].append(j)
                col4['Quater'].append(int(k.strip('.json')))

# CReated Dataframe Successfully
df_map_user = pd.DataFrame(col4)

#                 <---------extracting the datas to create a dataframe as top<----->TRANSACTION--------->

path5 =  r"E:\ML\pulse\data\top\transaction\country\india\state"
TOP_list = os.listdir(path5)

col5 = {'State': [], 'Year': [], 'Quater': [], 'District': [], 'Transaction_count': [],
        'Transaction_amount': []}
for i in TOP_list:
    aa = os.path.join(path5,i)
    Agg_yr = os.listdir(aa)

    for j in Agg_yr:
        bb = os.path.join(aa,j)
        Agg_yr_list = os.listdir(bb)

        for k in Agg_yr_list:
            cc = os.path.join(bb,k)
            Data = open(cc, 'r')
            E = json.load(Data)
            
            for z in E['data']['pincodes']:
                Name = z['entityName']
                count = z['metric']['count']
                amount = z['metric']['amount']
                col5['District'].append(Name)
                col5['Transaction_count'].append(count)
                col5['Transaction_amount'].append(amount)
                col5['State'].append(i)
                col5['Year'].append(j)
                col5['Quater'].append(int(k.strip('.json')))

# CReated Dataframe Successfully
df_top_transaction = pd.DataFrame(col5)

#                 <---------extracting the datas to create a dataframe as top<----->USER--------->

path6 = r"E:\ML\pulse\data\top\user\country\india\state"
USER_list = os.listdir(path6)

col6 = {'State': [], 'Year': [], 'Quater': [], 'District': [],
        'RegisteredUser': []}
for i in USER_list:
    aa = os.path.join(path6,i)
    Agg_yr = os.listdir(aa)

    for j in Agg_yr:
        bb = os.path.join(aa,j)
        Agg_yr_list = os.listdir(bb)

        for k in Agg_yr_list:
            cc = os.path.join(bb,k)
            Data = open(cc, 'r')
            F = json.load(Data)
            
            for t in F['data']['pincodes']:
                Name = t['name']
                registeredUser = t['registeredUsers']
                col6['District'].append(Name)
                col6['RegisteredUser'].append(registeredUser)
                col6['State'].append(i)
                col6['Year'].append(j)
                col6['Quater'].append(int(k.strip('.json')))

# CReated Dataframe Successfully
df_top_user = pd.DataFrame(col6)

#        >>>>>>>>>>-------   checking for missing and null values:   ------->>>>>>>>>> 

# df_aggregated_transaction.info()
# print('\n\n ----------------------------------------------------------------------\n\n')
# df_aggregated_user.info()
# print('\n\n ----------------------------------------------------------------------\n\n')
# df_map_transaction.info()
# print('\n\n ----------------------------------------------------------------------\n\n')
# df_map_user.info()
# print('\n\n ----------------------------------------------------------------------\n\n')
# df_top_transaction.info()
# print('\n\n ----------------------------------------------------------------------\n\n')
# df_top_user.info()

#       -------------------------         connecting MYSQL          -------------------------

# Establish MySQL database connection
mysql_connection = mysql.connector.connect(
    host='localhost',
    user='root',
    password='srk012126',
    database='phonepe'
)
cursor = mysql_connection.cursor()

# Create an SQLAlchemy engine from the MySQL connection
engine = create_engine('mysql+mysqlconnector://root:srk012126@localhost/phonepe')

# Assuming df_aggregated_transaction, df_aggregated_user, and other DataFrames are defined

# Inserting each DataFrame into the MySQL server using SQLAlchemy
# df_aggregated_transaction.to_sql('aggregated_transaction', con=engine, if_exists='replace', index=False)
# df_aggregated_user.to_sql('aggregated_user', con=engine, if_exists='replace', index=False)
# df_map_transaction.to_sql('map_transaction', con=engine, if_exists='replace', index=False)
# df_map_user.to_sql('map_user', con=engine, if_exists='replace', index=False)
# df_top_transaction.to_sql('top_transaction', con=engine, if_exists='replace', index=False)
# df_top_user.to_sql('top_user', con=engine, if_exists='replace', index=False)

#      ---------------------->>>>>      USING STREAMLIT      <<<<<-----------------------

# with st.sidebar:
SELECT = option_menu(
    menu_title=None,
    options=["About", "Search", "Home", "Basic insights", "Contact"],
    icons=["bar-chart", "search", "house", "toggles", "at"],
    default_index=2,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "white", "size": "cover"},
        "icon": {"color": "black", "font-size": "20px"},
        "nav-link": {"font-size": "20px", "text-align": "center", "margin": "-2px", "--hover-color": "#6F36AD"},
        "nav-link-selected": {"background-color": "#6F36AD"}
    }

)

if SELECT == "Basic insights":
    st.title("BASIC INSIGHTS")
    # st.write("----")
    st.subheader("Let's know some basic insights about the data")
    options = ["--select--", "Top 10 states based on year and amount of transaction",
               "Least 10 states based on type and amount of transaction",
               "Top 10 mobile brands based on percentage of transaction",
               "Top 10 Registered-users based on States and District(pincode)",
               "Top 10 Districts based on states and amount of transaction",
               "Least 10 Districts based on states and amount of transaction",
               "Least 10 registered-users based on Districts and states",
               "Top 10 transactions_type based on states and transaction_amount"]
    select = st.selectbox("Select the option", options)
    if select == "Top 10 states based on year and amount of transaction":
        cursor.execute(
            '''SELECT State, Year, Quater, MAX(Transaction_amount) AS Max_Transaction_amount
            FROM top_transaction GROUP BY State, Year, Quater ORDER BY Max_Transaction_amount DESC
            LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Max_Transaction_amount', 'Year', 'Quater'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="Max_Transaction_amount")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Least 10 states based on type and amount of transaction":
        cursor.execute(
            '''SELECT DISTINCT State,min(Transaction_amount) as Min_Transaction_amount,Year,Quater 
            FROM top_transaction 
            GROUP BY State,Year, Quater ORDER BY Min_Transaction_amount ASC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transaction_amount', 'Year', 'Quater'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="Transaction_amount")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Top 10 mobile brands based on percentage of transaction":
        cursor.execute(
            '''SELECT DISTINCT brands,max(Percentage)*100 as Max_percent
             FROM aggregated_user GROUP BY brands ORDER BY Max_percent DESC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['brands', 'Percentage'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="brands", y="Percentage")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Top 10 Registered-users based on States and District(pincode)":
        cursor.execute(
            '''SELECT DISTINCT State,District,max(RegisteredUser) as Max_RegisteredUser
            FROM top_user GROUP BY State,District ORDER BY Max_RegisteredUser DESC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'District', 'RegisteredUser'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="RegisteredUser")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Top 10 Districts based on states and amount of transaction":
        cursor.execute(
            '''SELECT DISTINCT State,District,max(amount) as Max_amount
            FROM map_transaction GROUP BY State,District ORDER BY Max_amount DESC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'District', 'Transaction_amount'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="Transaction_amount")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Least 10 Districts based on states and amount of transaction":
        cursor.execute(
            '''SELECT DISTINCT State,District,min(amount) as Min_amount
            FROM map_transaction GROUP BY State,District ORDER BY Min_amount ASC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'District', 'Transaction_amount'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="Transaction_amount")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Least 10 registered-users based on Districts and states":
        cursor.execute(
            '''SELECT DISTINCT State,District,min(RegisteredUser) as Min_RegisteredUser
            FROM top_user GROUP BY State,District ORDER BY Min_RegisteredUser ASC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'District', 'RegisteredUser'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="RegisteredUser")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

    elif select == "Top 10 transactions_type based on states and transaction_amount":
        cursor.execute(
            '''SELECT DISTINCT State,Transaction_type,max(Transaction_amount) as Max_Transaction_amount
            FROM aggregated_transaction 
            GROUP BY State,Transaction_type ORDER BY Max_Transaction_amount DESC LIMIT 10''')
        df = pd.DataFrame(cursor.fetchall(), columns=['State', 'Transaction_type', 'Transaction_amount'])
        col1, col2 = st.columns(2)
        with col1:
            st.write(df)
        with col2:
            fig = px.bar(df, x="State", y="Transaction_amount")
            fig.update_layout(height=400)
            tab1, tab2 = st.tabs(["Streamlit theme (default)", "Plotly native theme"])
            with tab1:
                st.plotly_chart(fig, theme="streamlit", use_container_width=True)
            with tab2:
                st.plotly_chart(fig, theme=None, use_container_width=True)

if SELECT == "Home":
    info_text = (
           "* PhonePe  is an Indian digital payments and financial technology company headquartered in Bengaluru, Karnataka, India.\n"
           "* PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer.\n"
           "* The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016.\n"
           "* It is owned by Flipkart, a subsidiary of Walmart."
           )
    st.subheader("PhonePe Information:")
    st.markdown(info_text)
    st.write("\n\n\n\n\n")
    st.image(img,width =200)
    st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")

if SELECT == "About":
    about = (
        "* The Indian digital payments story has truly captured the world's imagination.\n"
        "* From the largest towns to the remotest villages, there is a payments revolution being driven by the penetration of mobile phones, mobile internet and state-of-the-art payments infrastructure built as Public Goods championed by the central bank and the government.\n"
        "* Founded in December 2015, PhonePe has been a strong beneficiary of the API driven digitisation of payments in India. When we started, we were constantly looking for granular and definitive data sources on digital payments in India.\n "
        "* PhonePe Pulse is our way of giving back to the digital payments ecosystem."
        )
    st.markdown(about)
    st.write("\n\n\n\n\n")
    st.markdown("<h2 style='text-align: center;'>Phonepe Now Everywhere..!</h2>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'><a href='https://www.phonepe.com/app-download/'><button>DOWNLOAD THE APP NOW</button></a></p>", unsafe_allow_html=True)

if SELECT == "Contact":
    name = " KAPIL KUMAR R "
    mail = (f'{"Mail :"}  {"rkapilkumar1111@gmail.com"}')
    social_media = {"GITHUB": "https://github.com/08Kapil08",
                        }
    col1, col2 = st.columns(2)
    with col1:
        st.image(my_pic)
    with col2:
        st.title(name)
        st.subheader("An Aspiring DATA-SCIENTIST.... !")
        st.write("---")
        st.subheader(mail)
    # st.write("#")
    cols = st.columns(len(social_media))
    for index, (platform, link) in enumerate(social_media.items()):
        cols[index].write(f"[{platform}]({link})")

if SELECT == "Search":
    Topic = ["", "Brand", "District", "Registered-users", "Top-Transactions", "Transaction-Type"]
    choice_topic = st.selectbox("Search by", Topic)
    st.snow()

#  <---------------CREATING FUNCTIONS FOR QUERY SEARCH TO GET THE DATA FROM SQLlite---------------->

    def type_(type):
        cursor.execute(
            f"SELECT DISTINCT State,Quater,Year,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE Transaction_type = '{type}' ORDER BY State,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(),
                          columns=['State', 'Quater', 'Year', 'Transaction_type', 'Transaction_amount'])
        return df


    def type_year(year, type):
        cursor.execute(
            f"SELECT DISTINCT State,Year,Quater,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE Year = '{year}' AND Transaction_type = '{type}' ORDER BY State,Quater,Year");
        df = pd.DataFrame(cursor.fetchall(),
                          columns=['State', 'Year', "Quater", 'Transaction_type', 'Transaction_amount'])
        return df


    def type_state(state, year, type):
        cursor.execute(
            f"SELECT DISTINCT State,Year,Quater,Transaction_type,Transaction_amount FROM aggregated_transaction WHERE State = '{state}' AND Transaction_type = '{type}' And Year = '{year}' ORDER BY State,Quater,Year");
        dataframe = pd.DataFrame(cursor.fetchall(),
                                 columns=['State', 'Year', "Quater", 'Transaction_type', 'Transaction_amount'])
        return dataframe


    def district_choice_state(_state):
        cursor.execute(
            f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE State = '{_state}' ORDER BY State,Year,Quater,District");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'amount'])
        return dataframe


    def dist_year_state(year, _state):
        cursor.execute(
            f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE Year = '{year}' AND State = '{_state}' ORDER BY State,Year,Quater,District");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'amount'])
        return dataframe


    def district_year_state(_dist, year, _state):
        cursor.execute(
            f"SELECT DISTINCT State,Year,Quater,District,amount FROM map_transaction WHERE District = '{_dist}' AND State = '{_state}' AND Year = '{year}' ORDER BY State,Year,Quater,District");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'amount'])
        return dataframe


    def brand_(brand_type):
        cursor.execute(
            f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE brands='{brand_type}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'brands', 'Percentage'])
        return dataframe


    def brand_year(brand_type, year):
        cursor.execute(
            f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE Year = '{year}' AND brands='{brand_type}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'brands', 'Percentage'])
        return dataframe


    def brand_state(state, brand_type, year):
        cursor.execute(
            f"SELECT State,Year,Quater,brands,Percentage FROM aggregated_user WHERE State = '{state}' AND brands='{brand_type}' AND Year = '{year}' ORDER BY State,Year,Quater,brands,Percentage DESC");
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'brands', 'Percentage'])
        return dataframe


    def transaction_state(_state):
        cursor.execute(
            f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE State = '{_state}' GROUP BY State,Year,Quater")
        dataframe = pd.DataFrame(cursor.fetchall(),
                                 columns=['State', 'Year', "Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return dataframe


    def transaction_year(_state, _year):
        cursor.execute(
            f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND State = '{_state}' GROUP BY State,Year,Quater")
        dataframe = pd.DataFrame(cursor.fetchall(),
                                 columns=['State', 'Year', "Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return dataframe


    def transaction_quater(_state, _year, _quater):
        cursor.execute(
            f"SELECT State,Year,Quater,District,Transaction_count,Transaction_amount FROM top_transaction WHERE Year = '{_year}' AND Quater = '{_quater}' AND State = '{_state}' GROUP BY State,Year,Quater")
        dataframe = pd.DataFrame(cursor.fetchall(),
                                 columns=['State', 'Year', "Quater", 'District', 'Transaction_count', 'Transaction_amount'])
        return dataframe


    def registered_user_state(_state):
        cursor.execute(
            f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE State = '{_state}' ORDER BY State,Year,Quater,District")
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'RegisteredUser'])
        return dataframe


    def registered_user_year(_state, _year):
        cursor.execute(
            f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' ORDER BY State,Year,Quater,District")
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'RegisteredUser'])
        return dataframe


    def registered_user_district(_state, _year, _dist):
        cursor.execute(
            f"SELECT State,Year,Quater,District,RegisteredUser FROM map_user WHERE Year = '{_year}' AND State = '{_state}' AND District = '{_dist}' ORDER BY State,Year,Quater,District")
        dataframe = pd.DataFrame(cursor.fetchall(), columns=['State', 'Year', "Quater", 'District', 'RegisteredUser'])
        return dataframe

#choice_topic['transaction_type','Brand','Top_transactions','Registered_user',District']
    if choice_topic == "Transaction-Type":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(" TRANSACTION TYPE ")
            transaction_type = st.selectbox("search by", ["Choose an option", "Financial Services",
                                                          "Merchant payments", "Peer-to-peer payments",
                                                          "Recharge & bill payments", "Others"], 0)
        with col2:
            st.subheader(" SELECT THE YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                          'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
                          'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                          'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur',
                          'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                          'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("State", menu_state, 0)

        if transaction_type:
            col1, col2, col3, = st.columns(3)
            with col1:
                st.subheader(f'{transaction_type}')
                st.write(type_(transaction_type))
        if transaction_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(type_year(choice_year, transaction_type))
        if transaction_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(type_state(choice_state, choice_year, transaction_type))

    if choice_topic == "District":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                          'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
                          'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                          'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur',
                          'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                          'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader(" SELECT YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader(" SELECT DISTRICT ")
            district = st.selectbox("search by", df_map_transaction["District"].unique().tolist())
        if choice_state:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.subheader(f'{choice_state}')
                st.write(district_choice_state(choice_state))
        if choice_year and choice_state:
            with col2:
                st.subheader(f'in {choice_year} ')
                st.write(dist_year_state(choice_year, choice_state))
        if district and choice_state and choice_year:
            with col3:
                st.subheader(f'in {district} ')
                st.write(district_year_state(district, choice_year, choice_state))

    if choice_topic == "Brand":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(" SELECT BRAND ")
            mobiles = ['', 'Apple', 'Asus', 'COOLPAD', 'Gionee', 'HMD Global', 'Huawei',
                       'Infinix', 'Lava', 'Lenovo', 'Lyf', 'Micromax', 'Motorola', 'OnePlus',
                       'Oppo', 'Realme', 'Samsung', 'Tecno', 'Vivo', 'Xiaomi', 'Others']
            brand_type = st.selectbox("search by", mobiles, 0)
        with col2:
            st.subheader(" SELECT YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                          'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
                          'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                          'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur',
                          'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                          'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("State", menu_state, 0)

        if brand_type:
            col1, col2, col3, = st.columns(3)
            with col1:
                st.subheader(f'{brand_type}')
                st.write(brand_(brand_type))
        if brand_type and choice_year:
            with col2:
                st.subheader(f' in {choice_year}')
                st.write(brand_year(brand_type, choice_year))
        if brand_type and choice_state and choice_year:
            with col3:
                st.subheader(f' in {choice_state}')
                st.write(brand_state(choice_state, brand_type, choice_year))

    if choice_topic == "Top-Transactions":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                          'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
                          'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                          'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur',
                          'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                          'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader(" SELECT YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader(" SELECT Quater ")
            menu_quater = ["", "1", "2", "3", "4"]
            choice_quater = st.selectbox("Quater", menu_quater, 0)

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(transaction_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(transaction_year(choice_state, choice_year))
        if choice_state and choice_quater:
            with col3:
                st.subheader(f'{choice_quater}')
                st.write(transaction_quater(choice_state, choice_year, choice_quater))

    if choice_topic == "Registered-users":
        col1, col2, col3 = st.columns(3)
        with col1:
            st.subheader(" SELECT STATE ")
            menu_state = ['', 'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
                          'assam', 'bihar', 'chandigarh', 'chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu',
                          'delhi', 'goa', 'gujarat', 'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
                          'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh', 'maharashtra', 'manipur',
                          'meghalaya', 'mizoram', 'nagaland', 'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
                          'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh', 'uttarakhand', 'west-bengal']
            choice_state = st.selectbox("State", menu_state, 0)
        with col2:
            st.subheader(" SELECT YEAR ")
            choice_year = st.selectbox("Year", ["", "2018", "2019", "2020", "2021", "2022"], 0)
        with col3:
            st.subheader(" SELECT DISTRICT ")
            district = st.selectbox("search by", df_map_transaction["District"].unique().tolist())

        if choice_state:
            with col1:
                st.subheader(f'{choice_state}')
                st.write(registered_user_state(choice_state))
        if choice_state and choice_year:
            with col2:
                st.subheader(f'{choice_year}')
                st.write(registered_user_year(choice_state, choice_year))
        if choice_state and choice_year and district:
            with col3:
                st.subheader(f'{district}')
                st.write(registered_user_district(choice_state, choice_year, district))

# Close the MySQL connection
mysql_connection.close()


































