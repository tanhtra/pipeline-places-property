import pandas as pd

import snowflake.connector
import itertools

import streamlit as st

st.set_page_config(
    page_title="Pipeline - Places - Property - Serving Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

host = st.secrets['snowflake']['host']
user = st.secrets['snowflake']['user']
password = st.secrets['snowflake']['password']
account = st.secrets['snowflake']['account']
warehouse = st.secrets['snowflake']['warehouse']
database = st.secrets['snowflake']['database']
schema = st.secrets['snowflake']['schema']

cd = {
    'host' : host,
    'user' : user,
    'password' : password,
    'account' : account,
    'warehouse' : warehouse,
    'database' : database,
    'schema' : schema
}

@st.experimental_memo
def snowflake_query(snowflake_schema, snowflake_table, cd):

    query = f"select * from {snowflake_schema}.{snowflake_table}"

    connector = snowflake.connector.connect(
        host=cd['host'],
        user=cd['user'],
        password=cd['password'],
        account=cd['account'],
        warehouse=cd['warehouse'],
        database=cd['database'],
        schema=cd['schema'],
        client_session_keep_alive=True)
        
    try:
        cursor = connector.cursor().execute(query)
    finally:
        connector.close()
    df = cursor.fetch_pandas_all()

    return df

@st.experimental_memo
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

obt_property = snowflake_query('SERVING', 'OBT__PROPERTY', cd)
developer_stock = snowflake_query('SERVING', 'DEVELOPER_STOCK', cd)
vacancy = snowflake_query('SERVING', 'VACANCY', cd)

st.title('Pipeline - Places - Property - Serving Dashboard')

general_tabs, vacancy_tabs, developer_tabs = st.tabs(['General', 'Vacancy', 'Developer vacancy'])

with general_tabs:
    st.subheader('General')

    df_gen = obt_property
    df_gen['FLOORS'] = df_gen['FLOORS'].fillna(0.0).astype(int)

    df_gen.rename(columns={
        'NAME' : 'Name',
        'URL' : 'Url',
        'DETAIL_RENT' : 'Rent',
        'DETAIL_SALE' : 'Sale',
        'FLOORS' : 'Floors',
        'FEATURES' : 'Features',
        'YEAR' : 'Year',
        'MAP' : 'Map', 
        'DEVELOPER_NAME' : 'Developer'
    }, inplace=True)

    coords = df_gen['Map']\
                .str.replace('.jpg','')\
                .str.replace('https://photos.dotproperty.co.th/static_map/map_','')\
                .str.replace('https://photos.thailand-property.com/static_map/map_','')\
                .str.replace('https://photos.dotproperty.international/static_map/map_','')\
                .str.replace('https://photos.dotproperty-kh.com/static_map/map_','')\
                .str.replace('https://photos.dotproperty.com.ph/static_map/map_','')\
                .str.replace('https://photos.dotproperty.com.sg/static_map/map_','')\
                .str.replace('_', ',')
    
    df_gen[['lat', 'lon']] = coords.str.split(',', expand=True)[[0, 1]]
    df_gen['lat'] = df_gen['lat'].astype(float)
    df_gen['lon'] = df_gen['lon'].astype(float)
    df_gen['Year'] = df_gen['Year'].fillna(0).astype(int)
    df_gen['Rent'] = df_gen['Rent'].fillna(0).astype(int)
    df_gen['Sale'] = df_gen['Sale'].fillna(0).astype(int)
    df_gen['Developer'] = df_gen['Developer'].fillna('')

    max_rent = df_gen['Rent'].max().item()
    max_sale = df_gen['Sale'].max().item()

    st.session_state['max_num_rec'] = obt_property.shape[0]

    rent_col, sale_col, metric_row, metric_max = st.columns([3, 3, 1, 1], gap='large')

    with rent_col:
        start_rent, end_rent = st.slider('Select the range of rent stock', value=(0, max_rent))
        st.write('Querying rent levels between', start_rent, ' and ', end_rent)

        df_gen = df_gen[df_gen['Rent'].between(start_rent, end_rent)]
        st.session_state['num_rec'] = df_gen.shape[0]

    with sale_col:
        start_sale, end_sale = st.slider('Select the range of sale stock', value=(0, max_sale))
        st.write('Querying sale levels between', start_sale, ' and ', end_sale)

        df_gen = df_gen[df_gen['Sale'].between(start_sale, end_sale)]
        st.session_state['num_rec'] = df_gen.shape[0]

    with metric_row:
        st.metric(label='Rows returned', value=st.session_state['num_rec'])
        
    with metric_max:
        st.metric(label='Total row', value=st.session_state['max_num_rec'])

    developer_list = sorted(df_gen['Developer'].unique())
    
    def on_change_dev():
        st.session_state['num_rec'] = df_gen.shape[0]

    developer_select = st.multiselect('Developer filters', developer_list, on_change=on_change_dev)

    with st.expander("Selected developer(s)"):
        st.write('You selected:', developer_select)

    if developer_select:
        df_gen = df_gen[df_gen['Developer'].isin(developer_select)]

    st.session_state['num_rec'] = df_gen.shape[0]
    st.dataframe(df_gen)

    df_gen = df_gen[df_gen['lon'] > 100.00]

    st.map(df_gen, zoom=10)

    csv_obt = convert_df(df_gen)
    st.download_button(".CSV", csv_obt, "obt_map.csv", "text/csv", key='download-obt-csv')

with vacancy_tabs:
    st.subheader('Rental Vacancy')

    vacancy['INDEX_RENT'] = vacancy['INDEX_RENT'].fillna(0).astype(int)
    vacancy['INDEX_SALE'] = vacancy['INDEX_SALE'].fillna(0).astype(int)
    vacancy['DETAIL_RENT'] = vacancy['DETAIL_RENT'].fillna(0).astype(int)
    vacancy['DETAIL_SALE'] = vacancy['DETAIL_SALE'].fillna(0).astype(int)

    vacancy.rename(columns={
        'Name' : 'Name',
        'INDEX_RENT' : 'Index Rent',
        'INDEX_SALE' : 'Index Sale',
        'DETAIL_RENT' : 'Detail Rent',
        'DETAIL_SALE' : 'Detail Sale'
    }, inplace=True)

    st.dataframe(vacancy)

    csv_vacancy = convert_df(vacancy)
    st.download_button(".CSV", csv_vacancy, "vacancy.csv", "text/csv", key='download-vacancy-csv')

with developer_tabs:
    st.subheader('Developer Vacancy')

    developer_stock['TOTAL_INDEX_RENT'] = developer_stock['TOTAL_INDEX_RENT'].fillna(0).astype(int)
    developer_stock['TOTAL_INDEX_SALE'] = developer_stock['TOTAL_INDEX_SALE'].fillna(0).astype(int)
    developer_stock['TOTAL_DETAIL_RENT'] = developer_stock['TOTAL_DETAIL_RENT'].fillna(0).astype(int)
    developer_stock['TOTAL_DETAIL_SALE'] = developer_stock['TOTAL_DETAIL_SALE'].fillna(0).astype(int)

    developer_stock.rename(columns={
        'DEVELOPER_NAME' : 'Developer',
        'TOTAL_INDEX_RENT' : 'Total Index Rent',
        'TOTAL_INDEX_SALE' : 'Total Index Sale',
        'TOTAL_DETAIL_RENT' : 'Total Detail Rent',
        'TOTAL_DETAIL_SALE' : 'Total Detail Sale'
    }, inplace=True)

    st.dataframe(developer_stock)

    csv_developer = convert_df(developer_stock)
    st.download_button(".CSV", csv_developer, "developer.csv", "text/csv", key='download-developer-csv')
