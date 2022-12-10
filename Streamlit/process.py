import numpy as np
import altair as alt
import pandas as pd

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

import streamlit as st

st.set_page_config(
    page_title="Pipeline - Places - Property - RAW Dashboard",
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
        schema=cd['schema'])

    try:
        cursor = connector.cursor().execute(query)
    finally:
        connector.close()
    df = cursor.fetch_pandas_all()

    return df

raw_places = snowflake_query('RAW', 'CSV__PLACES_DATA', cd)
raw_transit = snowflake_query('RAW', 'CSV__TRANSIT_DATA', cd)
raw_index_meta = snowflake_query('RAW', 'JSON__INDEX_META', cd)
raw_property_details = snowflake_query('RAW', 'JSON__DETAILS', cd)

stg_details = snowflake_query('STAGING', 'STG__DETAILS', cd)
stg_developers = snowflake_query('STAGING', 'STG__DEVELOPERS', cd)
stg_features = snowflake_query('STAGING', 'STG__FEATURES', cd)
stg_places = snowflake_query('STAGING', 'STG__PLACES', cd)
stg_projects = snowflake_query('STAGING', 'STG__PROJECTS_LIST', cd)
stg_transit = snowflake_query('STAGING', 'STG__TRANSIT', cd)
stg_vacancy = snowflake_query('STAGING', 'STG__VACANCY', cd)

st.title('Pipeline Places Property - RAW dashboard')

places_t, transit_t, index_t, details_t = st.tabs(['Raw - Places', 'Raw - Transit',
                                                   'Raw - Index Metadata', 'Raw - Property Details'])

with places_t:
     st.header('Raw - Places')

     df_raw_places = raw_places[['NAME', 'geocodes.main.latitude', 'geocodes.main.longitude', 'location.formatted_address','FSQ_ID', 
               '_AIRBYTE_UNIQUE_KEY', '_AB_SOURCE_FILE_LAST_MODIFIED', '_AIRBYTE_AB_ID','_AIRBYTE_EMITTED_AT', 
               '_AIRBYTE_NORMALIZED_AT', '_AIRBYTE_CSV__PLACES_DATA_HASHID']]

     df_raw_places.rename(columns={
          'NAME' : 'name',
          'geocodes.main.latitude': 'lat',
          'geocodes.main.longitude': 'lon',
          'location.formatted_address' : 'address'
     }, inplace=True)

     st.write('There are ', df_raw_places.shape[0], ' rows of data')
     st.write('Last emitted at', df_raw_places['_AIRBYTE_EMITTED_AT'].max().strftime('%d-%m-%Y %H:%M:%S'))

     st.dataframe(df_raw_places)

     st.map(df_raw_places, zoom=10)

with transit_t:
     st.header('Raw - Transit')

     df_raw_transit = raw_transit[['NAME', 'CODE', 'LAT', 'LONG',
                                   '_AB_SOURCE_FILE_URL', '_AB_ADDITIONAL_PROPERTIES', 
                                   '_AB_SOURCE_FILE_LAST_MODIFIED', '_AIRBYTE_AB_ID', 
                                   '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT', 
                                   '_AIRBYTE_CSV__TRANSIT_DATA_HASHID']]

     df_raw_transit.rename(columns={
          'NAME' : 'name',
          'LAT': 'lat',
          'LONG': 'lon',
          'CODE' : 'code'
     }, inplace=True)

     st.write('There are ', df_raw_transit.shape[0], ' rows of data')
     st.write('Last emitted at', df_raw_transit['_AIRBYTE_EMITTED_AT'].max().strftime('%d-%m-%Y %H:%M:%S'))

     st.dataframe(df_raw_transit)

     st.map(df_raw_transit, zoom=9)

with index_t:
     st.header('Raw - Index Metadata')

     df_raw_index = raw_index_meta[['PROJECT_NAME', 'PROJECT_URL', 'PROJECT_RENT', 'PROJECT_SALE',
                                   '_AIRBYTE_UNIQUE_KEY', '_AB_SOURCE_FILE_URL', '_AB_ADDITIONAL_PROPERTIES',
                                   '_AB_SOURCE_FILE_LAST_MODIFIED', '_AIRBYTE_AB_ID',
                                   '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT',
                                   '_AIRBYTE_JSON__INDEX_META_HASHID']]

     df_raw_index.rename(columns={
          'PROJECT_NAME' : 'name',
          'PROJECT_URL': 'url',
          'PROJECT_RENT': 'rent',
          'PROJECT_SALE' : 'sale'
     }, inplace=True)

     st.write('There are ', df_raw_index.shape[0], ' rows of data')
     st.write('Last emitted at', df_raw_index['_AIRBYTE_EMITTED_AT'].max().strftime('%d-%m-%Y %H:%M:%S'))

     st.dataframe(df_raw_index)

with details_t:
     st.header('Raw - Property Details')

     df_raw_details = raw_property_details[['PROJECT_NAME', 'PROJECT_MAP', 'PROJECT_URL', 'PROJECT_RENT',
                                             'PROJECT_SALE', 'PROJECT_YEAR', 'PROJECT_FLOORS', 'PROJECT_FEATURES',
                                             'PROJECT_DEVELOPER', 
                                             '_AB_SOURCE_FILE_URL', '_AB_ADDITIONAL_PROPERTIES',
                                             '_AB_SOURCE_FILE_LAST_MODIFIED', '_AIRBYTE_AB_ID',
                                             '_AIRBYTE_EMITTED_AT', '_AIRBYTE_NORMALIZED_AT',
                                             '_AIRBYTE_JSON__DETAILS_HASHID', '_AIRBYTE_UNIQUE_KEY']]

     df_raw_details.rename(columns={
          'PROJECT_NAME' : 'name',
          'PROJECT_URL' : 'url',
          'PROJECT_RENT' : 'rent',
          'PROJECT_SALE' : 'sale',
          'PROJECT_MAP' : 'map',
          'PROJECT_FLOORS': 'floors', 
          'PROJECT_FEATURES' : 'features',
          'PROJECT_DEVELOPER' : 'developer' 
     }, inplace=True)

     st.write('There are ', df_raw_details.shape[0], ' rows of data')
     st.write('Last emitted at', df_raw_details['_AIRBYTE_EMITTED_AT'].max().strftime('%d-%m-%Y %H:%M:%S'))

     st.dataframe(df_raw_details)

st.title('Pipeline Places Property - Staging dashboard')

projects_st, features_st, transit_places_st =  st.tabs(['Staging - Projects', 'Staging - Features', ' Staging - Places'])

with projects_st:
     projects_1, projects_2 = st.columns(2)

     with projects_1:

          st.header('Staging - Projects data')

          df_projects = stg_projects[['NAME', 'URL', 'PROJECT_KEY']]
          df_projects.rename(columns={
               'NAME' : 'name',
               'URL' : 'url',
               'PROJECT_KEY' : 'key'
          }, inplace=True)

          st.write('There are ', df_projects.shape[0], ' rows of data')

          st.dataframe(df_projects)

     with projects_2:

          st.header('Staging - Project Developers')

          df_developers = stg_developers[['DEVELOPER_NAME', 'DEVELOPER_KEY']]
          df_developers.rename(columns={
               'DEVELOPER_NAME' : 'name',
               'DEVELOPER_KEY' : 'key'
          }, inplace=True)

          st.write('There are ', df_developers.shape[0], ' rows of data')

          st.dataframe(df_developers)

with features_st:
     st.header('Staging - Property Details')

     df_details = stg_details[['PROJECT_KEY', 'FLOORS', 'FEATURES', 'YEAR', 'MAP', 'DEVELOPER_KEY']]
     df_details.rename(columns={
                                   'PROJECT_KEY' : 'project_key',
                                   'FLOORS' : 'floors',
                                   'FEATURES' : 'features',
                                   'MAP' : 'map',
                                   'DEVELOPER_KEY' : 'developer_key'
     }, inplace=True)

     df_details['floors'] = df_details['floors'].fillna(0.0).astype(int)

     st.write('There are ', df_details.shape[0], ' rows of data')

     st.dataframe(df_details)

with transit_places_st:

     transit_c, places_c = st.columns(2)

     with places_c:

          st.header('Staging - Places')

          df_places = stg_places[['NAME', 'PLACES_KEY', 'LAT', 'LONG', 'ADDRESS', 'COORD']]
          df_places.rename(columns={
                                        'PLACES_KEY' : 'places_key',
                                        'NAME' : 'name',
                                        'LAT' : 'lat',
                                        'LONG' : 'lon',
                                        'ADDRESS' : 'address',
                                        'COORD' : 'coord'
          }, inplace=True)

          st.write('There are ', df_places.shape[0], ' rows of data')

          st.dataframe(df_places)

     with transit_c:

          st.header('Staging - Transit')

          df_transit = stg_transit[['NAME', 'CODE', 'TRANSIT_KEY', 'LAT', 'LONG', 'COORD']]
          df_transit.rename(columns={
                                        'PLACES_KEY' : 'places_key',
                                        'NAME' : 'name',
                                        'CODE' : 'code',
                                        'LAT' : 'lat',
                                        'LONG' : 'lon',
                                        'COORD' : 'coord'
          }, inplace=True)

          st.write('There are ', df_transit.shape[0], ' rows of data')

          st.dataframe(df_transit)

     df_pt_coord = pd.concat([df_places[['lat', 'lon']], df_transit[['lat', 'lon']]])

     st.map(df_pt_coord)