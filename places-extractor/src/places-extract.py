import os
import pandas as pd
import requests
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

import logging
import json

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import time

load_dotenv()

def query_places(code, location):

    places_data = []
    df_temp = None
    url = "https://api.foursquare.com/v3/places/search"
    api_key = os.environ.get("places_api_key")

    params = {
        "ll": location,
        "sort":"DISTANCE",
        "radius":"100000",
        "categories":"10000,13000,17000",
        "limit":"50"
    }

    headers = {
        "Accept": "application/json",
        "Authorization": api_key
    }

    response = requests.request("GET", url, params=params, headers=headers)

    if response.status_code == 200: 
        places_data.append(response.json())
        df_temp = pd.json_normalize(places_data, record_path=['results'])
        df_temp.drop(columns=['categories'], inplace=True)

        df_extract = df_temp[['fsq_id', 'name', 'geocodes.main.latitude', 'geocodes.main.longitude', 'location.formatted_address']]

        os.makedirs('data', exist_ok=True)
        df_extract.to_csv(f'data/{code}_places.csv', index=False)

    return df_extract

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',
        aws_access_key_id = os.environ.get("aws_access_key"),
        aws_secret_access_key= os.environ.get("aws_secret_key"))

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

host = os.environ.get("snowflake_host")
user = os.environ.get("snowflake_user")
password = os.environ.get("snowflake_password")
account = os.environ.get("snowflake_account")
warehouse = os.environ.get("snowflake_warehouse")
database = os.environ.get("snowflake_database")
schema = os.environ.get("snowflake_schema")

query = "select * from DBT_PROJECT03.stg__transit_data"
connector = snowflake.connector.connect(
    host=host,
    user=user,
    password=password,
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema)

try:
    cursor = connector.cursor().execute(query)
finally:
    connector.close()

df = cursor.fetch_pandas_all()
df['COORD'] = df[['LAT', 'LONG']].astype(str).apply(','.join, axis=1)

df['RESULT'] = df.apply(lambda x: query_places(x['CODE'], x['COORD']), axis=1)

directory = os.fsencode('data')
    
for file in os.listdir(directory):
     filename = os.fsdecode(file)
     if filename.endswith("_places.csv"): 
         upload_file(f'data/{filename}', 'dec-project-03', f'data/places/{filename}')