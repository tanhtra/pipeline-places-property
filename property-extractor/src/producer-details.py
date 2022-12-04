import json
from confluent_kafka.cimpl import Producer
import ccloud_lib

# Project 03
import pandas as pd
import requests
import time
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen

import os
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# config values 
limit = 100

def index_crawler(project_url):

    page = requests.get(project_url, headers={'User-Agent': 'Mozilla/5.0'})

    soup = BeautifulSoup(page.content, "html.parser")
    container_block = soup.find(class_="main-container")

    project_detail_name = container_block.find('h1', class_='page-title').text.strip()
    project_detail_features = container_block.find('ul', class_='features').find_all('li')
    project_detail_features_list = [ feature.text.strip() for feature in project_detail_features ]

    project_detail_meta = container_block.find('ul', class_='project-li-top')

    if project_detail_meta.find('span', text='Floors'):
        project_detail_floors = project_detail_meta.find('span', text='Floors').next_sibling.next_sibling.text.strip()
    else: 
        project_detail_floors = None

    if project_detail_meta.find('span', text='Developed by'):
        project_detail_developer = project_detail_meta.find('span', text='Developed by').next_sibling.next_sibling.text.strip()
    else: 
        project_detail_developer = None
    
    if project_detail_meta.find('span', text='Year built'):
        project_detail_year = project_detail_meta.find('span', text='Year built').next_sibling.next_sibling.text.strip()
    else: 
        project_detail_year = None

    project_map = container_block.find('a', id='go-to-map').find('img')['src']

    if container_block.find('a', id='open-tab-sale'):
        project_vacancy_sale = container_block.find('a', id='open-tab-sale').text
    else:
        project_vacancy_sale = '0'

    if container_block.find('a', id='open-tab-rent'):
        project_vacancy_rent = container_block.find('a', id='open-tab-rent').text
    else:
        project_vacancy_rent = '0'

    project_details = {
        "project_url" : project_url,
        "project_name" : project_detail_name,
        "project_features" : project_detail_features_list,
        "project_floors" : project_detail_floors,
        "project_developer" : project_detail_developer,
        "project_year" : project_detail_year,
        "project_map" : project_map,
        "project_sale" : project_vacancy_sale,
        "project_rent" : project_vacancy_rent
    }

    jd = json.dumps(project_details)
    time.sleep(1)

    return jd

def index_list():

    host = os.environ.get("snowflake_host")
    user = os.environ.get("snowflake_user")
    password = os.environ.get("snowflake_password")
    account = os.environ.get("snowflake_account")
    warehouse = os.environ.get("snowflake_warehouse")
    database = os.environ.get("snowflake_database")
    schema = os.environ.get("snowflake_schema")

    query = "select * from DBT_PROJECT03.STG__PROJECTS_LIST"
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

    return df['URL'].to_list()

def details_list():

    host = os.environ.get("snowflake_host")
    user = os.environ.get("snowflake_user")
    password = os.environ.get("snowflake_password")
    account = os.environ.get("snowflake_account")
    warehouse = os.environ.get("snowflake_warehouse")
    database = os.environ.get("snowflake_database")
    schema = os.environ.get("snowflake_schema")

    query = "select * from DBT_PROJECT03.STG__DETAILS_LIST"
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

    return df['URL'].to_list()

if __name__ == '__main__':
    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    producer = Producer(producer_conf)

    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    # Details Page Crawler
    load_dotenv()
    crawled_project_url = index_list()
    extracted_project_url = details_list()

    target_url = [ i for i in crawled_project_url if  i not in extracted_project_url ]

    for i, project_url in enumerate(target_url):
        details = index_crawler(project_url)

        producer.produce(topic, key=None, value=details, on_delivery=acked)
        producer.poll(0)

        if i >= limit:
            break;
    
    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))