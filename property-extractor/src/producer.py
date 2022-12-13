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
URL = 'https://www.thailand-property.com/condos/all/bangkok'
limit = -1

def index_crawler(URL):
    # Index Page Entry


    # Setting up BeautifulSoup4 Variables
    page = requests.get(URL, headers={'User-Agent': 'Mozilla/5.0'})

    soup = BeautifulSoup(page.content, "html.parser")
    result_block = soup.find(id="search-results") ## Needs a Try block

    # Checking if there are any crawlable objects
    if result_block.find_all('article','projects-list'):
        projects = result_block.find_all('article','projects-list')
    else:
        return

    # Get a list of already-crawled index pages
    crawled_project_url = index_list()
    
    # Crawl through the pages
    for project in projects:
        project_name = project.find('h4', class_='project-name').text
        project_url = project.find('a')['href']
        project_vacancy = project.find('div', class_='project-total-units').find_all('a')
        project_rent = project_vacancy[1].text.strip().split()[0]
        project_sale = project_vacancy[0].text.strip().split()[0]

        if project_url in crawled_project_url:
            print(f'Skipping {project_url} - already crawled')
            continue
        
        # Casting the extracted data into a workable format
        if None not in (project_name, project_url):
            project_index = {
                'project_name' : project_name,
                'project_url' : project_url,
                'project_rent' : project_rent,
                'project_sale' : project_sale
            } 
            jd = json.dumps(project_index)
            time.sleep(2)
            yield jd
    return

def index_list():
    load_dotenv()

    # Get list of already crawled target URLs from Snowflake

    host = os.environ.get("snowflake_host")
    user = os.environ.get("snowflake_user")
    password = os.environ.get("snowflake_password")
    account = os.environ.get("snowflake_account")
    warehouse = os.environ.get("snowflake_warehouse")
    database = os.environ.get("snowflake_database")
    schema = os.environ.get("snowflake_schema")

    query = "select * from RAW.JSON__INDEX_META"
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

    return df['PROJECT_URL'].to_list()

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

    #for doc in read_file(input_file, num_messages):
    #    producer.produce(topic, key=None, value=json.dumps(doc), on_delivery=acked)
    #    producer.poll(0)
    #producer.flush()
    #print("{} messages were produced to topic {}!".format(delivered_records, topic))

    # Index Page Crawler
    page = requests.get(URL, headers={'User-Agent': 'Mozilla/5.0'})

    soup = BeautifulSoup(page.content, "html.parser")
    index_content = soup.find(id="content")

    project_count = index_content.find('span', id="properties_total").text

    project_count = int(project_count.replace(',',''))
    project_page = int(project_count / 20) + 1

    if limit != -1:
        project_page = limit + 1

    for i in range(1,project_page):
        for jd in index_crawler(f"{URL}?page={i}"):
                print(jd)
                producer.produce(topic, key=None, value=jd, on_delivery=acked)
                producer.poll(0)
    
    producer.flush()
    print("{} messages were produced to topic {}!".format(delivered_records, topic))