# Property Crawler Pipeline

The PCP (Property Crawler Pipeline) is a combination concepts that forms an almost end to end data extraction pipeline using (but not limited to)

- Beautiful Soup 4 as the crawler engine
- FourSquare API the source for places information
- Kafka (Confluent) as event-driven microservices tool
- Airbyte (Cloud) as the S3 to Snowflake data replication tool
- Snowflake as the data warehouse
- dbt as the data transformation tool
- AWS (S3) as the file / export repository
- and Streamlit as dashboard


# Getting started

## Transformation tools

### dbt

- Sign up to dbt cloud
- Initialise dbt cloud using the dbt_project.yml

### AWS S3

- Sign up to AWS and create a bucket - using the same region as your Confluent cluster
- Create an access key for the .env file

### Confluent (Kafka)

- Sign up to Confluent
- Create a new cluster and generate a new API key

- Replace the tags inside the kafka.config files with your Confluent details

```
# Required connection configs for Kafka producer, consumer, and admin

# bootstrap server - do not include the protocol e.g. pkc-2396y.us-east-1.aws.confluent.cloud:8443
bootstrap.servers=<FILL WITH CONFLUENT BOOTSTRAP SERVER>

security.protocol=SASL_SSL
sasl.mechanisms=PLAIN

# username for kafka
sasl.username=<FILL WITH CONFLUENT USERNAME>

# password for kafka
sasl.password=<FILL WITH CONFLUENT PASSWORD>
compression.type=lz4
batch.size=10000
request.timeout.ms=120000
queue.buffering.max.messages=200000
```

### Airbyte

- Sign up to Airbyte cloud
- Set up Snowflake destination
- Set up S3 source(s) for the Transit (CSV), Places (CSV), Index (JSON) and Details (JSON) files 
- Create Connection(s)

### Foursquare

- Sign up to Foursquare developer programme
- Generate API key for places extractor

## Setup .env file

- Rename the template.env file to .env
- Replace the tags inside the file with your snowflake and Foursquare API details

```
places_api_key=<FILL WITH PLACES API KEY>

aws_access_key=<FILL WITH AWS S3 BUCKET ACCESS KEY>
aws_secret_key=<FILL WITH AWS S3 BUCKET SECRET KEY>

snowflake_host=<XXX>.snowflakecomputing.com
snowflake_user=<FILL WITH USERNAME>
snowflake_password=<FILL WITH USER PASSWORD>
snowflake_account=<XXX>
snowflake_warehouse=<FILL WITH WAREHOUSE NAME>
snowflake_database=<FILL WITH DATABASE NAME>
snowflake_schema=<FILL WITH DATABASE SCHEMA>
```