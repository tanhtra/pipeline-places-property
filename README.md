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

### Setup .env file


- Rename the template.env file to .env
- Replace the tags inside the file with your snowflake and Foursquare API details

```
places_api_key=<FILL WITH PLACES API KEY>

snowflake_host=<XXX>.snowflakecomputing.com
snowflake_user=<FILL WITH USERNAME>
snowflake_password=<FILL WITH USER PASSWORD>
snowflake_account=<XXX>
snowflake_warehouse=<FILL WITH WAREHOUSE NAME>
snowflake_database=<FILL WITH DATABASE NAME>
snowflake_schema=<FILL WITH DATABASE SCHEMA>
```