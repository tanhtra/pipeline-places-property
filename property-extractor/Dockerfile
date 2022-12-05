FROM python:3.9

WORKDIR /src

ENV PYTHONPATH=/src
ENV places_api_key=""
ENV snowflake_host=""
ENV snowflake_user=""
ENV snowflake_password=""
ENV snowflake_account=""
ENV snowflake_warehouse=""
ENV snowflake_database=""
ENV snowflake_schema=""

COPY /src .

RUN pip install -r requirements.txt 

CMD ["python", "producer.py"]