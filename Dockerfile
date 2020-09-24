
FROM python:3.8-slim

RUN apt-get update \
    && apt-get install -y postgresql-client curl wget unzip zip bc gettext-base \
    && apt-get install -y --no-install-recommends postgis \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary

COPY localedb /usr/local/bin/localedb

RUN mkdir -p /usr/share/localedb

COPY localedb_man.py /usr/share/localedb/localedb_man.py

VOLUME /usr/share/localedb/schemas /usr/share/localedb/scripts /usr/share/localedb/data

WORKDIR /usr/share/localedb

ENTRYPOINT ["localedb"]

