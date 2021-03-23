FROM python:3.8-slim

RUN apt-get update && apt-get install -y wget lsb-release gnupg

RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list' \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update

RUN apt-get update \
    && apt-get install -y postgresql-client-12 curl wget unzip zip bc gettext-base \
    && apt-get install -y --no-install-recommends postgis \
    && rm -rf /var/lib/apt/lists/*

COPY docker-requirements.txt /tmp/docker-requirements.txt

RUN pip install -r /tmp/docker-requirements.txt

COPY localedb /usr/local/bin/localedb

RUN mkdir -p /usr/share/localedb

COPY localedb_man.py /usr/share/localedb/localedb_man.py
COPY scripts /usr/share/localedb/scripts

VOLUME /usr/share/localedb/schemas /usr/share/localedb/scripts /usr/share/localedb/data

WORKDIR /usr/share/localedb

ENTRYPOINT ["localedb"]
