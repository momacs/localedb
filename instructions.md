


Quick start

```
## Start docker and postgis (may take a minute for postgres to become available)
docker-compose up -d

## Setup database
docker-compose run --rm localedb setup

## Load disease data
docker-compose run --rm localedb load dis c19

## Load geo data
docker-compose run --rm localedb load geo AK

## Load population data
docker-compose run --rm localedb load pop AK

## Display Info
docker-compose run --rm localedb info all
```



Notes


Removed `start stop uninstall update` as they don't really apply when everything is in docker-compose

Removed `is_prod` since script runs in a container

Because the bash script does not live inside the postgres container anymore
it is not possible to get the fs stats of the postgres data directory


