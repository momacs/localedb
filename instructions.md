


## Quick start

```
## Build docker container:
./build-docker.sh

## Start Docker Postgres (may take a minute for postgres to become available):
docker-compose up -d

## Setup database:
docker-compose run --rm localedb setup

## Load disease data:
docker-compose run --rm localedb load dis COVID-19

## Load geo data:
docker-compose run --rm localedb load geo AK

## Load population data:
docker-compose run --rm localedb load pop AK

## Load vaccination data:
docker-compose run --rm localedb load vax

## Display info:
docker-compose run --rm localedb info all

## Stop Docker Postgres:
docker-compose stop --timeout 300
docker-compose down --timeout 300
```



## Development

Any changes to the docker image require `./build-docker.sh` to be run to update the image.

Run `docker-compose up -d`
`localedb` and `localedb_man.py` are mounted as volumes. So any changes will take effect immediatly

`docker-compose run --rm localedb info`, edit file locally, run again and new changes will be present




## Notes

Removed `start stop uninstall update` as they don't really apply when everything is in docker-compose

Removed `is_prod` since script runs in a container

Because the bash script does not live inside the postgres container anymore
it is not possible to get the fs stats of the postgres data directory
