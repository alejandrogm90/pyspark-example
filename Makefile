docker_run = docker-compose run --rm pyspark python3


all: build

rerun:
	docker-compose down
	docker-compose up --build

up:
	docker-compose up

down:
	docker-compose down

build:
	docker-compose up --build

list:
	docker-compose ps

app1:
	$(docker_run) src/app_1.py

app2:
	$(docker_run) src/app_2.py data/cities.csv
