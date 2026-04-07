up:
	docker compose up -d

up-dev:
	docker build -t sketchdb-base:latest -f ../asap-common/installation/Dockerfile ../asap-common
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

down:
	docker compose down

down-dev:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml down
