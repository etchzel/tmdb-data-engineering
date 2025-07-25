build:
		docker build -t job-runner -f ./runner/Dockerfile ./runner
		docker build -t airflow-qoala -f ./services/airflow/Dockerfile ./services/airflow

up:
		docker compose --env-file .env --file ./services/iceberg_minio/docker-compose.yaml --project-directory . up -d
		docker compose --env-file .env --file ./services/trino/docker-compose.yaml up -d
		docker compose --env-file .env --file ./services/airflow/docker-compose.yaml --project-directory . up -d
		docker compose --env-file .env --file ./services/superset/docker-compose.yaml --project-directory . up -d

down:
		docker compose --env-file .env --file ./services/superset/docker-compose.yaml --project-directory . down
		docker compose --env-file .env --file ./services/airflow/docker-compose.yaml --project-directory . down
		docker compose --env-file .env --file ./services/trino/docker-compose.yaml down
		docker compose --env-file .env --file ./services/iceberg_minio/docker-compose.yaml --project-directory . down

.PHONY: build up down