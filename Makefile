build:
		docker build -t airflow-qoala -f ./services/airflow/Dockerfile ./services/airflow

up:
    docker compose --env-file .env --file ./services/iceberg_minio/docker-compose.yaml --project-directory . up -d
    docker compose --env-file .env --file ./services/trino/docker-compose.yaml --project-directory . up -d
    docker compose --env-file .env --file ./services/airflow/docker-compose.yaml --project-directory . up -d
    docker compose --env-file .env --file ./services/superset/docker-compose.yaml --project-directory . up -d

down:
		docker compose --env-file .env --file ./services/superset/docker-compose.yaml --project-directory . down
		docker compose --env-file .env --file ./services/airflow/docker-compose.yaml --project-directory . down
		docker compose --env-file .env --file ./services/trino/docker-compose.yaml --project-directory . down
		docker compose --env-file .env --file ./services/iceberg_minio/docker-compose.yaml --project-directory . down

.PHONY: up down