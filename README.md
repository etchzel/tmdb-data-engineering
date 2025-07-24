# TMDB Movies and Series End to End Data Engineering

This repo is created to demonstrate an end-to-end data engineering pipeline for TMDB movies and series data.

## Introduction

This project focuses on building a data engineering pipeline to extract, transform, and load (ETL) data related to movies and series from TMDB. The pipeline is designed to handle large-scale data efficiently and provide insights through processed datasets.

## Architecture

This project follows a Lakehouse architecture setup, which combines the best features of data lakes and data warehouses. The architecture is designed to handle large-scale data processing and analytics efficiently.

### Tech Stack

- **Airflow**: For orchestrating and scheduling data pipeline workflows.
- **MinIO**: As the object storage layer for the data lake.
- **Iceberg & Iceberg REST Catalog**: For managing large-scale tabular  with features such as ACID transactions, Schema Evolution, and Time Travel.
- **Trino**: For distributed SQL query execution and analytics on the data lake.

## Features

- Data extraction from Kaggle & TMDB API.
- Data transformation and cleaning.
- Loading data into a data lakehouse setup.
- Containerized environment for easy deployment.

## Prerequisites

- Docker
- Make

## Setup Instructions

1. Clone the repository:

   ```bash
   git clone <repository-url>
   ```

2. Navigate to the project directory:

   ```bash
   cd qoala-test
   ```

   NOTE: On Mac with ARM architecture CPU, make sure to use `linux/arm64` images.

3. Build the Docker image:

   ```bash
   make build
   ```

4. Run the container:

   ```bash
   docker run -d tmdb-pipeline
   ```

## Usage

- To extract data, run the `extract.py` script located in the `jobs/` folder.
- Additional scripts and jobs can be added to the pipeline as needed.

## Folder Structure

```txt
├── services/                 # Contains service-specific configurations
│   ├── airflow/              # Airflow service setup
│   ├── iceberg_minio/        # Iceberg & MinIO service setup
│   ├── superset/             # Superset service setup
│   ├── trino/                # Airflow service setup
├── runner/                   # Contains files related to task execution
│   ├── jobs/                 # Contains job scripts
│   │   └── python/           # Python job scripts
│   │   └── sql/              # SQL job scripts
│   ├── Dockerfile            # Dockerfile for task isolation
│   └── requirements.txt      # Script for data extraction
├── .env                      # Environment variables file
├── .gitignore                # Ignore files to push to git
├── Makefile                  # Makefile for build and utility commands
├── README.md                 # Project documentation
```

## Point of Improvement

- Add SSL
- Use secrets instead of env var to secure credentials
