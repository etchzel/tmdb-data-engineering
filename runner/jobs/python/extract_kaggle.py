import argparse
import logging
import os
import pyarrow
from pyiceberg.catalog import load_catalog, Catalog

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S %Z"
)

def get_table(table_name: str, 
              schema: pyarrow.Schema) -> Catalog:
    """
    Retrieve or create an Iceberg table.
    If the table already exists, it will be loaded; otherwise, a new table will be created with the provided schema.

    Args:
        table_name (str): The name of the Iceberg table.
        schema (pyarrow.Schema): The schema for the Iceberg table.

    Returns:
        Catalog: The Iceberg table object.
    """

    # catalog config
    config = {
        "uri": "http://iceberg-rest-catalog:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://minio:9000",
        "s3.access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "s3.secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "s3.path_style_access": "true"
    }

    # load catalog and create namespace if not exists. namespace should be parameterized in next iteration
    catalog = load_catalog(name="iceberg", **config)
    catalog.create_namespace_if_not_exists("bronze")

    # check if table exists, if not create it
    if catalog.table_exists(identifier=("bronze", table_name)):
        table = catalog.load_table(identifier=("bronze", table_name))
    else:
        table = catalog.create_table(
            identifier=("bronze", table_name),
            schema=schema
        )

    return table

def main(filepath: str, 
         infer_rows: int,
         table_name: str,
         write_mode: str = "append") -> None:
    import polars as pl
    from datetime import datetime, UTC

    # conn properties & file source
    source = f"s3://warehouse/{filepath}"
    storage_options = {
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        "endpoint_url": "http://minio:9000"
    }

    # read NDJSON file and infer the schema by scanning a specified number of rows, then get the row count and add an ingestion timestamp
    logging.info(f"Scanning data from {source} with schema inference of {infer_rows} rows.")
    _ingest_ts = pl.lit(datetime.now(tz=UTC)).dt.timestamp("us").alias("_ingest_ts")
    data = pl.scan_ndjson(source=source, 
                          infer_schema_length=infer_rows,
                          storage_options=storage_options).with_columns(_ingest_ts)
    schema = pl.DataFrame({}, schema=data.collect_schema()).to_arrow().schema
    row_count = data.select(pl.len()).collect().item()
    
    logging.info(f"Retrieve or create Iceberg table: {table_name}.")
    table = get_table(table_name=table_name, schema=schema)

    logging.info(f"Writing data to Iceberg table: {table_name}.")
    if write_mode == "append":
        table.append(data.collect().to_arrow())
    else:
        table.overwrite(data.collect().to_arrow())

    logging.info(f"Rows written to Iceberg table {table_name}: {row_count}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data from kaggle dataset and save as Iceberg table.")
    parser.add_argument("--filepath", type=str, required=True, help="Path to the NDJSON file.")
    parser.add_argument("--infer_rows", type=int, default=10000, help="Number of rows to infer schema from.")
    parser.add_argument("--table_name", type=str, required=True, help="Name of the Iceberg table to create or append to.")
    parser.add_argument("--write_mode", type=str, default="append", choices=["append", "overwrite"], help="Write mode for the Iceberg table.")
    args = parser.parse_args()

    main(filepath=args.filepath,
         infer_rows=args.infer_rows, 
         table_name=args.table_name, 
         write_mode=args.write_mode)