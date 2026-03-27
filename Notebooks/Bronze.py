# GlobalMart — Bronze layer (Delta Live Tables + Auto Loader)

# Pipeline type: DLT (Python). Target: `globalmart.bronze`.

import dlt
from pyspark.sql import functions as F

CATALOG = "globalmart"
RAW_BASE = f"/Volumes/{CATALOG}/raw/landing_files/GlobalMart_Retail_Data"
SCHEMA_HINT_ROOT = f"/Volumes/{CATALOG}/raw/landing_files/_schemas/dlt_bronze"


# Common CSV streaming reader

def csv_streaming(file_glob_pattern: str, schema_name: str):
    schema_loc = f"{SCHEMA_HINT_ROOT}/{schema_name}"
    
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", file_glob_pattern)
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("readerCaseSensitive", "false")
        .load(RAW_BASE)
        # lineage columns (standard Bronze pattern)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
    )


# Common JSON streaming reader

def json_streaming(file_glob_pattern: str, schema_name: str):
    schema_loc = f"{SCHEMA_HINT_ROOT}/{schema_name}_json"
    
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", file_glob_pattern)
        .option("cloudFiles.schemaLocation", schema_loc)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(RAW_BASE)
        # lineage columns (standard Bronze pattern)
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file_path", F.col("_metadata.file_path"))
    )


# Bronze tables (6 entities)

@dlt.table(
    name="customers",
    comment="Bronze customers — raw ingestion from 6 regional files. Schema is union of all variations.",
    table_properties={"quality": "bronze"},
)
def ingest_customers():
    return csv_streaming("customers_*.csv", "customers")


@dlt.table(
    name="orders",
    comment="Bronze orders — raw ingestion from multiple regions.",
    table_properties={"quality": "bronze"},
)
def ingest_orders():
    return csv_streaming("orders_*.csv", "orders")


@dlt.table(
    name="transactions",
    comment="Bronze transactions — raw ingestion.",
    table_properties={"quality": "bronze"},
)
def ingest_transactions():
    return csv_streaming("transactions_*.csv", "transactions")


@dlt.table(
    name="returns",
    comment="Bronze returns — JSON ingestion with schema evolution; raw structure preserved.",
    table_properties={"quality": "bronze"},
)
def ingest_returns():
    return json_streaming("returns_*.json", "returns")


@dlt.table(
    name="products",
    comment="Bronze products — JSON ingestion",
    table_properties={"quality": "bronze"},
)
def ingest_products():
    return json_streaming("products.json", "products")


@dlt.table(
    name="vendors",
    comment="Bronze vendors — single CSV file ingested as-is.",
    table_properties={"quality": "bronze"},
)
def ingest_vendors():
    return csv_streaming("vendors.csv", "vendors")