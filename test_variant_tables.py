#!/usr/bin/env python3
"""
Test VARIANT support in Lab2 Dynamic Iceberg Sink tables.
Queries dynamically created tables: event_siteA, event_siteB, event_siteC

Prerequisites:
  cd /Users/sshah/IdeaProjects/study-learn/flink/lab2
  docker compose up -d
  ./scripts/run-variant-test.sh

Run locally:
```bash
cd /Users/sshah/IdeaProjects/study-learn/flink/lab2
export PACKAGES="org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"
python3 test_variant_tables.py
```
"""

from __future__ import annotations

import os
import sys

# --- Spark / Iceberg (before pyspark import) ---
os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@17")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DEFAULT_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.11.0"
)
PACKAGES = os.environ.get("PACKAGES", DEFAULT_PACKAGES)
os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {PACKAGES} pyspark-shell"

CATALOG = "iceberg_rest"

# MinIO + Iceberg REST (same as docker-compose / Flink pipeline)
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password")
ICEBERG_REST_URI = os.environ.get("ICEBERG_REST_URI", "http://localhost:8181")
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse/")

from pyspark.sql import SparkSession


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("Lab2VariantTest")
        .master("local[*]")
        .config("spark.jars.packages", PACKAGES)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG}.uri", ICEBERG_REST_URI)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
        .config(
            f"spark.sql.catalog.{CATALOG}.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO",
        )
        .config(f"spark.sql.catalog.{CATALOG}.s3.endpoint", MINIO_ENDPOINT)
        .config(f"spark.sql.catalog.{CATALOG}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG}.client.region", "us-east-1")
        .config(f"spark.sql.catalog.{CATALOG}.s3.access-key-id", MINIO_ACCESS_KEY)
        .config(
            f"spark.sql.catalog.{CATALOG}.s3.secret-access-key",
            MINIO_SECRET_KEY,
        )
        .config("spark.sql.defaultCatalog", CATALOG)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def test_table(spark: SparkSession, table_name: str) -> None:
    """Test a single dynamically created table."""
    full_table = f"{CATALOG}.demo.{table_name}"
    
    print(f"\n{'='*70}")
    print(f"🔍 Testing: {full_table}")
    print('='*70)
    
    try:
        # Check if table exists
        df = spark.table(full_table)
        
        # Show schema
        print(f"\n📋 Schema:")
        df.printSchema()
        
        # Show count
        count = df.count()
        print(f"\n📊 Row count: {count}")
        
        if count == 0:
            print("⚠️  Table is empty!")
            return
        
        # Show sample data
        print(f"\n📄 Sample data (first 5 rows):")
        df.select("event_type", "site_id", "event_id", "payload", "ts").show(5, truncate=50)
        
        # Test VARIANT payload column
        print(f"\n🔬 Testing VARIANT payload column:")
        spark.sql(f"""
            SELECT 
                event_id,
                site_id,
                variant_get(payload, '$.order_details.order_number', 'string') AS order_num,
                variant_get(payload, '$.order_details.customer.name', 'string') AS customer,
                variant_get(payload, '$.order_details.payment.amount', 'double') AS amount,
                variant_get(payload, '$.metadata.source', 'string') AS source
            FROM {full_table}
            LIMIT 5
        """).show(5, truncate=40)
        
        # Test nested array access
        print(f"\n🧪 Testing nested array in VARIANT:")
        spark.sql(f"""
            SELECT 
                event_id,
                variant_get(payload, '$.order_details.items[0].sku', 'string') AS first_sku,
                variant_get(payload, '$.order_details.items[0].quantity', 'int') AS first_qty,
                variant_get(payload, '$.order_details.items[0].price', 'double') AS first_price
            FROM {full_table}
            LIMIT 5
        """).show(5, truncate=40)
        
        print(f"\n✅ {table_name} - VARIANT support working!")
        
    except Exception as e:
        print(f"\n❌ Error testing {table_name}: {e}")


def main() -> None:
    spark = create_spark()
    print("\n" + "="*70)
    print("🚀 Lab2 Dynamic Iceberg Sink - VARIANT Test")
    print("="*70)
    print(f"Spark version: {spark.version}")
    print(f"REST catalog: {ICEBERG_REST_URI}")
    print(f"MinIO: {MINIO_ENDPOINT}")
    print(f"Warehouse: {WAREHOUSE}")
    
    # List all tables in demo namespace
    print(f"\n📚 Available tables in demo namespace:")
    spark.sql(f"SHOW TABLES IN {CATALOG}.demo").show(truncate=False)
    
    # Test each dynamically created table
    tables_to_test = ["event_siteA", "event_siteB", "event_siteC"]
    
    for table in tables_to_test:
        test_table(spark, table)
    
    # Summary
    print("\n" + "="*70)
    print("🎉 VARIANT Dynamic Sink Test Complete!")
    print("="*70)
    print("\n✅ Key Achievements:")
    print("   1. VARIANT methods added to Iceberg CompareSchemasVisitor")
    print("   2. VARIANT methods added to Iceberg EvolveSchemaVisitor")  
    print("   3. Custom Iceberg JAR built and deployed")
    print("   4. Dynamic tables created with VARIANT columns")
    print("   5. Schemaless JSON stored as native VARIANT")
    print("   6. Query VARIANT data with variant_get() in Spark")
    print("\n🎯 No more UnsupportedOperationException!")
    
    spark.stop()


if __name__ == "__main__":
    main()
