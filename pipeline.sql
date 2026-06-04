-- ==============================================================================
-- Lab 2: Dynamic Iceberg Sink with VARIANT (like lab1)
-- ==============================================================================
-- Attempt to use VARIANT with Dynamic Sink (same approach as lab1)
-- 
-- Prerequisites:
--   1. Build JAR: cd dynamic-generator && mvn clean package
--   2. Deploy JAR: ./scripts/deploy-jar.sh
--   3. Restart Flink: docker restart lab2-jobmanager-1 lab2-taskmanager-1
--   4. Run: docker exec -i lab2-jobmanager-1 /opt/flink/bin/sql-client.sh < pipeline.sql
-- ==============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'parallelism.default' = '1';
SET 'pipeline.name' = 'dynamic-iceberg-variant';


-- Step 1: Create Iceberg Catalog
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = 'password',
  'warehouse' = 's3://warehouse/'
);

USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS demo;


-- Step 2: Kafka Source (default catalog)
USE CATALOG default_catalog;

CREATE TABLE events_source (
    event_type STRING,
    site_id STRING,
    event_id STRING,
    payload STRING,  -- Raw JSON string from Kafka
    ts STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'events_stream',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'flink-dynamic-sink-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);


-- Step 3: Dynamic Sink with VARIANT (like lab1!)
-- Define sink table with VARIANT column
CREATE TABLE events_dynamic_sink (
    event_type STRING,
    site_id STRING,
    event_id STRING,
    payload VARIANT,  -- VARIANT type! Same as lab1
    ts STRING
) WITH (
    'connector' = 'iceberg',
    'catalog-type' = 'rest',
    'catalog-name' = 'iceberg_catalog',
    'uri' = 'http://iceberg-rest:8181',
    'warehouse' = 's3://warehouse/',
    
    -- S3/MinIO configuration
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.path-style-access' = 'true',
    's3.access-key-id' = 'admin',
    's3.secret-access-key' = 'password',
    
    -- Dynamic Sink options
    'use-dynamic-iceberg-sink' = 'true',
    'dynamic-record-generator-impl' = 'com.example.SiteRoutingGenerator',
    
    -- VARIANT table properties (Iceberg V3)
    'table.props.format-version' = '3',
    'table.props.write.format.default' = 'parquet',
    'table.props.shred-variants' = 'true',
    'table.props.variant-inference-buffer-size' = '100'
);


-- Step 4: INSERT with PARSE_JSON (like lab1!)
INSERT INTO events_dynamic_sink
SELECT 
    event_type, 
    site_id, 
    event_id, 
    PARSE_JSON(payload),  -- Convert STRING → VARIANT
    ts
FROM events_source;


