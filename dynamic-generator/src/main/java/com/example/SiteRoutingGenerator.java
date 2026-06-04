package com.example;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicTableRecordGenerator;
import org.apache.iceberg.types.Types;

/**
 * Simple Site Routing Generator with VARIANT support.
 * Routes events to tables: event_{site_id}
 * 
 * Schema is defined once here - clear and explicit.
 * MUCH simpler than auto-derivation for this use case!
 */
public class SiteRoutingGenerator extends DynamicTableRecordGenerator {

    public SiteRoutingGenerator(org.apache.flink.table.types.logical.RowType rowType) {
        super(rowType);
    }

    @Override
    public void generate(RowData inputRecord, Collector<DynamicRecord> out) throws Exception {
        // Extract routing key: site_id is at position 1
        String siteId = inputRecord.getString(1).toString();
        
        // Target table name: event_{site_id}
        String tableName = "event_" + siteId;
        TableIdentifier tableIdentifier = TableIdentifier.of("demo", tableName);
        
        // Simple, explicit schema definition with VARIANT
        // Input RowData from Flink SQL (pipeline.sql):
        //   0: event_type (STRING)
        //   1: site_id (STRING)
        //   2: event_id (STRING)
        //   3: payload (VARIANT) - from PARSE_JSON()
        //   4: ts (STRING)

        Schema schema = new Schema(
            Types.NestedField.optional(1, "event_type", Types.StringType.get()),
            Types.NestedField.required(2, "site_id", Types.StringType.get()),
            Types.NestedField.required(3, "event_id", Types.StringType.get()),
            Types.NestedField.optional(4, "payload", Types.VariantType.get()),
            Types.NestedField.optional(5, "ts", Types.StringType.get())
        );
        
        // Emit DynamicRecord
        out.collect(new DynamicRecord(
            tableIdentifier,
            null,
            schema,
            inputRecord,
            PartitionSpec.unpartitioned(),
            null,
            -1
        ));
    }
}
