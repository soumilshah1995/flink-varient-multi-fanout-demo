# Lab 2: Flink Dynamic Iceberg Sink with VARIANT Support

**Status:** ✅ **WORKING**  
**Key Achievement:** Successfully implemented VARIANT support for Dynamic Iceberg Sink

---

## Quick Start

```bash
# Build everything (Iceberg JAR + Dynamic Generator)
./scripts/build-all.sh

# Start stack
docker compose build --no-cache
docker compose up -d

# Run test
./scripts/run-variant-test.sh

# Verify with Spark
python3 test_variant_tables.py
```

## What This Does

- **Routes events by `site_id`** to separate Iceberg tables dynamically
- **Uses VARIANT columns** for schemaless JSON (no Avro schemas needed!)
- **Creates 3 tables**: `event_siteA`, `event_siteB`, `event_siteC`
- **Stores nested JSON** as native VARIANT type

## The Achievement

We successfully added VARIANT support to Flink's Dynamic Iceberg Sink by implementing two missing methods:

1. **CompareSchemasVisitor.variant()** - Schema comparison (~15 lines)
2. **EvolveSchemaVisitor.variant()** - Schema evolution (~15 lines)

**Result:** Lab1 (static sink) + Lab2 (dynamic sink) both work with VARIANT! 🎉

## Architecture

```
Kafka (events_stream)
  ↓
Flink SQL (PARSE_JSON)
  ↓
Dynamic Iceberg Sink + Custom Generator
  ├→ demo.event_siteA (payload: VARIANT, 10 rows)
  ├→ demo.event_siteB (payload: VARIANT, 10 rows)
  └→ demo.event_siteC (payload: VARIANT, 10 rows)
```

## Documentation

📚 **Complete Guide:** [`docs/COMPLETE-GUIDE.md`](docs/COMPLETE-GUIDE.md)

Additional docs:
- [Original README](docs/README-ORIGINAL.md) - Initial goals
- [VARIANT Explanation](docs/README-VARIANT.md) - The problem explained
- [Lab1 vs Lab2 Comparison](docs/README-COMPARISON.md) - What was different
- [Fix Details](docs/VARIANT-FIX-NEEDED.md) - Technical implementation
- [Success Report](docs/VARIANT-PATCH-SUCCESS.md) - Final verification

## Services

| Service | URL | Purpose |
|---------|-----|---------|
| Flink UI | http://localhost:8081 | Monitor jobs |
| MinIO Console | http://localhost:9001 | Browse data (admin/password) |
| Kafka UI | http://localhost:9080 | Monitor topics |

## Key Files

| File | Purpose |
|------|---------|
| `pipeline.sql` | Flink SQL with VARIANT column |
| `producer.py` | Generates schemaless JSON events |
| `test_variant_tables.py` | Spark verification script |
| `dynamic-generator/src/.../SiteRoutingGenerator.java` | Custom routing logic |
| `iceberg/flink/v2.1/.../CompareSchemasVisitor.java` | ✅ Patched |
| `iceberg/flink/v2.1/.../EvolveSchemaVisitor.java` | ✅ Patched |

## Test Results

```
🎉 VARIANT Dynamic Sink Test Complete!

✅ Key Achievements:
   1. VARIANT methods added to Iceberg CompareSchemasVisitor
   2. VARIANT methods added to Iceberg EvolveSchemaVisitor
   3. Custom Iceberg JAR built and deployed
   4. Dynamic tables created with VARIANT columns
   5. Schemaless JSON stored as native VARIANT
   6. Query VARIANT data with variant_get() in Spark

🎯 No more UnsupportedOperationException!
```

---

**For full documentation, see:** [`docs/COMPLETE-GUIDE.md`](docs/COMPLETE-GUIDE.md)
