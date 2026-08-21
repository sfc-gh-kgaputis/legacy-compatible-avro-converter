# Legacy Snowflake Avro Converter Analysis

## Executive summary

The legacy `com.snowflake.kafka.connector.records.SnowflakeAvroConverter` and Confluent's
`io.confluent.connect.avro.AvroConverter` both deserialize Avro, but they do not guarantee the same
JSON document shape.

The legacy converter decoded Avro directly into a `GenericRecord` and converted that record
directly to JSON. Confluent's converter first maps Avro into Kafka Connect's type system. Kafka
Connector 4.x then recursively maps the resulting Connect `Struct` into Java maps for SSv2. Avro
features that Kafka Connect cannot represent natively, most notably multi-type unions, can therefore
gain synthetic branch objects and additional JSON path components.

This matters even when schematization is disabled. The following configuration preserves the
`RECORD_CONTENT VARIANT` table layout, but it does not restore the legacy converter's internal
Avro-to-JSON representation:

```
snowflake.enable.schematization=false
```

## History and removal timeline

`SnowflakeAvroConverter` was a Snowflake-specific Kafka Connect converter used by the legacy
connector data path. Important milestones include:

| Date | Commit | Change |
|---|---|---|
| 2020-12-21 | `ac42b562` | Added Avro decimal/BigDecimal handling. |
| 2021-09-14 | `03056e8d` | Added the fixed `reader.schema` option. |
| 2022-03-04 | `b196499b` | Prohibited Snowflake custom converters for Snowpipe Streaming. The class remained available for the legacy Snowpipe path. |
| 2024-11-12 | `fc0c9ec6` | Migrated its JSON handling to the unshaded Jackson packages. |
| 2025-11-07 | `3edd4d11` | Removed SSv1, legacy Snowpipe, and their converter/wrapper infrastructure. |

The converter remained present and byte-for-byte unchanged through:

- Kafka Connector `3.2.x`;
- Kafka Connector `3.5.4`; and
- Kafka Connector `4.0.0-rc1`.

It was absent starting with `4.0.0-rc2` and is not part of Kafka Connector 4.0 or 4.1.

The deletion was not an Avro-specific defect fix. Commit `3edd4d11` was a broad architectural
cleanup that removed the SSv1 and legacy file-based Snowpipe implementations. The converter was
deleted because it depended on that removed infrastructure.

## Why the legacy implementation is not directly compatible with KC 4.x

The legacy converter did not return ordinary Kafka Connect records. It returned Snowflake-specific
types:

- `SnowflakeJsonSchema`; and
- `SnowflakeRecordContent`.

Those types carried JSON nodes, broken-record bytes, null-record state, and the Avro schema ID. The
legacy `RecordService`, Snowpipe sink service, and SSv1 streaming service contained explicit logic
for detecting and unpacking them.

Kafka Connector 4.x removed those classes and services. Its SSv2 path instead expects the worker's
converter to return standard Kafka Connect values, normally `Struct` or `Map`, and eventually calls
the SSv2 SDK with a `Map<String, Object>`:

```
Kafka converter
  -> Kafka Connect Schema + value
  -> SnowflakeSinkRecord
  -> Map<String, Object>
  -> SSv2 appendRow()
```

Copying the old class into KC 4.x would fail for two reasons:

1. Its output classes and the code that consumed them no longer exist.
2. Its direct `GenericRecord`-to-JSON contract does not match the standard Connect-value contract
   used by the SSv2 path.

A KC 4.x compatibility converter is feasible, but it must retain the legacy Avro decoding semantics
while returning standard JSON-compatible Java maps, lists, scalars, and nulls. It should not restore
the deleted SSv1 wrapper types.

## Compared data paths

Legacy path reproduced from the v3.5.4 converter:

```
Confluent-framed Avro bytes
  -> GenericDatumReader(writer schema, optional reader schema)
  -> GenericRecord.toString()
  -> JSON document
```

Current KC 4.1.0 path:

```
Confluent-framed Avro bytes
  -> io.confluent.connect.avro.AvroConverter
  -> Kafka Connect Schema + Struct
  -> SnowflakeSinkRecord.from(..., enableSchematization=false, ...)
  -> RECORD_CONTENT value sent toward SSv2
```

## Confirmed behavior matrix

| Behavior | Legacy direct decoder | Confluent + KC 4.1.0 |
|---|---|---|
| Nullable union `['null', 'string']` | Unwrapped string | Unwrapped string |
| Multi-type union `['null', 'string', 'int']` | Unwrapped active value | Branch wrapper such as `{'string':'value'}` |
| Ordinary nested record | Nesting preserved | Nesting preserved |
| Explicit nullable-union null | JSON null | JSON null |
| Configured reader schema | Applies projection/defaults | Normal converter uses registered writer schema |
| Decimal logical type | JSON number | Same JSON number in the tested case |
| Date logical type | Integer days from epoch | ISO timestamp string at midnight UTC |
| Timestamp-millis | Integer epoch milliseconds | ISO timestamp string |

The tests deliberately separate confirmed differences from claims that are not universal.
Confluent 7.9.2 does not wrap a simple two-branch nullable union and does not flatten an ordinary
nested record. Path breakage is reproduced with a genuine multi-type union, where the legacy path
`RECORD_CONTENT:value` becomes `RECORD_CONTENT:value:string`.

## Behavior differences in detail

### Simple nullable unions

For a common nullable field:

```
{"name":"status","type":["null","string"],"default":null}
```

Confluent 7.9.2 has `flatten.singleton.unions=true` by default. Both tested paths produce:

```
{"status":"active"}
```

The general statement that all Avro union values become `{"string":"value"}` is not correct for
this two-branch nullable case.

### Multi-type unions

Kafka Connect has no native union type. For a field with multiple non-null alternatives:

```
{"name":"value","type":["null","string","int"],"default":null}
```

The tested representations are:

```
Legacy:
{"value":"person@example.com"}

Confluent + KC 4.1.0:
{"value":{"string":"person@example.com"}}
```

This changes the Snowflake JSON path:

```
Legacy path:  RECORD_CONTENT:value
Current path: RECORD_CONTENT:value:string
```

A query using the legacy path returns SQL `NULL` because the value is now below an additional
object level. The data is not necessarily absent; the old path no longer identifies it.

When a multi-type union appears inside nested records, the wrapper is inserted at that nested
location. This can look like a broader nesting or placement change even though ordinary record
nesting itself remains intact.

### Nested records

An ordinary nested Avro record without a multi-type union retains its hierarchy in both tested
paths. The test suite verifies that a value at `customer.email` remains at that path.

Reports of nested-path changes should therefore be checked for a union, reader-schema projection,
alias, or schema evolution boundary at the point where the path diverges.

### Reader schema and defaults

The legacy converter supported a connector-specific `reader.schema` setting. It passed the writer
and reader schemas to `GenericDatumReader`, enabling Avro schema resolution before JSON generation.
That could:

- project or reorder fields;
- promote compatible numeric types;
- resolve renamed fields when aliases were available; and
- insert reader-schema defaults for fields missing from the writer schema.

The normal Confluent converter configuration used by KC 4.1.0 does not consume the legacy inline
`reader.schema` setting. The test suite shows a reader-only field with a default appearing in the legacy
result but not in the normal current result.

This is another reason a field may appear null or absent after migration: the field may previously
have been materialized by reader-schema resolution rather than being present in the writer payload.

### Logical types

The legacy converter registered Avro's `DecimalConversion`, but it otherwise generated JSON from
the resolved `GenericRecord` representation. The current path converts Avro logical types into
Kafka Connect logical types and then applies KC 4.1.0 normalization.

Confirmed examples:

- Decimal `90.0000` is a JSON number in both tested paths.
- Avro `date` is an integer number of days from the epoch in the legacy JSON, but KC 4.1.0 emits an
  ISO timestamp string at midnight UTC.
- Avro `timestamp-millis` is an integer epoch-millisecond value in the legacy JSON, but KC 4.1.0
  emits an ISO timestamp string.

These are value-representation differences rather than merely path differences. Existing SQL casts,
JSON comparisons, downstream views, and hashing logic may need adjustment even when the field name
is unchanged.

### Null semantics

For an explicit null inside the tested nullable union, both paths preserve JSON null. However,
top-level null records are handled by different surrounding architectures:

- The legacy converter created a special `SnowflakeRecordContent` null marker containing an empty
  JSON object and a separate internal null-record flag.
- KC 4.1.0 receives a null Connect value and classifies the `SinkRecord` as a tombstone before
  constructing `RECORD_CONTENT`.

Top-level tombstone behavior should therefore be evaluated separately from a null field inside an
otherwise valid Avro record.

## Migration impact

Applications are most likely to be affected when they depend on the exact semi-structured document
contract rather than only on successful Avro deserialization. Review:

- Snowflake views and queries with hard-coded `RECORD_CONTENT` paths;
- streams, tasks, dynamic tables, and materialized transformations using those paths;
- downstream tools that distinguish missing fields from explicit JSON null;
- SQL casts that expect numeric epoch values instead of ISO temporal strings;
- fields populated by the legacy `reader.schema`; and
- hashes or equality checks over the complete VARIANT document.

The safest validation method is to run representative Confluent-framed messages through both paths
and compare the resulting JSON trees. Your own schemas should be added to this test harness when
available, especially schemas containing nested unions, aliases, defaults, logical types, arrays,
and maps.

## Compatibility implementation direction

A production compatibility converter for KC 4.x should:

1. Parse the Confluent magic byte and schema ID.
2. Fetch the writer schema.
3. Apply optional writer/reader resolution with `GenericDatumReader`.
4. Preserve the legacy union, nesting, logical-type, default, and null representation intentionally.
5. Return standard JSON-compatible Java values for the KC 4.x/SSv2 pipeline.

It should initially be scoped to `snowflake.enable.schematization=false`, because its purpose is to
preserve the legacy `RECORD_CONTENT` document contract. Supporting schematization would introduce a
separate set of column mapping and schema evolution semantics.

## Evidence in this repository

The analysis is backed by executable tests rather than only source inspection:

- `LegacySnowflakeAvroDecoder.java` adapts the direct decoding logic from KC v3.5.4.
- `AvroTestSupport.java` sends identical Confluent-framed bytes through both paths.
- `AvroCompatibilityTest.java` asserts the resulting JSON shapes and logical-type values.

Run the evidence suite with:

```
mvn test
```

This repository is a compatibility investigation and test harness, not a supported production
converter.