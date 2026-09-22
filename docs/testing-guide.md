# Legacy Compatible Avro Converter — Testing Guide

**Version 1.1.0** · Kafka Connect converter for migrating from Kafka Connector v3 to v4 without
changing downstream SQL.

---

## Support status — read first

This is a **custom `value.converter`**, which is not a supported Kafka Connector 4.x
configuration path. Custom converters are not reviewed, tested, or maintained by Snowflake
Engineering, and the v4 startup validator does not inspect `value.converter`, so the connector
will start regardless.

Use this in a **non-production environment** to evaluate whether the approach solves your
problem. Obtain written approval from Snowflake Support or the responsible product team before
any production use. This project is not affiliated with or endorsed by Snowflake as a product.

---

## What problem this solves

If a field in your Avro schema is a union of **named record** branches — a common shape in
event-sourced topics, where one field carries one of many event types — the two converters
represent it differently. Taking a field named `action` as the example:

| | `action` in `RECORD_CONTENT` |
|---|---|
| Legacy `SnowflakeAvroConverter` (v3) | The active branch's fields, inlined directly under `action` |
| `io.confluent.connect.avro.AvroConverter` (v4) | One key per branch name — inactive branches `null`, active branch nested one level deeper |

So after a straightforward migration, every existing `RECORD_CONTENT:action:<field>` expression
returns `NULL`. The value is still present, but it has moved to
`RECORD_CONTENT:action:<BranchName>:<field>`.

This converter restores the legacy representation **at ingest**, so the landing table looks the
way it did under v3 and your existing streams, tasks, and views need no change. It applies to any
union-valued field, at any depth, not just a field named `action`.

### How it works

It delegates all Schema Registry work — wire-format framing, schema lookup by ID, caching,
authentication, TLS — to Confluent's `KafkaAvroDeserializer`, then replaces only the
Avro-to-Connect mapping step: each Avro union is resolved to its active branch and that branch's
fields are inlined. Nothing else about the record is altered.

---

## Assumptions and preconditions

Please confirm each of these against your environment. Where one does not hold, the note says what
to expect.

### Environment

| # | Assumption | If it does not hold |
|---|---|---|
| 1 | Kafka Connector **4.x**, Snowpipe Streaming high-performance architecture. Validated on **4.1.0** | Untested on other 4.x releases; 4.1.0 is the reference |
| 2 | Kafka Connect API **3.9.x** (what KC 4.1.0 targets) | Newer Connect API versions are untested by Snowflake for the connector itself |
| 3 | Java **11 or later** on the Connect worker | The JAR is Java 11 bytecode and will not load on Java 8 |

### Configuration

| # | Assumption | If it does not hold |
|---|---|---|
| 4 | **`snowflake.enable.schematization=false`** | This is the only tested configuration. With schematization enabled, column mapping and schema evolution behavior are untested |
| 5 | **Confluent Schema Registry**, standard wire format: magic byte `0x00` + 4-byte schema ID | A Schema Registry is **required** — Confluent's converter raises `ConfigException` without `schema.registry.url`, and this converter inherits that. If your v3 setup used `SnowflakeAvroConverterWithoutSchemaRegistry`, or a non-Confluent registry such as AWS Glue or Apicurio, **this converter does not apply**. See [confluent-compatibility.md](confluent-compatibility.md) |
| 6 | Any legacy `reader.schema` is one fixed Avro schema applied to all values handled by this converter instance | Supported in 1.1.0. The value must be an inline schema string; it is parsed at startup and uses Avro writer-to-reader resolution for every payload |
| 7 | Applied as `value.converter` only | The key converter is untouched. `fromConnectData` delegates to a standard `AvroConverter`; this converter is sink-focused |
| 8 | `avro.use.logical.type.converters` and `specific.avro.reader` are **not** relied upon | Both are forced to `false` internally; your values are ignored. Enabling logical type converters produces values Kafka Connector 4.1.0 rejects outright, so this is a hard requirement rather than a preference. See [confluent-compatibility.md](confluent-compatibility.md) |
| 9 | Any SMTs you configure are safe to run **after** conversion | SMTs see the already-collapsed value |

### Data and representation

| # | Assumption | Note |
|---|---|---|
| 10 | An Avro union holds exactly one branch value | Guaranteed by Avro itself, so the collapse is always well-defined. The 45 nulls you see today are an artifact of the Connect representation, not of your data |
| 11 | A union that includes `null` and holds `null` should stay `null` | Handled: `action` becomes `null` and the record is retained, not dropped |
| 12 | Temporal logical types (date, time, timestamp) arrive as **raw epoch integers/longs** | This matches legacy v3 behavior. If any downstream consumer expects ISO-8601 strings, it will see numbers instead |
| 13 | **Decimal** logical types (`bytes`/`fixed`) become JSON numbers via `double` | Values needing more than ~15 significant digits lose precision. The wide parity corpus confirms the original converter's final Jackson JSON had the same loss for a tested 30-digit value |
| 14 | Enums render as the symbol string; UUID logical type renders as a string | Matches legacy |
| 15 | Tombstones (null Kafka value) pass through as a null record | Preserved for v4 offset handling |
| 16 | JSON **key order** is not preserved end to end | Snowflake normalizes VARIANT object keys alphabetically regardless of converter. Compare by path, not by JSON text |

### Coverage limits

- **Your specific branch shapes.** The test suite exercises a 46-branch union with three distinct branch field lists. The collapse does not depend on branch count or shape, but branch-specific surprises cannot be ruled out without your schema.
- **Error handling policy.** Conversion failures throw `DataException`; standard Connect `errors.tolerance` and dead-letter-queue settings apply, but were not exercised.
- **Kubernetes / Strimzi.** Validated under Docker Compose on ARM64, not on a Strimzi cluster.
- **Throughput and latency.** Correctness only; no performance measurement.
- **Offset migration and cutover.** Not rehearsed. See your migration runbook.
- **Custom logical types.** Unregistered custom types follow their underlying Avro type; add an
  explicit fixture for any custom conversion you depend on.

The executable wide-range matrix covers primitives and numeric boundaries, non-finite numbers,
escaping, plain bytes and fixed, populated arrays/maps, nested containers, finite recursive values,
union placement, the standard temporal logical-type family, decimal bytes/fixed, and binary reader
defaults. See [legacy-json-parity.md](legacy-json-parity.md).

---

## Installation

### The classloader requirement — this matters

Kafka Connect gives every top-level `plugin.path` entry its **own isolated classloader**. This JAR
is deliberately thin (12 KB) and carries no dependencies: it resolves the Confluent Avro and Apache
Avro classes at runtime from the **Snowflake connector's own fat JAR**, which bundles them
unrelocated.

Therefore the JAR must be placed **directly alongside** `snowflake-kafka-connector-*.jar`, in the
same directory:

```
/opt/kafka/plugins/snowflake-kafka-connector/
    snowflake-kafka-connector-4.1.0.jar
    bc-fips-*.jar
    bcpkix-fips-*.jar
    legacy-compatible-avro-converter-1.1.0.jar    <-- add this file here
```

Placing it in a **separate** plugin directory will fail with `NoClassDefFoundError`, because that
directory gets a different classloader that cannot see the bundled Confluent classes.

You do **not** need to add any Confluent Avro JARs. They already ship inside the Snowflake
connector.

### Verify it loaded

Restart the Connect worker, then:

```bash
curl -s "http://<connect-host>:8083/connector-plugins?connectorsOnly=false" \
  | jq -r '.[] | select(.type=="converter") | .class'
```

`com.snowflake.labs.kafka.converter.LegacyCompatibleAvroConverter` should appear in the list. If it
does not, the JAR is in the wrong directory or the worker did not restart.

### Strimzi / container images

Add the JAR into the connector's plugin directory in your image build:

```dockerfile
COPY legacy-compatible-avro-converter-1.1.0.jar \
     /opt/kafka/plugins/snowflake-kafka-connector/
```

A `.zip` is also provided if your tooling prefers to unpack an archive into that directory. It
contains only this JAR.

---

## Connector configuration

Change `value.converter` to this class and keep your existing Schema Registry settings:

```properties
value.converter=com.snowflake.labs.kafka.converter.LegacyCompatibleAvroConverter
value.converter.schema.registry.url=https://your-schema-registry:8081
value.converter.basic.auth.credentials.source=USER_INFO
value.converter.basic.auth.user.info=<user>:<password>

# Required for the RECORD_CONTENT VARIANT contract
snowflake.enable.schematization=false
```

Except for the locally parsed `value.converter.reader.schema` and
`value.converter.legacy.json.parity.enabled`, all `value.converter.*` properties are passed through
to the underlying Confluent deserializer, so TLS and authentication settings work exactly as they
do with the standard converter.

If the v3 connector used a fixed reader schema, configure the same schema as a single-line JSON
string:

```properties
value.converter.reader.schema={"type":"record","name":"Event","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"source","type":"string","default":"legacy"}]}
```

The converter fails configuration immediately if this value is not a string or cannot be parsed as
an Avro schema. At runtime, reader defaults, field aliases, field projection, and compatible numeric
promotions follow the Avro library in the tested dependency matrix. A missing required reader field
or incompatible type fails that record as a `DataException`; normal Connect error-tolerance and
dead-letter-queue policy then applies.

The default `value.converter.legacy.json.parity.enabled=true` matches the original Snowflake JSON
rendering for plain `bytes`, plain `fixed`, and non-finite numbers. Existing 1.0.0 adopters should
test the default first. If downstream code depends on the 1.0.0 Java representations, set the flag
to `false` during the upgrade and plan that downstream migration explicitly; no other behavior is
changed by the opt-out.

### Suggested test method

Run **two connectors against the same topic**, differing only in `value.converter`, writing to two
different tables via `snowflake.topic2table.map`. That gives you a direct side-by-side comparison
on identical input, and it does not disturb your existing pipeline.

---

## Verification

Replace the table names with your own.

**1. Do the legacy paths resolve again?**

```sql
SELECT
  COUNT(*)                                             AS total_rows,
  COUNT(RECORD_CONTENT:action:type::STRING)            AS type_resolves,
  MAX(ARRAY_SIZE(OBJECT_KEYS(RECORD_CONTENT:action)))  AS max_action_keys
FROM your_schema.landing_via_compat_converter;
```

Expect `type_resolves` to equal `total_rows`, and `max_action_keys` to be the width of your widest
active branch — **not** the number of union branches.

**2. Side-by-side against the standard converter**

```sql
SELECT 'compat'  AS variant,
       COUNT(*)                                            AS total_rows,
       MAX(ARRAY_SIZE(OBJECT_KEYS(RECORD_CONTENT:action)))  AS action_keys,
       COUNT(RECORD_CONTENT:action:type::STRING)            AS type_resolves
FROM your_schema.landing_via_compat_converter
UNION ALL
SELECT 'standard',
       COUNT(*),
       MAX(ARRAY_SIZE(OBJECT_KEYS(RECORD_CONTENT:action))),
       COUNT(RECORD_CONTENT:action:type::STRING)
FROM your_schema.landing_via_standard_converter;
```

For reference, our validation run on Kafka Connector 4.1.0 produced:

| variant | rows | action_keys | type_resolves |
|---|---|---|---|
| compat | 12 | 5 | 12 |
| standard | 9 | 46 | 0 |

**3. No values lost**

```sql
-- replace these with your own top-level field names
SELECT COUNT(*) AS rows_missing_a_top_level_field
FROM your_schema.landing_via_compat_converter
WHERE RECORD_CONTENT:eventId IS NULL
   OR RECORD_CONTENT:timestamp IS NULL
   OR RECORD_CONTENT:relatedIds IS NULL
   OR RECORD_CONTENT:attributesByProvider IS NULL;
-- expect 0
```

**4. Run your real downstream SQL.** The decisive test is a representative sample of your existing
streams, tasks, and views executed unchanged against the compat table.

---

## Rollback

Set `value.converter` back to `io.confluent.connect.avro.AvroConverter` and restart the connector.
Nothing else changes; the JAR can be left in place or removed. This converter writes no state and
alters no Snowflake object.

---

## Reporting results

If you evaluate this, the most useful things to report are:

1. Whether assumption **5** (Confluent wire format) held, and whether a fixed `reader.schema` was
   configured.
2. Whether your schemas use **decimal** logical types, and whether precision mattered (assumption 13).
3. Behavior of union branches containing **nested unions**, populated **arrays/maps**, or **temporal** fields.
4. Whether your existing downstream SQL ran unchanged against the compat table.
5. Any plugin load or worker startup problem, especially on Kubernetes/Strimzi or ARM64.

Open an issue with the Avro schema shape (sanitized) and the observed vs expected
`RECORD_CONTENT` for a single record. That is usually enough to reproduce.

---

## Obtaining the JAR

Download from the GitHub Releases page, or resolve via Maven — see the repository README for the
`<repository>` and `<dependency>` declarations. The JAR is ~12 KB with no bundled dependencies.

Built and validated against Kafka Connector **4.1.0**, Kafka Connect **3.9.2**, Confluent
**7.9.2**, Java **11** bytecode on a **21** runtime, ARM64.
