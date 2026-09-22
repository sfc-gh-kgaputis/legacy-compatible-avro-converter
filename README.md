# Legacy Compatible Avro Converter

> **Not an official Snowflake offering.** This is a community project, provided with no support
> or warranty, and is not affiliated with or endorsed by Snowflake as a product. A custom
> `value.converter` is not a supported Kafka Connector 4.x configuration path — see
> [docs/testing-guide.md](docs/testing-guide.md) before considering production use.

A Kafka Connect converter that preserves the legacy `RECORD_CONTENT` JSON contract when migrating
from Snowflake Kafka Connector **v3 + classic Snowpipe** to **v4 + Snowpipe Streaming**, so that
downstream SQL reading `RECORD_CONTENT` paths does not have to change.

## The problem it solves

The deprecated `SnowflakeAvroConverter` collapsed an Avro union to the active branch's fields,
inlined at the union's path. Confluent's `AvroConverter` — the supported choice for v4 — instead
emits one key per union branch name, with inactive branches `null` and the active branch nested
one level deeper. Every existing `RECORD_CONTENT:<field>:<subfield>` path across that union
therefore returns `NULL` after migration.

This converter restores the legacy representation at ingest, so no view sits in the query path
and no downstream object changes.

## Install

The JAR must be placed **directly alongside** `snowflake-kafka-connector-*.jar` in the same
plugin directory. It is intentionally thin and resolves its Confluent Avro and Apache Avro classes
from the Snowflake connector's own fat JAR, which bundles them unrelocated. A separate
`plugin.path` entry gets an isolated classloader and will fail with `NoClassDefFoundError`.

```
/opt/kafka/plugins/snowflake-kafka-connector/
    snowflake-kafka-connector-4.1.0.jar
    legacy-compatible-avro-converter-1.1.0.jar   <-- here
```

```properties
value.converter=com.snowflake.labs.kafka.converter.LegacyCompatibleAvroConverter
value.converter.schema.registry.url=https://your-schema-registry:8081
snowflake.enable.schematization=false
```

To apply one fixed Avro reader schema to every value handled by the converter, add its JSON schema
as a string-valued converter property:

```properties
value.converter.reader.schema={"type":"record","name":"Event","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"source","type":"string","default":"legacy"}]}
```

The schema is parsed once when the converter is configured. Invalid schema text and non-string
values fail startup with `ConfigException`. Compatible writer schemas are resolved through Avro's
normal writer-to-reader rules, including defaults, aliases, field projection, and numeric promotion.
An incompatible payload fails as `DataException`; the converter never falls back to writer-only
decoding. The setting is fixed per converter instance, not selected per record or Schema Registry
subject.

### Scope

This is a **drop-in replacement for `io.confluent.connect.avro.AvroConverter`**. It assumes you can
already use the Confluent converter and are simply not getting the legacy representation from it.
It does not reproduce the removed `SnowflakeAvroConverter` family — notably, **Avro without a
Schema Registry is not supported**, because Confluent's converter has no registry-less mode either.

Two settings are deliberately overridden and two are inert. See
[docs/confluent-compatibility.md](docs/confluent-compatibility.md) for the full support matrix.

Read [docs/testing-guide.md](docs/testing-guide.md) before deploying — it lists 16 assumptions that
decide whether this converter applies to your setup at all.

## Maven

Releases are published to a static Maven repository inside this repo.

```xml
<repositories>
  <repository>
    <id>legacy-compatible-avro-converter</id>
    <url>https://raw.githubusercontent.com/sfc-gh-kgaputis/legacy-compatible-avro-converter/main/maven</url>
  </repository>
</repositories>

<dependency>
  <groupId>com.snowflake.labs</groupId>
  <artifactId>legacy-compatible-avro-converter</artifactId>
  <version>1.1.0</version>
</dependency>
```

The published artifact has **no transitive dependencies** — every build dependency is `provided`
or `test` scope. JARs are also attached to each [GitHub Release](../../releases).

---

## What is in this repo

Java 11 Maven project providing:

1. **A production compatibility converter** (`LegacyCompatibleAvroConverter`) that
   preserves the legacy `RECORD_CONTENT` JSON contract when migrating from KC v3/classic
   Snowpipe to KC v4/SSv2 with `snowflake.enable.schematization=false`.

2. **An investigation and test harness** that reproduces the legacy decoding behavior,
   runs the same Avro payloads through three comparison paths, and asserts exact JSON
   shapes for known compatibility differences.

The project uses a mock Schema Registry and requires no running Kafka broker or Snowflake
account.

---

## Build and test

```
mvn test
```

The first build downloads the Snowflake KC 4.1.0 shaded artifact (~171 MB).

```
mvn verify
```

Runs tests and checks packaging.

```
mvn package -DskipTests
```

Produces the thin converter JAR in `target/`.

```
mvn package -P plugin-zip
```

Produces a deployment ZIP (`target/legacy-compatible-avro-converter-1.1.0.zip`)
for drop-in placement in an existing KC plugin directory.

```
mvn dependency:tree -Dverbose
```

Inspect the resolved dependency tree and check for version conflicts.

---

## Tested version matrix

| Component | Version |
|---|---|
| Snowflake Kafka Connector | 4.1.0 (test-scope oracle) |
| Confluent Platform | 7.9.2 |
| Apache Kafka | 3.9.2 |
| Java | 11 |

Override versions at build time:

```
mvn test -Dkc.version=4.1.0 -Dconfluent.version=7.9.10
```

The release verification matrix includes the 4.1.0 / 7.9.2 baseline and a Confluent 7.9.10
override. Kafka Connector 4.2.0 and Confluent 7.10.0 are not published under those artifact
coordinates and are not claimed as tested.

---

## What the tests cover

- Simple nullable unions `['null','string']` — produce the same unwrapped output in both paths.
- Multi-type unions `['null','string','int']` — the key confirmed incompatibility:
  the legacy path emits `"value"` while the standard Confluent + KC 4.1 path emits
  `{"string":"value"}`, changing the Snowflake path from `RECORD_CONTENT:value` to
  `RECORD_CONTENT:value:string`.
- Nested records with and without embedded unions.
- Reader-schema defaults (fields absent from the writer payload).
- Decimal logical type (JSON number in both paths).
- Date and timestamp-millis logical types (integer epoch in the legacy path; ISO string
  in the standard KC 4.1 path).
- Explicit null preservation inside nullable unions.
- Tombstone records (null payloads).
- Concurrency: a single converter instance is safe under multi-threaded Kafka Connect
  worker usage.
- Fixed `reader.schema` resolution: defaults, aliases, projection, numeric promotion, incompatible
  schema failures, multiple writer IDs, both converter overloads, and concurrent use.

---

## Release notes

### 1.1.0

- Adds the legacy fixed `reader.schema` option with Avro writer-to-reader resolution delegated to
  Confluent's public deserializer API.
- Validates the configured reader schema during converter startup and reports incompatible records
  as topic-specific `DataException`s with the Avro failure retained as the cause.
- Reproduces legacy JSON rendering for plain `bytes`, plain `fixed`, and non-finite floating-point
  values, backed by a wide differential corpus.
- Keeps tombstones, outbound serialization, legacy union collapse, and logical-type representation
  unchanged.

### 1.0.0

- Initial public release preserving the legacy schema-less `RECORD_CONTENT` representation.

---

## Compatibility converter design

`LegacyCompatibleAvroConverter` implements the public Kafka Connect `Converter` interface.
It delegates Confluent-framed deserialization, Schema Registry lookup, TLS, caching, and
retry to Confluent's `KafkaAvroDeserializer` (the same operational machinery used by
`AvroConverter`). It then maps the Avro datum to a standard `Map<String, Object>` via
`LegacyAvroValueMapper` instead of routing through Kafka Connect's intermediate type
system (`Struct`/`SchemaAndValue`). This preserves:

- raw epoch integers for date, time, and timestamp fields;
- `BigDecimal` for decimal fields;
- direct union-branch unwrapping for multi-type unions;
- all ordinary record nesting and explicit nulls.

See [`docs/legacy_avro_converter_analysis.md`](docs/legacy_avro_converter_analysis.md)
for the migration analysis,
[`docs/legacy-json-parity.md`](docs/legacy-json-parity.md) for the wide-range executable output
comparison, and
[`docs/deployment-guide.md`](docs/deployment-guide.md) for deployment instructions.

---

## Source layout

```
src/main/java/com.snowflake.labs.kafka.converter/
  LegacyCompatibleAvroConverter.java   — production Converter implementation
  LegacyAvroValueMapper.java           — schema-aware Avro-to-legacy-map mapper
  LegacySnowflakeAvroDecoder.java      — v3 behavioral oracle (reference only)

src/test/java/com.snowflake.labs.kafka.converter/
  AvroTestSupport.java                 — mock registry, three-path comparison harness
  AvroCompatibilityTest.java           — executable compatibility contract
  NamedRecordUnionCompatibilityTest.java — union-of-named-records contract
  ConfluentConverterCompatibilityTest.java — Confluent AvroConverter config-surface parity
  LegacyJsonParityTest.java               — wide legacy JSON differential corpus
  LegacyAvroValueMapperTest.java       — unit tests for the value mapper

src/assembly/plugin-zip.xml           — assembly descriptor for deployment ZIP

docs/
  confluent-compatibility.md          — support matrix vs Confluent AvroConverter
  legacy_avro_converter_analysis.md   — behavior analysis and migration impact
  legacy-json-parity.md               — confirmed parity, differences, and coverage limits
  deployment-guide.md                 — production deployment, Strimzi ARM64, version matrix
```

---

> `LegacyCompatibleAvroConverter` is a custom converter, which is not a supported Kafka
> Connector 4.x configuration path. Read the supportability notice in
> [docs/deployment-guide.md](docs/deployment-guide.md) and obtain written approval from
> Snowflake Support or the responsible product team before production use.
