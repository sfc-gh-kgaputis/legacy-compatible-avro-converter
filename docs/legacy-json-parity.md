# Legacy Avro JSON Parity

This document compares `LegacyCompatibleAvroConverter` output with the JSON path in the original
Snowflake `SnowflakeAvroConverter` at upstream commit
`03056e8d5dde9159bf53f36cc79f306adaf7fb64`.

The legacy path was:

```text
Confluent wire bytes
  -> GenericDatumReader(writer schema, optional reader schema, GenericData + DecimalConversion)
  -> GenericRecord.toString()
  -> Jackson readTree()
```

`LegacyJsonParityTest` sends the same framed payload through that oracle and the compatibility
converter, then compares the final Kafka Connector 4.1.0 `RECORD_CONTENT` JSON.

## Confirmed parity

| Input category | Result |
|---|---|
| Boolean, int, long, finite float/double, numeric extrema, negative zero | Exact JSON-text parity |
| Float/double `NaN`, positive infinity, negative infinity | Exact parity as JSON strings (`"NaN"`, `"Infinity"`, `"-Infinity"`) |
| Strings, control characters, quotes, slashes, Unicode, escaped map keys | Exact JSON-text parity |
| Plain Avro `bytes` | Exact parity as an ISO-8859-1-decoded JSON string |
| Plain Avro `fixed` and `duration` fixed | Exact parity as an array of signed byte integers |
| Enum and UUID logical type | Exact string parity |
| Populated arrays, maps, nested containers, and records inside containers | Exact JSON-text parity |
| Finite recursive record values | Exact JSON-text parity |
| Nullable, multi-type, named-record, nested, array-contained, and map-contained unions | Active branch is unwrapped as in the legacy output |
| Date, time-millis, time-micros, timestamp-millis, timestamp-micros, local-timestamp-millis, local-timestamp-micros | Exact parity as raw Avro int/long values |
| Decimal `bytes` and decimal `fixed` | Exact final JSON numeric text for ordinary and high-precision tested values |
| Reader defaults, aliases, projection, numeric promotion, and binary defaults | Exact resolved JSON parity |
| Tombstones | Equivalent KC tombstone behavior; the legacy connector used its internal null wrapper instead |

Two binary discrepancies were found and corrected during this pass. The converter previously
returned Java `byte[]` for both plain `bytes` and plain `fixed`. That did not reproduce Avro's
legacy JSON rendering. Version 1.1.0 now maps plain `bytes` to the original ISO-8859-1 string and
plain `fixed` to the original signed-byte integer array.

Non-finite float and double values were also pinned explicitly. Avro's JSON renderer quotes them,
so the compatibility mapper returns their string form rather than exposing a non-JSON numeric value.

### Compatibility with this converter's 1.0.0 release

The corrections above are enabled by default through `legacy.json.parity.enabled=true`. An existing
1.0.0 adopter can set `value.converter.legacy.json.parity.enabled=false` to retain exactly the three
earlier representations:

| Input | Default 1.1.0 | Opt-out / 1.0.0 behavior |
|---|---|---|
| Plain `bytes` | ISO-8859-1 string | `byte[]` |
| Plain `fixed` | Signed-byte integer list | `byte[]` |
| `NaN` and infinities | String | Raw `Float`/`Double` |

The opt-out intentionally does not match the original Snowflake JSON path for those inputs. It does
not alter decimal handling, unions, logical types, reader-schema resolution, tombstones, errors, or
outbound conversion.

## Confirmed differences

### Top-level non-record schemas

The original converter cast the result of `GenericDatumReader.read` to `GenericRecord`. A top-level
string, number, array, map, enum, fixed, or union therefore failed with `ClassCastException` and was
handled as a broken record by the legacy connector.

The compatibility converter accepts those top-level values because Confluent's deserializer returns
them normally and `LegacyAvroValueMapper` can map them. This is an intentional strict superset of the
legacy input surface. It does not alter output for the top-level record payloads the original
converter supported.

### Error transport

The original converter converted malformed wire data, registry failures, and Avro decode failures
into its private `SnowflakeRecordContent` broken-record wrapper unless configured to break on a
registry error. That wrapper no longer exists in Kafka Connector 4.x.

The compatibility converter raises `DataException` with the underlying failure retained as the
cause. Kafka Connect `errors.tolerance`, dead-letter-queue, and retry policy own the outcome.

### Tombstone representation

The original converter returned a private null-record wrapper whose JSON node contained `{}` and
whose separate flag identified the tombstone. The compatibility converter returns
`SchemaAndValue.NULL`, which Kafka Connector 4.x recognizes as a tombstone before constructing
`RECORD_CONTENT`. The record-level outcome is equivalent, but the internal representation differs.

## Decimal precision finding

The legacy reader does produce `BigDecimal` for decimal logical types. However, the legacy
`GenericRecord.toString()` output is parsed by its Jackson version into a JSON number that uses
double-like precision for the tested 30-digit value. The compatibility converter's explicit
`BigDecimal`-to-`double` mapping therefore matches the final legacy JSON text in the tested cases,
including the same precision loss.

This does not make the value exact. Applications requiring more than roughly 15 significant digits
must not treat the historical `RECORD_CONTENT` number as a lossless decimal representation.

## Coverage boundary

This pass is an executable corpus, not a proof over every possible Avro schema. Remaining limits:

- Schematization remains out of scope; comparisons use `snowflake.enable.schematization=false`.
- Reflective and specific-record readers are forced off and are not part of the contract.
- Corrupt binary encodings and Schema Registry operational failures are covered as error paths, not
  exhaustively fuzzed byte by byte.
- Truly cyclic in-memory object graphs cannot be serialized as normal finite Avro payloads; finite
  recursive schemas and values are covered.
- Custom logical types without a registered conversion are treated according to their underlying
  Avro type and should be added as explicit fixtures if relied upon.

## Executable evidence

- `LegacyJsonParityTest` covers the wide-range final-output corpus and intentional differences.
- `AvroCompatibilityTest` covers legacy-versus-stock-Confluent differences and three-path union and
  logical-type behavior.
- `NamedRecordUnionCompatibilityTest` covers the wide named-record union that motivated the project.
- `ReaderSchemaCompatibilityTest` covers fixed reader-schema resolution and failure semantics.
- `LegacyAvroValueMapperTest` pins mapper-level representations and edge cases.
