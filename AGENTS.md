# Legacy Compatible Avro Converter

Java 11 Maven project supporting a Kafka Connector v3→v4 migration. Provides a production
compatibility converter and a behavior-comparison test harness.

See `README.md` for build/test commands and `docs/deployment-guide.md` for deployment.

## Key Structure

**Production code** (compiled without any Snowflake KC dependency):
- `src/main/.../LegacyCompatibleAvroConverter.java` — public Kafka Connect `Converter`
  delegating to Confluent `KafkaAvroDeserializer` for framing/registry, then mapping
  via `LegacyAvroValueMapper`.
- `src/main/.../LegacyAvroValueMapper.java` — schema-aware Avro-to-legacy-map mapper.

**Test and oracle code** (may use KC internal classes for comparison):
- `src/main/.../LegacySnowflakeAvroDecoder.java` — v3 behavioral oracle.
- `src/test/.../AvroTestSupport.java` — mock registry, three-path comparison harness.
- `src/test/.../AvroCompatibilityTest.java` — executable compatibility contract.

**Build configuration**:
- `pom.xml` — `connect-api` and `kafka-connect-avro-converter` are `provided`; KC is
  `test`-only. Version matrix in profiles; defaults in `<properties>`.
- `src/assembly/plugin-zip.xml` — thin converter ZIP for Strimzi deployment.

**Documentation**:
- `docs/legacy_avro_converter_analysis.md` — behavior analysis and migration impact.
- `docs/deployment-guide.md` — KC v4 configuration, Strimzi ARM64, version matrix,
  supportability notice.

## Workflow

- Test: `mvn test`
- Full verify + package: `mvn verify`
- Thin converter JAR: `mvn package -DskipTests`
- Deployment ZIP: `mvn package -P plugin-zip`
- Dependency tree: `mvn dependency:tree -Dverbose`

## Rules

- **Production independence**: production code under `src/main/` must compile with only
  `provided`-scope deps (`connect-api`, `kafka-connect-avro-converter`). Any import from
  `com.snowflake.kafka.connector.*` belongs in `src/test/` only.
- Keep the project standalone: tests must not require a running Kafka broker, Schema
  Registry, or Snowflake account.
- Base behavior claims on passing comparison tests. Add or extend a test before
  documenting a new difference.
- Preserve the distinction between simple nullable unions (`['null','string']`, which are
  unwrapped identically) and genuine multi-type unions (which gain a branch wrapper under
  the standard KC path).
- `LegacySnowflakeAvroDecoder` is the v3 behavioral oracle, not a supported production
  converter.
- Do not edit or commit `target/`.
- Never commit credentials, Schema Registry secrets, KC connection properties, or
  unsanitized customer payloads. Private fixtures go in `fixtures/private/` (git-ignored).

## Compatibility contract

The target is parity with `io.confluent.connect.avro.AvroConverter`, not with the removed
`SnowflakeAvroConverter` family. Anything Confluent accepts, this should accept, because its config
map is forwarded to `KafkaAvroDeserializer`; only the Avro-to-Connect mapping is replaced. The one
converter extension is `reader.schema`, which is parsed locally and supplied through Confluent's
public reader-schema overload.

A Schema Registry is required and registry-less Avro is out of scope — Confluent's converter has no
such mode either. Two settings are forced (`specific.avro.reader`,
`avro.use.logical.type.converters`) and two are inert (`enhanced.avro.schema.support`,
`connect.meta.data`). Before changing either forcing, note that enabling logical type converters
yields values KC 4.1.0 rejects. See `docs/confluent-compatibility.md`, and keep it in step with
`ConfluentConverterCompatibilityTest`.
