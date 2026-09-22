# Confluent AvroConverter Compatibility

The design goal is that `LegacyCompatibleAvroConverter` is a **drop-in replacement for
`io.confluent.connect.avro.AvroConverter`**: anything Confluent's converter accepts, this one
accepts, because its configuration map is handed to Confluent's `KafkaAvroDeserializer`. Only the
Avro-to-Connect *mapping* step is replaced. The one local extension is `reader.schema`, which is
parsed before the remaining settings are forwarded.

Version 1.1.0 also accepts the legacy connector-specific `reader.schema` property. It is parsed by
this converter and passed to Confluent's public reader-schema deserialization overload, so registry
lookup, rules, migrations, caching, auth, and TLS remain in Confluent's implementation while Avro
performs normal writer-to-reader resolution.

This document states where that holds, where it deliberately does not, and what is out of scope.

The scope boundary in one sentence: **this converter assumes you can already use the Confluent
converter, and are simply not getting the legacy representation from it.** It does not aim to
reproduce every capability of the removed `SnowflakeAvroConverter` family.

## Not supported, by design

| Not supported | Why |
|---|---|
| **Avro without a Schema Registry** | Confluent's converter requires `schema.registry.url` and has no registry-less mode — it raises `ConfigException` without it. Since this converter delegates to the same deserializer, it inherits that requirement. There is nothing to add. |
| **Self-describing Avro** (schema embedded per message, Avro object container files) | This is what the v3 `SnowflakeAvroConverterWithoutSchemaRegistry` read. Confluent's converter cannot read it either. Out of scope. |
| **The `SnowflakeJsonConverter` / `SnowflakeAvroConverter` config surface** | Not a goal. The target is parity with the Confluent converter, not with the removed Snowflake ones. |
| **`schema.reflection=true`** | Produces reflective POJOs rather than `GenericRecord`, which the mapper needs. Untested; assume broken. |
| **`specific.avro.key.type` / `specific.avro.value.type`** | Specific reading is forced off (below), so these have no effect. |
| **Source-side use** | `fromConnectData` delegates to a stock `AvroConverter`. This converter is sink-focused and the outbound path is not independently tested. |
| **Dynamic or subject-selected reader schemas** | `reader.schema` is one fixed schema per configured converter instance. Per-record lookup, subject discovery, and registry-hosted reader selection are out of scope. |

If you need Avro without a Schema Registry, you need a different approach entirely — not this
converter with different settings.

## Deliberately forced — your value is ignored

Two settings are overridden internally. Setting them yourself has no effect.

| Setting | Forced to | Why |
|---|---|---|
| `specific.avro.reader` | `false` | The mapper requires `GenericRecord`. With specific reading enabled, deserialization fails outright. |
| `avro.use.logical.type.converters` | `false` | Legacy representation keeps temporal logical types as raw epoch integers and longs. **This is also a hard requirement, not only a fidelity choice:** with logical type converters enabled, the resulting values are rejected by Kafka Connector 4.1.0 — the record does not load at all. |

Both are pinned by tests that fail if the forcing is removed.

## Inert — accepted but has no effect

`AvroDataConfig` settings configure `AvroData.toConnectData()`, which is precisely the layer this
converter bypasses. They are accepted without error and change nothing:

- `enhanced.avro.schema.support`
- `connect.meta.data`

If you are relying on either to shape your output, this converter is not the right tool.

## Pass-through — works as it does with Confluent's converter

Everything else in the deserializer's configuration surface (62 keys in the base serde config for
Confluent 7.9.2) is forwarded untouched. Grouped:

| Group | Examples |
|---|---|
| Registry endpoint | `schema.registry.url` (including comma-separated failover), `schema.registry.url.randomize` |
| Basic auth | `basic.auth.credentials.source`, `basic.auth.user.info`, `schema.registry.basic.auth.user.info` |
| Bearer / OAuth | `bearer.auth.credentials.source`, `bearer.auth.token`, `bearer.auth.client.id`, `bearer.auth.issuer.endpoint.url`, `bearer.auth.identity.pool.id`, `bearer.auth.scope`, and the rest of the `bearer.auth.*` family |
| TLS | the full `schema.registry.ssl.*` family — truststore, keystore, protocols, cipher suites, engine factory |
| Networking | `http.connect.timeout.ms`, `http.read.timeout.ms`, `proxy.host`, `proxy.port`, `max.retries`, `retries.wait.ms`, `retries.max.wait.ms` |
| Lookup semantics | `use.schema.id`, `use.latest.version`, `use.latest.with.metadata`, `id.compatibility.strict`, `latest.compatibility.strict`, `normalize.schemas`, `latest.cache.size`, `latest.cache.ttl.sec` |
| Subject naming | `value.subject.name.strategy`, `key.subject.name.strategy`, `context.name.strategy` |
| Data contracts | `rule.executors`, `rule.actions`, `rule.service.loader.enable`, `propagate.schema.tags` |

## Fixed reader schema

`reader.schema` is an extension matching the legacy Snowflake converter's property name rather than
a stock `AvroConverter` setting. The value must be a string containing one Avro schema. It is parsed
once during `configure`; invalid text and non-string values raise `ConfigException` before records
are consumed.

For each non-null payload, Confluent still obtains the writer schema from the wire-format schema ID.
The configured schema is supplied as the reader schema, and the returned `GenericRecord` carries
that resolved schema into `LegacyAvroValueMapper`. This supports reader defaults, aliases,
projection, compatible numeric promotion, and named-type resolution as implemented by the tested
Avro dependency. Incompatible schemas raise a topic-specific `DataException` retaining the original
Avro failure in the cause chain. Tombstones bypass resolution.

### A note on subject naming strategies

They are accepted, and they make no difference on the read path. The 4-byte schema ID in the
Confluent wire prefix is authoritative for deserialization; the subject is never consulted. A test
registers a schema under a subject that no strategy would generate and confirms the record still
resolves. So `TopicNameStrategy`, `RecordNameStrategy`, and `TopicRecordNameStrategy` all behave
identically here — as they do with Confluent's converter.

### What is not covered by tests

Auth, TLS, proxying, retries, and data-contract rule executors are pass-through *by construction* —
they are consumed by Confluent's deserializer before this converter's code runs. They are not
covered by the test suite, because exercising them needs a live Schema Registry rather than the
in-memory mock. If one of them misbehaves, the bug is between your configuration and Confluent's
deserializer, and would reproduce with the stock converter too.

## Test coverage

`ConfluentConverterCompatibilityTest` covers:

- Schema resolution by payload ID under a non-default subject
- A declared subject naming strategy being accepted and inert
- The header-aware `toConnectData(topic, headers, value)` overload agreeing with the payload-only overload
- Missing `schema.registry.url` failing with `ConfigException`, matching Confluent
- `specific.avro.reader=true` being overridden
- `avro.use.logical.type.converters=true` being overridden
- `enhanced.avro.schema.support` and `connect.meta.data` being inert
- A representative slice of pass-through settings being accepted without altering output
- Fixed reader-schema defaults, aliases, projection, numeric promotion, configuration validation,
  incompatible schemas, tombstones, both overloads, multiple writer IDs, and concurrency
