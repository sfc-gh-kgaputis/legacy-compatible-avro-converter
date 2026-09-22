package com.snowflake.labs.kafka.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;

final class AvroTestSupport {
  static final String TOPIC = "avro-compat";
  static final String REGISTRY_URL = "mock://avro-compat";

  private final MockSchemaRegistryClient registry = new MockSchemaRegistryClient();
  private final AvroConverter confluentConverter = new AvroConverter(registry);
  private final LegacySnowflakeAvroDecoder legacyDecoder =
      new LegacySnowflakeAvroDecoder(registry);
  private final ObjectMapper objectMapper = new ObjectMapper();

  AvroTestSupport() {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", REGISTRY_URL);
    // flatten.singleton.unions=true is the Confluent 7.x default; make it explicit so tests
    // document that two-branch nullable union unwrapping is intentional, not accidental.
    config.put("flatten.singleton.unions", "true");
    confluentConverter.configure(config, false);
  }

  /** Two-path comparison: legacy decoder vs Confluent AvroConverter + KC 4.1.0 RECORD_CONTENT. */
  Comparison compare(Schema schema, Object datum) throws Exception {
    byte[] payload = serialize(schema, datum);
    return new Comparison(legacyDecoder.decode(payload), toKcRecordContent(confluentConverter, payload));
  }

  /**
   * Three-path comparison: legacy, Confluent+KC, and the compatibility converter+KC.
   * The {@code compat} field is {@code null} when {@code LegacyCompatibleAvroConverter} is not yet
   * on the classpath. Tests should guard with {@code Assumptions.assumeTrue(result.compat != null)}.
   */
  ThreePathComparison compareAll(Schema schema, Object datum) throws Exception {
    byte[] payload = serialize(schema, datum);
    Optional<Converter> compatConverter = tryCreateCompatConverter();
    JsonNode compatResult = compatConverter
        .map(c -> toKcRecordContent(c, payload))
        .orElse(null);
    return new ThreePathComparison(
        legacyDecoder.decode(payload),
        toKcRecordContent(confluentConverter, payload),
        compatResult);
  }

  JsonNode decodeLegacyWithReaderSchema(Schema writerSchema, Schema readerSchema, Object datum)
      throws Exception {
    return legacyDecoder.decode(serialize(writerSchema, datum), readerSchema);
  }

  JsonNode recordContentWithReaderSchema(
      Schema writerSchema, Schema readerSchema, Object datum) throws Exception {
    Converter converter = compatConverterWithReaderSchema(readerSchema);
    return recordContent(converter, serialize(writerSchema, datum));
  }

  /** Passes null bytes through the Confluent AvroConverter, simulating a tombstone SinkRecord. */
  SchemaAndValue convertCurrentTombstone() {
    return confluentConverter.toConnectData(TOPIC, null);
  }

  /**
   * Creates a {@link LegacyCompatibleAvroConverter} wired to the shared
   * {@link MockSchemaRegistryClient} so schemas registered via {@link #serialize} are visible.
   *
   * <p>Uses the package-private injection constructor to share the registry directly —
   * no reflection or field replacement required.
   */
  Optional<Converter> tryCreateCompatConverter() {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", REGISTRY_URL);
    LegacyCompatibleAvroConverter conv = new LegacyCompatibleAvroConverter(registry);
    conv.configure(config, false);
    return Optional.of(conv);
  }

  /**
   * Applies an existing {@link Converter} to a pre-serialized payload and returns the KC
   * {@code RECORD_CONTENT} {@link JsonNode}. Used by schema-evolution tests that share a single
   * converter instance across multiple serialized payloads.
   */
  JsonNode recordContent(Converter converter, byte[] payload) {
    return toKcRecordContent(converter, payload);
  }

  /**
   * Serializes under an arbitrary Schema Registry subject. Lets tests assert that the read path
   * resolves by schema ID from the payload, independent of any subject naming strategy.
   */
  byte[] serializeUnderSubject(String subject, Schema schema, Object datum) throws Exception {
    return frame(registry.register(subject, schema), schema, datum);
  }

  /**
   * Builds a compatibility converter with additional or overriding configuration, so tests can
   * assert how it behaves under the full Confluent AvroConverter config surface.
   */
  Converter compatConverterWith(Map<String, Object> extra) {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", REGISTRY_URL);
    config.putAll(extra);
    LegacyCompatibleAvroConverter conv = new LegacyCompatibleAvroConverter(registry);
    conv.configure(config, false);
    return conv;
  }

  Converter compatConverterWithReaderSchema(Schema readerSchema) {
    Map<String, Object> config = new HashMap<>();
    config.put(LegacyCompatibleAvroConverter.READER_SCHEMA_CONFIG, readerSchema.toString());
    return compatConverterWith(config);
  }

  /** Applies a converter's header-aware overload and returns KC RECORD_CONTENT. */
  JsonNode recordContentViaHeaders(Converter converter, byte[] payload) {
    SchemaAndValue converted =
        converter.toConnectData(TOPIC, new org.apache.kafka.common.header.internals.RecordHeaders(), payload);
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, 0, null, null, converted.schema(), converted.value(), 0L);
    SnowflakeSinkRecord snowflakeRecord =
        SnowflakeSinkRecord.from(sinkRecord, new SnowflakeMetadataConfig(), false, false);
    if (!snowflakeRecord.isValid()) {
      throw new AssertionError("KC rejected converted record", snowflakeRecord.getBrokenReason());
    }
    return objectMapper.valueToTree(snowflakeRecord.getContent().get("RECORD_CONTENT"));
  }

  /** Serializes datum to Confluent wire format: magic byte 0x00 + 4-byte schema ID + Avro binary. */
  byte[] serialize(Schema schema, Object datum) throws Exception {
    return frame(registry.register(TOPIC + "-value", schema), schema, datum);
  }

  private byte[] frame(int schemaId, Schema schema, Object datum) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
    new GenericDatumWriter<Object>(schema).write(datum, encoder);
    encoder.flush();

    byte[] avroData = output.toByteArray();
    ByteBuffer framed = ByteBuffer.allocate(5 + avroData.length);
    framed.put((byte) 0);
    framed.putInt(schemaId);
    framed.put(avroData);
    return framed.array();
  }

  private JsonNode toKcRecordContent(Converter converter, byte[] payload) {
    SchemaAndValue converted = converter.toConnectData(TOPIC, payload);
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, 0, null, null, converted.schema(), converted.value(), 0L);
    SnowflakeSinkRecord snowflakeRecord =
        SnowflakeSinkRecord.from(sinkRecord, new SnowflakeMetadataConfig(), false, false);
    if (!snowflakeRecord.isValid()) {
      throw new AssertionError("KC 4.1.0 rejected converted record", snowflakeRecord.getBrokenReason());
    }
    return objectMapper.valueToTree(snowflakeRecord.getContent().get("RECORD_CONTENT"));
  }

  static final class Comparison {
    final JsonNode legacy;
    final JsonNode current;

    Comparison(JsonNode legacy, JsonNode current) {
      this.legacy = legacy;
      this.current = current;
    }
  }

  static final class ThreePathComparison {
    /** Legacy v3: GenericDatumReader + datum.toString() representation. */
    final JsonNode legacy;
    /** KC 4.1.0: Confluent AvroConverter + SnowflakeSinkRecord RECORD_CONTENT. */
    final JsonNode current;
    /**
     * Compatibility converter + SnowflakeSinkRecord RECORD_CONTENT.
     * {@code null} when {@code LegacyCompatibleAvroConverter} is not yet available.
     */
    final JsonNode compat;

    ThreePathComparison(JsonNode legacy, JsonNode current, JsonNode compat) {
      this.legacy = legacy;
      this.current = current;
      this.compat = compat;
    }
  }
}
