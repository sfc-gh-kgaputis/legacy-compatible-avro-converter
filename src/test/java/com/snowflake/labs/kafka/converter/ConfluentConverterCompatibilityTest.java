package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Asserts that {@link LegacyCompatibleAvroConverter} is a drop-in replacement for
 * {@code io.confluent.connect.avro.AvroConverter} across its configuration surface.
 *
 * <p>The design intent is that everything Confluent's converter accepts, this converter also
 * accepts, because the whole configuration map is handed to Confluent's
 * {@code KafkaAvroDeserializer}. Only the Avro-to-Connect <em>mapping</em> step is replaced. These
 * tests pin the three categories where that intent needs stating explicitly:
 *
 * <ol>
 *   <li><b>Pass-through</b> — registry lookup semantics behave as Confluent's does.
 *   <li><b>Deliberately forced</b> — two settings are overridden internally and the caller's value
 *       is ignored, because legacy representation depends on it.
 *   <li><b>Inert</b> — {@code AvroDataConfig} settings have no effect, because the
 *       {@code AvroData} mapping layer they configure is exactly what this converter bypasses.
 * </ol>
 *
 * <p>Settings that need a live Schema Registry — TLS, basic and bearer auth, proxying, retries,
 * data-contract rule executors — are pass-through by construction and are not covered here.
 */
class ConfluentConverterCompatibilityTest {

  private AvroTestSupport support;

  @BeforeEach
  void setUp() {
    support = new AvroTestSupport();
  }

  private static Schema recordWithTimestamp() {
    Schema ts = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    return SchemaBuilder.record("Evt").namespace("com.example.events").fields()
        .name("id").type().stringType().noDefault()
        .name("occurredAt").type(ts).noDefault()
        .endRecord();
  }

  private static GenericRecord instance(Schema schema) {
    GenericRecord r = new GenericData.Record(schema);
    r.put("id", "e-1");
    r.put("occurredAt", 1_700_000_000_000L);
    return r;
  }

  // ---------------------------------------------------------------------------------------------
  // 1. Pass-through: schema resolution matches Confluent's, which is by ID from the payload
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("schema resolves by payload ID, so any subject naming strategy works")
  void resolvesByPayloadSchemaIdRegardlessOfSubject() throws Exception {
    Schema schema = recordWithTimestamp();

    // RecordNameStrategy and TopicRecordNameStrategy produce subjects like these. The read path
    // never consults the subject: the 4-byte ID in the wire prefix is authoritative. Registering
    // under a subject no strategy would generate proves the point.
    byte[] payload = support.serializeUnderSubject("com.example.events.Evt", schema, instance(schema));

    Converter conv = support.compatConverterWith(new HashMap<>());
    JsonNode content = support.recordContent(conv, payload);

    assertEquals("e-1", content.at("/id").textValue(),
        "a non-default subject must not affect deserialization");
    assertEquals(1_700_000_000_000L, content.at("/occurredAt").longValue());
  }

  @Test
  @DisplayName("declaring a subject naming strategy does not change the result")
  void subjectNameStrategyConfigIsAcceptedAndInert() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));

    Map<String, Object> cfg = new HashMap<>();
    cfg.put("value.subject.name.strategy",
        "io.confluent.kafka.serializers.subject.RecordNameStrategy");

    JsonNode content = support.recordContent(support.compatConverterWith(cfg), payload);
    assertEquals("e-1", content.at("/id").textValue());
  }

  @Test
  @DisplayName("the header-aware toConnectData overload behaves identically")
  void headerAwareOverloadMatchesPayloadOnlyOverload() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));
    Converter conv = support.compatConverterWith(new HashMap<>());

    assertEquals(support.recordContent(conv, payload),
        support.recordContentViaHeaders(conv, payload),
        "KC v4 may call the Headers overload; it must agree with the payload-only path");
  }

  @Test
  @DisplayName("missing schema.registry.url fails, exactly as Confluent's converter does")
  void missingRegistryUrlFailsLikeConfluent() {
    LegacyCompatibleAvroConverter conv = new LegacyCompatibleAvroConverter();
    assertThrows(ConfigException.class, () -> conv.configure(new HashMap<>(), false),
        "a Schema Registry is required; there is no registry-less mode, matching Confluent");
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Deliberately forced: the caller's value is ignored
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("specific.avro.reader=true is overridden; output stays a schemaless Map")
  void specificAvroReaderIsForcedOff() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));

    Map<String, Object> cfg = new HashMap<>();
    cfg.put("specific.avro.reader", "true");

    Converter conv = support.compatConverterWith(cfg);
    Object value = conv.toConnectData(AvroTestSupport.TOPIC, payload).value();

    assertInstanceOf(Map.class, value,
        "the mapper needs GenericRecord, so specific reading is forced off regardless of config");
    JsonNode content = support.recordContent(conv, payload);
    assertEquals("e-1", content.at("/id").textValue());
  }

  @Test
  @DisplayName("avro.use.logical.type.converters=true is overridden; timestamps stay raw epoch")
  void logicalTypeConvertersAreForcedOff() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));

    Map<String, Object> cfg = new HashMap<>();
    cfg.put("avro.use.logical.type.converters", "true");

    JsonNode content = support.recordContent(support.compatConverterWith(cfg), payload);

    assertTrue(content.at("/occurredAt").isNumber(),
        "legacy representation is a raw epoch number, not an ISO string; the caller cannot opt out");
    assertEquals(1_700_000_000_000L, content.at("/occurredAt").longValue());
  }

  // ---------------------------------------------------------------------------------------------
  // 3. Inert: AvroDataConfig settings configure a layer this converter does not use
  // ---------------------------------------------------------------------------------------------

  @Test
  @DisplayName("enhanced.avro.schema.support and connect.meta.data have no effect")
  void avroDataConfigSettingsAreInert() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));

    JsonNode plain = support.recordContent(support.compatConverterWith(new HashMap<>()), payload);

    Map<String, Object> cfg = new HashMap<>();
    cfg.put("enhanced.avro.schema.support", "true");
    cfg.put("connect.meta.data", "true");
    JsonNode configured = support.recordContent(support.compatConverterWith(cfg), payload);

    assertEquals(plain, configured,
        "these configure AvroData.toConnectData(), which this converter bypasses entirely");
  }

  @Test
  @DisplayName("unknown and registry-only settings are accepted without error")
  void unrelatedConfluentSettingsAreAccepted() throws Exception {
    Schema schema = recordWithTimestamp();
    byte[] payload = support.serialize(schema, instance(schema));

    // A representative slice of the pass-through surface. None should be rejected, and none
    // should alter the mapped output.
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("normalize.schemas", "true");
    cfg.put("use.latest.version", "false");
    cfg.put("id.compatibility.strict", "false");
    cfg.put("max.retries", "3");
    cfg.put("http.read.timeout.ms", "30000");
    cfg.put("auto.register.schemas", "false");

    JsonNode content = support.recordContent(support.compatConverterWith(cfg), payload);
    assertEquals("e-1", content.at("/id").textValue());
    assertEquals(1_700_000_000_000L, content.at("/occurredAt").longValue());
  }
}
