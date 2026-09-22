package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ReaderSchemaCompatibilityTest {

  private AvroTestSupport support;

  @BeforeEach
  void setUp() {
    support = new AvroTestSupport();
  }

  @Test
  @DisplayName("absence of reader.schema preserves writer-schema output exactly")
  void absentReaderSchemaPreservesCurrentBehavior() throws Exception {
    Schema schema = SchemaBuilder.record("Event").namespace("com.example").fields()
        .requiredString("id")
        .name("choice").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
        .endRecord();
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("id", "e-1");
    datum.put("choice", "active");
    byte[] payload = support.serialize(schema, datum);

    Converter defaultConverter = support.tryCreateCompatConverter().orElseThrow();
    Converter explicitConfig = support.compatConverterWith(new HashMap<>());

    assertEquals(
        support.recordContent(defaultConverter, payload),
        support.recordContent(explicitConfig, payload));
  }

  @Test
  @DisplayName("reader-only field default is materialized in RECORD_CONTENT")
  void readerDefaultIsMaterialized() throws Exception {
    Schema writer = record("Account", field("id", Schema.create(Schema.Type.STRING)));
    Schema reader = record("Account",
        field("id", Schema.create(Schema.Type.STRING)),
        fieldWithDefault("status", Schema.create(Schema.Type.STRING), "new"));
    GenericRecord datum = recordValue(writer, "id", "a-1");

    JsonNode legacy = support.decodeLegacyWithReaderSchema(writer, reader, datum);
    JsonNode content = support.recordContentWithReaderSchema(writer, reader, datum);

    assertEquals(legacy, content);
    assertEquals("new", content.at("/status").textValue());
  }

  @Test
  @DisplayName("reader field alias renames writer field")
  void readerAliasResolvesWriterField() throws Exception {
    Schema writer = record("Person", field("oldName", Schema.create(Schema.Type.STRING)));
    Schema.Field renamed = field("newName", Schema.create(Schema.Type.STRING));
    renamed.addAlias("oldName");
    Schema reader = record("Person", renamed);

    JsonNode content = support.recordContentWithReaderSchema(
        writer, reader, recordValue(writer, "oldName", "Ada"));

    assertEquals("Ada", content.at("/newName").textValue());
    assertTrue(content.at("/oldName").isMissingNode());
  }

  @Test
  @DisplayName("reader projection omits writer-only fields")
  void readerProjectionOmitsWriterOnlyField() throws Exception {
    Schema writer = record("Projection",
        field("keep", Schema.create(Schema.Type.STRING)),
        field("drop", Schema.create(Schema.Type.STRING)));
    Schema reader = record("Projection", field("keep", Schema.create(Schema.Type.STRING)));
    GenericRecord datum = recordValue(writer, "keep", "yes", "drop", "no");

    JsonNode content = support.recordContentWithReaderSchema(writer, reader, datum);

    assertEquals(1, content.size());
    assertEquals("yes", content.at("/keep").textValue());
    assertTrue(content.at("/drop").isMissingNode());
  }

  @Test
  @DisplayName("compatible numeric promotion uses reader type")
  void compatibleNumericPromotionResolves() throws Exception {
    Schema writer = record("Metric", field("count", Schema.create(Schema.Type.INT)));
    Schema reader = record("Metric", field("count", Schema.create(Schema.Type.LONG)));

    SchemaAndValue converted = support.compatConverterWithReaderSchema(reader).toConnectData(
        AvroTestSupport.TOPIC,
        support.serialize(writer, recordValue(writer, "count", 42)));
    Map<?, ?> value = assertInstanceOf(Map.class, converted.value());

    assertInstanceOf(Long.class, value.get("count"));
    assertEquals(42L, value.get("count"));
  }

  @Test
  @DisplayName("missing required reader field fails with the Avro cause retained")
  void missingRequiredReaderFieldFails() throws Exception {
    Schema writer = record("Required", field("id", Schema.create(Schema.Type.STRING)));
    Schema reader = record("Required",
        field("id", Schema.create(Schema.Type.STRING)),
        field("required", Schema.create(Schema.Type.STRING)));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    DataException error = assertThrows(DataException.class, () -> converter.toConnectData(
        "required-topic", support.serialize(writer, recordValue(writer, "id", "r-1"))));

    assertTrue(error.getMessage().contains("configured reader schema"));
    assertTrue(error.getMessage().contains("required-topic"));
    assertAvroCause(error);
  }

  @Test
  @DisplayName("incompatible field type fails without falling back to writer schema")
  void incompatibleFieldTypeFails() throws Exception {
    Schema writer = record("Incompatible", field("value", Schema.create(Schema.Type.STRING)));
    Schema reader = record("Incompatible", field("value", Schema.create(Schema.Type.INT)));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    DataException error = assertThrows(DataException.class, () -> converter.toConnectData(
        "incompatible-topic",
        support.serialize(writer, recordValue(writer, "value", "not-an-int"))));

    assertTrue(error.getMessage().contains("incompatible-topic"));
    assertAvroCause(error);
  }

  @Test
  @DisplayName("invalid reader schema text fails during configure")
  void invalidSchemaTextFailsDuringConfigure() {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", AvroTestSupport.REGISTRY_URL);
    config.put(LegacyCompatibleAvroConverter.READER_SCHEMA_CONFIG, "{not-json");

    ConfigException error = assertThrows(ConfigException.class,
        () -> new LegacyCompatibleAvroConverter().configure(config, false));

    assertTrue(error.getMessage().contains("reader.schema"));
    assertTrue(error.getMessage().contains("valid Avro schema"));
  }

  @Test
  @DisplayName("non-string reader schema fails during configure")
  void nonStringSchemaFailsDuringConfigure() {
    Map<String, Object> config = new HashMap<>();
    config.put("schema.registry.url", AvroTestSupport.REGISTRY_URL);
    config.put(LegacyCompatibleAvroConverter.READER_SCHEMA_CONFIG, Map.of("type", "string"));

    ConfigException error = assertThrows(ConfigException.class,
        () -> new LegacyCompatibleAvroConverter().configure(config, false));

    assertTrue(error.getMessage().contains("reader.schema"));
    assertTrue(error.getMessage().contains("must be a string"));
  }

  @Test
  @DisplayName("tombstones bypass reader schema resolution")
  void tombstoneRemainsNull() {
    Schema reader = record("Tombstone", field("id", Schema.create(Schema.Type.STRING)));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    assertSame(SchemaAndValue.NULL, converter.toConnectData(AvroTestSupport.TOPIC, null));
    assertSame(
        SchemaAndValue.NULL,
        converter.toConnectData(AvroTestSupport.TOPIC, new RecordHeaders(), null));
  }

  @Test
  @DisplayName("headers overload returns the same reader-resolved result")
  void headersOverloadMatchesPayloadOnlyOverload() throws Exception {
    Schema writer = record("HeaderEvent", field("id", Schema.create(Schema.Type.STRING)));
    Schema reader = record("HeaderEvent",
        field("id", Schema.create(Schema.Type.STRING)),
        fieldWithDefault("source", Schema.create(Schema.Type.STRING), "reader"));
    byte[] payload = support.serialize(writer, recordValue(writer, "id", "h-1"));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    assertEquals(
        support.recordContent(converter, payload),
        support.recordContentViaHeaders(converter, payload));
  }

  @Test
  @DisplayName("one converter resolves multiple compatible writer schema IDs")
  void oneConverterHandlesMultipleWriterSchemas() throws Exception {
    Schema writerV1 = record("MultiWriter", field("id", Schema.create(Schema.Type.STRING)));
    Schema writerV2 = record("MultiWriter",
        field("id", Schema.create(Schema.Type.STRING)),
        field("writerOnly", Schema.create(Schema.Type.INT)));
    Schema reader = record("MultiWriter",
        field("id", Schema.create(Schema.Type.STRING)),
        fieldWithDefault("readerOnly", Schema.create(Schema.Type.STRING), "defaulted"));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    JsonNode first = support.recordContent(converter,
        support.serialize(writerV1, recordValue(writerV1, "id", "v1")));
    JsonNode second = support.recordContent(converter,
        support.serialize(writerV2, recordValue(writerV2, "id", "v2", "writerOnly", 7)));

    assertEquals("defaulted", first.at("/readerOnly").textValue());
    assertEquals("defaulted", second.at("/readerOnly").textValue());
    assertTrue(second.at("/writerOnly").isMissingNode());
  }

  @Test
  @DisplayName("fixed reader schema conversion is thread-safe and deterministic")
  void concurrentConversionIsDeterministic() throws Exception {
    Schema writer = record("Concurrent", field("id", Schema.create(Schema.Type.STRING)));
    Schema reader = record("Concurrent",
        field("id", Schema.create(Schema.Type.STRING)),
        fieldWithDefault("state", Schema.create(Schema.Type.STRING), "ready"));
    byte[] payload = support.serialize(writer, recordValue(writer, "id", "c-1"));
    Converter converter = support.compatConverterWithReaderSchema(reader);
    ExecutorService executor = Executors.newFixedThreadPool(8);
    List<Future<SchemaAndValue>> futures = new ArrayList<>();

    try {
      for (int i = 0; i < 200; i++) {
        futures.add(executor.submit(
            () -> converter.toConnectData(AvroTestSupport.TOPIC, payload)));
      }
      Object expected = futures.get(0).get(30, TimeUnit.SECONDS).value();
      for (Future<SchemaAndValue> future : futures) {
        assertEquals(expected, future.get(30, TimeUnit.SECONDS).value());
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  @DisplayName("named-record union collapse still matches the legacy oracle after resolution")
  void namedRecordUnionCollapseSurvivesReaderResolution() throws Exception {
    Schema createdWriter = SchemaBuilder.record("Created").namespace("com.example.action").fields()
        .requiredString("type")
        .requiredString("id")
        .endRecord();
    Schema ignoredWriter = SchemaBuilder.record("Ignored").namespace("com.example.action").fields()
        .requiredString("type")
        .endRecord();
    Schema writer = record("UnionEvent",
        field("action", Schema.createUnion(createdWriter, ignoredWriter)));

    Schema createdReader = SchemaBuilder.record("Created").namespace("com.example.action").fields()
        .requiredString("type")
        .requiredString("id")
        .name("source").type().stringType().stringDefault("reader")
        .endRecord();
    Schema ignoredReader = SchemaBuilder.record("Ignored").namespace("com.example.action").fields()
        .requiredString("type")
        .endRecord();
    Schema reader = record("UnionEvent",
        field("action", Schema.createUnion(createdReader, ignoredReader)));

    GenericRecord action = recordValue(createdWriter, "type", "Created", "id", "u-1");
    GenericRecord datum = recordValue(writer, "action", action);

    JsonNode legacy = support.decodeLegacyWithReaderSchema(writer, reader, datum);
    JsonNode content = support.recordContentWithReaderSchema(writer, reader, datum);

    assertEquals(legacy, content);
    assertEquals("Created", content.at("/action/type").textValue());
    assertEquals("reader", content.at("/action/source").textValue());
    assertTrue(content.at("/action/Created").isMissingNode());
  }

  @Test
  @DisplayName("logical types retain legacy representation after reader resolution")
  void logicalTypesRemainLegacyCompatible() throws Exception {
    Schema writerTimestamp = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema readerTimestamp = LogicalTypes.timestampMillis()
        .addToSchema(Schema.create(Schema.Type.LONG));
    Schema writer = record("Logical", field("occurredAt", writerTimestamp));
    Schema reader = record("Logical",
        field("occurredAt", readerTimestamp),
        fieldWithDefault("sequence", Schema.create(Schema.Type.LONG), 0L));
    GenericRecord datum = recordValue(writer, "occurredAt", 1_700_000_000_000L);

    JsonNode legacy = support.decodeLegacyWithReaderSchema(writer, reader, datum);
    JsonNode content = support.recordContentWithReaderSchema(writer, reader, datum);

    assertEquals(legacy.toString(), content.toString());
    assertTrue(content.at("/occurredAt").isNumber());
    assertEquals(1_700_000_000_000L, content.at("/occurredAt").longValue());
  }

  @Test
  @DisplayName("reader.schema does not change outbound serialization")
  void readerSchemaDoesNotChangeOutboundSerialization() {
    Schema reader = record("Outbound", field("id", Schema.create(Schema.Type.STRING)));
    Converter converter = support.compatConverterWithReaderSchema(reader);

    assertNull(converter.fromConnectData(AvroTestSupport.TOPIC, null, null));
  }

  private static Schema record(String name, Schema.Field... fields) {
    Schema schema = Schema.createRecord(name, null, "com.example", false);
    schema.setFields(List.of(fields));
    return schema;
  }

  private static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, (Object) null);
  }

  private static Schema.Field fieldWithDefault(String name, Schema schema, Object defaultValue) {
    return new Schema.Field(name, schema, null, defaultValue);
  }

  private static GenericRecord recordValue(Schema schema, Object... fields) {
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < fields.length; i += 2) {
      record.put((String) fields[i], fields[i + 1]);
    }
    return record;
  }

  private static void assertAvroCause(Throwable error) {
    Throwable current = error;
    while (current != null && !(current instanceof AvroTypeException)) {
      current = current.getCause();
    }
    assertInstanceOf(AvroTypeException.class, current);
  }
}
