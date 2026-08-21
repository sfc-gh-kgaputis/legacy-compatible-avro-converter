package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.Test;

class AvroCompatibilityTest {
  private final AvroTestSupport support = new AvroTestSupport();

  // ============================================================
  // Existing two-path tests (fixed / enhanced)
  // ============================================================

  @Test
  void simpleNullableUnionIsUnwrappedByBothConverters() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"UnionRecord\",\"fields\":["
                    + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("status", "active");

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals("active", result.legacy.at("/status").textValue());
    assertEquals("active", result.current.at("/status").textValue());
  }

  @Test
  void multiTypeUnionStringBranchAddsBranchWrapperAndBreaksLegacyPath() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"MultiUnionRecord\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("value", "person@example.com");

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals("person@example.com", result.legacy.at("/value").textValue());
    assertEquals(
        "person@example.com",
        result.current.at("/value/string").textValue(),
        result.current.toPrettyString());
    assertNull(result.current.at("/value").textValue());
  }

  @Test
  void multiTypeUnionIntBranchAddsBranchWrapper() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"MultiUnionRecord\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("value", 42);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // legacy: raw int, no branch wrapper
    assertEquals(42, result.legacy.at("/value").intValue());
    // current: int branch wrapped with type key
    assertEquals(
        42,
        result.current.at("/value/int").intValue(),
        "int branch should be wrapped: " + result.current.toPrettyString());
    assertTrue(
        result.current.at("/value").isObject(),
        "current path /value must be an object wrapping the int branch");
  }

  @Test
  void ordinaryNestedRecordPlacementIsPreserved() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Envelope\",\"fields\":[{\"name\":\"customer\","
                    + "\"type\":{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"email\","
                    + "\"type\":[\"null\",\"string\"],\"default\":null}]}}]}");
    Schema customerSchema = schema.getField("customer").schema();
    GenericRecord customer = new GenericData.Record(customerSchema);
    customer.put("email", "person@example.com");
    GenericRecord envelope = new GenericData.Record(schema);
    envelope.put("customer", customer);

    AvroTestSupport.Comparison result = support.compare(schema, envelope);

    assertEquals("person@example.com", result.legacy.at("/customer/email").textValue());
    assertEquals("person@example.com", result.current.at("/customer/email").textValue());
  }

  @Test
  void explicitUnionNullRemainsNullInBothRepresentations() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"NullRecord\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("value", null);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertTrue(result.legacy.get("value").isNull(), "legacy path: explicit null must be JSON null");
    assertTrue(result.current.get("value").isNull(), "current path: explicit null must be JSON null");
  }

  @Test
  void readerSchemaAddsDefaultsOnlyInLegacyDecoder() throws Exception {
    Schema writerSchema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Versioned\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"}]}");
    Schema readerSchema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Versioned\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"region\",\"type\":\"string\",\"default\":\"unknown\"}]}");
    GenericRecord record = new GenericData.Record(writerSchema);
    record.put("id", 7);

    AvroTestSupport.Comparison normal = support.compare(writerSchema, record);

    assertEquals(
        "unknown",
        support.decodeLegacyWithReaderSchema(writerSchema, readerSchema, record).get("region").textValue());
    assertFalse(normal.current.has("region"), "current path must not inject reader schema defaults");
    assertFalse(normal.legacy.has("region"), "legacy path without reader schema must not have reader-only field");
  }

  @Test
  void decimalLogicalTypeUsesDifferentJavaPathsButSameJsonNumber() throws Exception {
    Schema decimalSchema =
        LogicalTypes.decimal(20, 4).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema =
        SchemaBuilder.record("DecimalRecord")
            .fields()
            .name("amount")
            .type(decimalSchema)
            .noDefault()
            .endRecord();
    BigDecimal amount = new BigDecimal("90.0000");
    GenericRecord record = new GenericData.Record(schema);
    record.put("amount", ByteBuffer.wrap(amount.unscaledValue().toByteArray()));

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals(0, new BigDecimal(result.legacy.get("amount").asText()).compareTo(amount));
    assertEquals(0, new BigDecimal(result.current.get("amount").asText()).compareTo(amount));
  }

  @Test
  void dateAndTimestampMillisLogicalTypesHaveDifferentJsonRepresentations() throws Exception {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timestampSchema =
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema =
        SchemaBuilder.record("TemporalRecord")
            .fields()
            .name("eventDate")
            .type(dateSchema)
            .noDefault()
            .name("eventTime")
            .type(timestampSchema)
            .noDefault()
            .endRecord();
    int days = (int) LocalDate.of(2024, 1, 15).toEpochDay();
    long millis =
        LocalDate.of(2024, 1, 15).atTime(10, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    GenericRecord record = new GenericData.Record(schema);
    record.put("eventDate", days);
    record.put("eventTime", millis);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals(days, result.legacy.get("eventDate").intValue());
    assertEquals(millis, result.legacy.get("eventTime").longValue());
    assertEquals("2024-01-15T00:00:00.000Z", result.current.get("eventDate").textValue());
    assertEquals("2024-01-15T10:00:00.000Z", result.current.get("eventTime").textValue());
  }

  // ============================================================
  // New two-path coverage tests
  // ============================================================

  @Test
  void nestedMultiTypeUnionAddsBranchWrapperAtNestedLevel() throws Exception {
    // A multi-type union inside a nested record gets the branch wrapper at the nested location,
    // not at the outer record level. Customers seeing path changes in nested schemas are often
    // hitting a union inside a nested record rather than a top-level nesting change.
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
                    + "{\"name\":\"inner\",\"type\":{"
                    + "\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
                    + "{\"name\":\"tag\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}"
                    + "]}}]}");
    Schema innerSchema = schema.getField("inner").schema();
    GenericRecord inner = new GenericData.Record(innerSchema);
    inner.put("tag", "hello");
    GenericRecord outer = new GenericData.Record(schema);
    outer.put("inner", inner);

    AvroTestSupport.Comparison result = support.compare(schema, outer);

    assertEquals("hello", result.legacy.at("/inner/tag").textValue());
    assertEquals(
        "hello",
        result.current.at("/inner/tag/string").textValue(),
        "branch wrapper should be at /inner/tag/string, not /inner/string/tag: "
            + result.current.toPrettyString());
  }

  @Test
  void arrayOfNullableUnionItemsIsUnwrappedInBothPaths() throws Exception {
    // With flatten.singleton.unions=true, array items with two-branch nullable unions
    // are unwrapped in the current path — no branch wrappers, same as legacy.
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"TagRecord\",\"fields\":["
                    + "{\"name\":\"tags\",\"type\":{\"type\":\"array\","
                    + "\"items\":[\"null\",\"string\"]}}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("tags", Arrays.asList("hello", null, "world"));

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // Both paths: nullable union array items are flat, no branch wrapper
    assertEquals("hello", result.legacy.at("/tags/0").textValue());
    assertTrue(result.legacy.at("/tags/1").isNull());
    assertEquals("world", result.legacy.at("/tags/2").textValue());
    assertEquals("hello", result.current.at("/tags/0").textValue());
    assertTrue(result.current.at("/tags/1").isNull());
    assertEquals("world", result.current.at("/tags/2").textValue());
  }

  @Test
  void arrayOfMultiTypeUnionItemsHaveBranchWrappersInCurrentPath() throws Exception {
    // Multi-type union items in an array get branch wrappers in the current path but not legacy.
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"ItemRecord\",\"fields\":["
                    + "{\"name\":\"items\",\"type\":{\"type\":\"array\","
                    + "\"items\":[\"null\",\"string\",\"int\"]}}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("items", Arrays.asList("hello", 42, null));

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // legacy: flat values within the array
    assertEquals("hello", result.legacy.at("/items/0").textValue());
    assertEquals(42, result.legacy.at("/items/1").intValue());
    assertTrue(result.legacy.at("/items/2").isNull());
    // current: each non-null item is wrapped with its branch type key
    assertEquals(
        "hello",
        result.current.at("/items/0/string").textValue(),
        result.current.toPrettyString());
    assertEquals(
        42,
        result.current.at("/items/1/int").intValue(),
        result.current.toPrettyString());
    assertTrue(result.current.at("/items/2").isNull());
  }

  @Test
  void mapWithNullableUnionValuesIsUnwrappedInBothPaths() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"AttrRecord\",\"fields\":["
                    + "{\"name\":\"attrs\",\"type\":{\"type\":\"map\","
                    + "\"values\":[\"null\",\"string\"]}}]}");
    Map<String, Object> mapValue = new HashMap<>();
    mapValue.put("name", "Alice");
    mapValue.put("dept", null);
    GenericRecord record = new GenericData.Record(schema);
    record.put("attrs", mapValue);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // Both paths: nullable union map values are flat
    assertEquals("Alice", result.legacy.at("/attrs/name").textValue());
    assertTrue(result.legacy.at("/attrs/dept").isNull());
    assertEquals("Alice", result.current.at("/attrs/name").textValue());
    assertTrue(result.current.at("/attrs/dept").isNull());
  }

  @Test
  void mapWithMultiTypeUnionValuesHaveBranchWrappersInCurrentPath() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"PropRecord\",\"fields\":["
                    + "{\"name\":\"props\",\"type\":{\"type\":\"map\","
                    + "\"values\":[\"null\",\"string\",\"int\"]}}]}");
    Map<String, Object> mapValue = new HashMap<>();
    mapValue.put("str", "hello");
    mapValue.put("num", 42);
    mapValue.put("missing", null);
    GenericRecord record = new GenericData.Record(schema);
    record.put("props", mapValue);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // legacy: flat values
    assertEquals("hello", result.legacy.at("/props/str").textValue());
    assertEquals(42, result.legacy.at("/props/num").intValue());
    assertTrue(result.legacy.at("/props/missing").isNull());
    // current: string and int values wrapped with branch type key
    assertEquals(
        "hello",
        result.current.at("/props/str/string").textValue(),
        result.current.toPrettyString());
    assertEquals(
        42,
        result.current.at("/props/num/int").intValue(),
        result.current.toPrettyString());
    assertTrue(result.current.at("/props/missing").isNull());
  }

  @Test
  void enumTypeRendersAsSymbolStringInBothPaths() throws Exception {
    // ENUM logical type is compatible: both paths produce the symbol string — no path breakage.
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"StatusRecord\",\"fields\":["
                    + "{\"name\":\"status\",\"type\":"
                    + "{\"type\":\"enum\",\"name\":\"Status\","
                    + "\"symbols\":[\"ACTIVE\",\"INACTIVE\",\"PENDING\"]}}]}");
    Schema enumSchema = schema.getField("status").schema();
    GenericRecord record = new GenericData.Record(schema);
    record.put("status", new GenericData.EnumSymbol(enumSchema, "ACTIVE"));

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals("ACTIVE", result.legacy.at("/status").textValue());
    assertEquals("ACTIVE", result.current.at("/status").textValue());
  }

  @Test
  void uuidLogicalTypeRendersAsStringInBothPaths() throws Exception {
    Schema uuidSchema = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    Schema schema =
        SchemaBuilder.record("IdRecord")
            .fields()
            .name("id")
            .type(uuidSchema)
            .noDefault()
            .endRecord();
    String uuid = "550e8400-e29b-41d4-a716-446655440000";
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", uuid);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // UUID logical type on STRING: both paths produce the UUID string unchanged
    assertEquals(uuid, result.legacy.at("/id").textValue());
    assertEquals(uuid, result.current.at("/id").textValue());
  }

  @Test
  void timestampMicrosIsPassedThroughAsRawLongInBothPaths() throws Exception {
    // Unlike timestamp-millis (which KC converts to ISO string), Confluent 7.9.x maps
    // timestamp-micros to Connect INT64 (raw passthrough). Both paths preserve the raw
    // microseconds value unchanged, so downstream consumers see a numeric epoch microseconds
    // field rather than an ISO string. This is a notable contrast with timestamp-millis behavior.
    Schema tsSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema =
        SchemaBuilder.record("TsMicrosRecord")
            .fields()
            .name("ts")
            .type(tsSchema)
            .noDefault()
            .endRecord();
    long micros =
        LocalDate.of(2024, 1, 15).atTime(10, 0).toInstant(ZoneOffset.UTC).toEpochMilli() * 1000L;
    GenericRecord record = new GenericData.Record(schema);
    record.put("ts", micros);

    AvroTestSupport.Comparison result = support.compare(schema, record);

    // Both legacy and current paths preserve raw epoch microseconds (no ISO conversion)
    assertEquals(micros, result.legacy.get("ts").longValue(), "legacy: raw epoch microseconds");
    assertEquals(micros, result.current.get("ts").longValue(),
        "current: Confluent 7.9.x maps timestamp-micros to INT64 passthrough (unlike timestamp-millis)");
  }

  @Test
  void decimalNegativeValueProducesSameJsonNumber() throws Exception {
    Schema decimalSchema =
        LogicalTypes.decimal(20, 4).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema =
        SchemaBuilder.record("NegDecimalRecord")
            .fields()
            .name("amount")
            .type(decimalSchema)
            .noDefault()
            .endRecord();
    BigDecimal amount = new BigDecimal("-90.0000");
    GenericRecord record = new GenericData.Record(schema);
    record.put("amount", ByteBuffer.wrap(amount.unscaledValue().toByteArray()));

    AvroTestSupport.Comparison result = support.compare(schema, record);

    assertEquals(0, new BigDecimal(result.legacy.get("amount").asText()).compareTo(amount));
    assertEquals(0, new BigDecimal(result.current.get("amount").asText()).compareTo(amount));
  }

  @Test
  void tombstoneNullPayloadProducesNullSchemaAndValue() {
    // A Kafka tombstone (null value bytes) must produce SchemaAndValue.NULL, not a converted record.
    // KC v4 filters tombstone SinkRecords before RECORD_CONTENT construction.
    SchemaAndValue result = support.convertCurrentTombstone();

    assertNull(result.schema(), "tombstone must yield null schema");
    assertNull(result.value(), "tombstone must yield null value");
  }

  // ============================================================
  // Three-path tests — aborted (not failed) when LegacyCompatibleAvroConverter
  // is not yet on the classpath. These are the acceptance oracle for the
  // compatibility converter implementation.
  // ============================================================

  @Test
  void threePathNullableUnionAgreesAcrossAllPaths() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"UnionRecord\",\"fields\":["
                    + "{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("status", "active");

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    assertEquals("active", result.legacy.at("/status").textValue());
    assertEquals("active", result.current.at("/status").textValue());
    assertEquals(
        "active",
        result.compat.at("/status").textValue(),
        "compat path must agree with legacy for simple nullable union: " + result.compat.toPrettyString());
  }

  @Test
  void threePathMultiTypeUnionIsUnwrappedByCompatConverter() throws Exception {
    // The compat converter must reproduce the legacy RECORD_CONTENT path (no branch wrapper)
    // while reusing Confluent's Schema Registry framing, TLS, caching, and retry semantics.
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"MultiUnionRecord\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("value", "person@example.com");

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    // legacy and compat: flat value (the primary acceptance criterion)
    assertEquals("person@example.com", result.legacy.at("/value").textValue());
    assertEquals(
        "person@example.com",
        result.compat.at("/value").textValue(),
        "compat must unwrap multi-type union to match legacy: " + result.compat.toPrettyString());
    // verify no branch wrapper on compat path
    assertFalse(
        result.compat.at("/value").isObject(),
        "compat /value must not be a branch-wrapper object");
    // current path still has the wrapper (documents the KC 4.x behavior)
    assertEquals("person@example.com", result.current.at("/value/string").textValue());
  }

  @Test
  void threePathDateLogicalTypePreservedAsEpochIntegerByCompatConverter() throws Exception {
    // The compat converter must preserve date as raw epoch days (legacy) not ISO string (current).
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema schema =
        SchemaBuilder.record("DateRecord")
            .fields()
            .name("eventDate")
            .type(dateSchema)
            .noDefault()
            .endRecord();
    int days = (int) LocalDate.of(2024, 1, 15).toEpochDay();
    GenericRecord record = new GenericData.Record(schema);
    record.put("eventDate", days);

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    assertEquals(days, result.legacy.get("eventDate").intValue());
    assertEquals("2024-01-15T00:00:00.000Z", result.current.get("eventDate").textValue());
    assertEquals(
        days,
        result.compat.get("eventDate").intValue(),
        "compat must preserve date as epoch integer, not ISO string: " + result.compat.toPrettyString());
  }

  @Test
  void threePathTombstoneHandledByCompatConverter() {
    // A tombstone (null bytes) must yield SchemaAndValue.NULL from the compat converter,
    // not a conversion error or empty record.
    Optional<Converter> compatOpt = support.tryCreateCompatConverter();
    assumeTrue(compatOpt.isPresent(), "LegacyCompatibleAvroConverter not yet available");

    SchemaAndValue result = compatOpt.get().toConnectData(AvroTestSupport.TOPIC, null);

    assertNull(result.schema(), "compat converter must return null schema for tombstone");
    assertNull(result.value(), "compat converter must return null value for tombstone");
  }

  @Test
  void malformedConfluentPayloadIsRejectedByCompatConverter() {
    Converter compat = support.tryCreateCompatConverter().orElseThrow(AssertionError::new);
    byte[] invalidMagicBytePayload = {1, 0, 0, 0, 1};

    DataException error =
        assertThrows(
            DataException.class,
            () -> compat.toConnectData(AvroTestSupport.TOPIC, invalidMagicBytePayload));

    assertTrue(error.getMessage().contains("Failed to deserialize Avro payload"));
  }

  @Test
  void threePathTimestampMillisPreservedAsEpochLongByCompatConverter() throws Exception {
    // compat must produce raw epoch millis; current path converts to ISO string.
    Schema tsSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema =
        SchemaBuilder.record("TsMillisRecord")
            .fields()
            .name("ts")
            .type(tsSchema)
            .noDefault()
            .endRecord();
    long millis = LocalDate.of(2024, 1, 15).atTime(10, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    GenericRecord record = new GenericData.Record(schema);
    record.put("ts", millis);

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    assertEquals(millis, result.legacy.get("ts").longValue(), "legacy: raw epoch millis");
    assertEquals("2024-01-15T10:00:00.000Z", result.current.get("ts").textValue(),
        "current: Confluent converts to ISO string");
    assertEquals(millis, result.compat.get("ts").longValue(),
        "compat must preserve raw epoch millis, not convert to ISO string: " + result.compat.toPrettyString());
    assertFalse(result.compat.get("ts").isTextual(),
        "compat ts must be numeric, not string: " + result.compat.toPrettyString());
  }

  @Test
  void threePathDecimalPreservedAsJsonNumberByCompatConverter() throws Exception {
    Schema decimalSchema =
        LogicalTypes.decimal(20, 4).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema =
        SchemaBuilder.record("DecimalRecord3P")
            .fields()
            .name("amount")
            .type(decimalSchema)
            .noDefault()
            .endRecord();
    BigDecimal amount = new BigDecimal("90.0000");
    GenericRecord record = new GenericData.Record(schema);
    record.put("amount", ByteBuffer.wrap(amount.unscaledValue().toByteArray()));

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    assertEquals(0, amount.compareTo(new BigDecimal(result.legacy.get("amount").asText())));
    assertEquals(0, amount.compareTo(new BigDecimal(result.compat.get("amount").asText())),
        "compat must preserve decimal as JSON number: " + result.compat.toPrettyString());
    assertFalse(result.compat.get("amount").isTextual(),
        "compat decimal must not be stringified: " + result.compat.toPrettyString());
  }

  @Test
  void threePathMultiTypeUnionIntBranchIsUnwrappedByCompatConverter() throws Exception {
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"MultiUnionInt\",\"fields\":["
                    + "{\"name\":\"value\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("value", 42);

    AvroTestSupport.ThreePathComparison result = support.compareAll(schema, record);
    assumeTrue(result.compat != null, "LegacyCompatibleAvroConverter not yet available");

    assertEquals(42, result.legacy.at("/value").intValue(), "legacy: raw int");
    assertEquals(42, result.current.at("/value/int").intValue(), "current: wrapped with int key");
    assertEquals(42, result.compat.at("/value").intValue(),
        "compat must unwrap int branch to match legacy: " + result.compat.toPrettyString());
    assertFalse(result.compat.at("/value").isObject(),
        "compat /value must not be a branch-wrapper object");
  }

  // ============================================================
  // Schema evolution integration test — single converter instance, multiple schema versions.
  // Verifies that one LegacyCompatibleAvroConverter handles evolving schema IDs correctly
  // and produces the correct KC RECORD_CONTENT for old and new records.
  // ============================================================

  @Test
  void schemaEvolutionMultipleVersionsThroughSingleCompatConverterInstance() throws Exception {
    // v1 schema: id + amount only
    Schema v1 =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"amount\",\"type\":\"double\"}]}");

    // v2 schema: adds optional region (nullable union, default null) and
    // currency (string with default "USD").  Both new fields carry defaults
    // per Avro schema evolution rules for backward compatibility.
    Schema v2 =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
                    + "{\"name\":\"id\",\"type\":\"int\"},"
                    + "{\"name\":\"amount\",\"type\":\"double\"},"
                    + "{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":null},"
                    + "{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\"}]}");

    // One converter instance — simulates a KC worker receiving records from an evolving topic
    Converter compat = support.tryCreateCompatConverter().orElse(null);
    assumeTrue(compat != null, "LegacyCompatibleAvroConverter not yet available");

    // Serialize records using their respective writer schemas; each gets its own schema ID in
    // the shared MockSchemaRegistryClient, which the converter's KafkaAvroDeserializer resolves
    // per payload via the embedded schema ID.
    GenericRecord v1Record = new GenericData.Record(v1);
    v1Record.put("id", 101);
    v1Record.put("amount", 9.99);
    byte[] v1Payload = support.serialize(v1, v1Record);

    GenericRecord v2RecordWithRegion = new GenericData.Record(v2);
    v2RecordWithRegion.put("id", 102);
    v2RecordWithRegion.put("amount", 19.99);
    v2RecordWithRegion.put("region", "US");
    v2RecordWithRegion.put("currency", "EUR");
    byte[] v2Payload = support.serialize(v2, v2RecordWithRegion);

    // Explicit null on the optional field: must be preserved as JSON null, not omitted
    GenericRecord v2RecordNullRegion = new GenericData.Record(v2);
    v2RecordNullRegion.put("id", 103);
    v2RecordNullRegion.put("amount", 5.00);
    v2RecordNullRegion.put("region", null);
    v2RecordNullRegion.put("currency", "USD");
    byte[] v2NullPayload = support.serialize(v2, v2RecordNullRegion);

    int v1SchemaId = ByteBuffer.wrap(v1Payload, 1, Integer.BYTES).getInt();
    int v2SchemaId = ByteBuffer.wrap(v2Payload, 1, Integer.BYTES).getInt();
    int v2NullSchemaId = ByteBuffer.wrap(v2NullPayload, 1, Integer.BYTES).getInt();
    assertNotEquals(v1SchemaId, v2SchemaId, "v1 and v2 payloads must use different schema IDs");
    assertEquals(v2SchemaId, v2NullSchemaId, "v2 payloads must use the same schema ID");

    // Process all three payloads through the SAME converter instance
    JsonNode v1Rc = support.recordContent(compat, v1Payload);
    JsonNode v2Rc = support.recordContent(compat, v2Payload);
    JsonNode v2NullRc = support.recordContent(compat, v2NullPayload);

    // v1 RECORD_CONTENT: only writer-schema fields present; no default injection from v2
    assertEquals(101, v1Rc.get("id").intValue(), "v1 id");
    assertEquals(9.99, v1Rc.get("amount").doubleValue(), 0.001, "v1 amount");
    assertFalse(v1Rc.has("region"), "v1 RECORD_CONTENT must not have region: " + v1Rc.toPrettyString());
    assertFalse(v1Rc.has("currency"), "v1 RECORD_CONTENT must not have currency: " + v1Rc.toPrettyString());

    // v2 RECORD_CONTENT with region set: nullable union field unwrapped (flat string, no wrapper)
    assertEquals(102, v2Rc.get("id").intValue(), "v2 id");
    assertEquals(19.99, v2Rc.get("amount").doubleValue(), 0.001, "v2 amount");
    assertEquals(
        "US",
        v2Rc.get("region").textValue(),
        "v2 region must be a flat string (no branch-wrapper object): " + v2Rc.toPrettyString());
    assertFalse(v2Rc.get("region").isObject(), "v2 region must not be branch-wrapped");
    assertEquals("EUR", v2Rc.get("currency").textValue(), "v2 currency");

    // v2 RECORD_CONTENT with explicit null region: field present and JSON null (meaningful null)
    assertEquals(103, v2NullRc.get("id").intValue(), "v2-null id");
    assertTrue(
        v2NullRc.has("region"),
        "v2 null-region field must be present in RECORD_CONTENT: " + v2NullRc.toPrettyString());
    assertTrue(
        v2NullRc.get("region").isNull(),
        "explicit null region must be JSON null: " + v2NullRc.toPrettyString());
    assertEquals("USD", v2NullRc.get("currency").textValue(), "v2-null currency");
  }

  // ============================================================
  // Concurrency test — aborted when LegacyCompatibleAvroConverter is unavailable.
  // Verifies that a single converter instance (as used by KC worker threads) is thread-safe.
  // ============================================================

  @Test
  void compatConverterIsSafeUnderConcurrentLoad() throws Exception {
    Optional<Converter> compatOpt = support.tryCreateCompatConverter();
    assumeTrue(compatOpt.isPresent(), "LegacyCompatibleAvroConverter not yet available");

    Converter compat = compatOpt.get();
    Schema schema =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
                    + "{\"name\":\"v\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("v", "concurrent-test-value");
    // Pre-serialize once so all threads use the same payload (same schema ID in registry)
    byte[] payload = support.serialize(schema, record);

    int parallelism = 8;
    int callsPerThread = 25;
    ExecutorService exec = Executors.newFixedThreadPool(parallelism);
    List<Future<SchemaAndValue>> futures = new ArrayList<>();
    for (int i = 0; i < parallelism * callsPerThread; i++) {
      futures.add(exec.submit(() -> compat.toConnectData(AvroTestSupport.TOPIC, payload)));
    }

    // Collect all results and compare to the first
    SchemaAndValue reference = futures.get(0).get(30, TimeUnit.SECONDS);
    for (Future<SchemaAndValue> f : futures) {
      SchemaAndValue got = f.get(30, TimeUnit.SECONDS);
      assertEquals(
          reference.value(),
          got.value(),
          "All concurrent toConnectData calls must return consistent values");
    }
    exec.shutdown();
    assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
  }
}
