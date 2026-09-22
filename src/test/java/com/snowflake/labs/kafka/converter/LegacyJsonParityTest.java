package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Differential coverage for the legacy GenericRecord.toString() JSON contract. */
class LegacyJsonParityTest {

  private AvroTestSupport support;

  @BeforeEach
  void setUp() {
    support = new AvroTestSupport();
  }

  @Test
  @DisplayName("primitive values and numeric boundaries match legacy JSON")
  void primitiveValuesAndBoundariesMatch() throws Exception {
    Schema schema = SchemaBuilder.record("PrimitiveValues").namespace("com.example.parity").fields()
        .requiredBoolean("booleanValue")
        .requiredInt("minInt")
        .requiredInt("maxInt")
        .requiredLong("minLong")
        .requiredLong("maxLong")
        .requiredFloat("floatValue")
        .requiredDouble("doubleValue")
        .requiredDouble("negativeZero")
        .endRecord();
    GenericRecord datum = record(schema,
        "booleanValue", true,
        "minInt", Integer.MIN_VALUE,
        "maxInt", Integer.MAX_VALUE,
        "minLong", Long.MIN_VALUE,
        "maxLong", Long.MAX_VALUE,
        "floatValue", 1.25f,
        "doubleValue", -9876.54321d,
        "negativeZero", -0.0d);

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("NaN and infinities retain the legacy quoted string representation")
  void nonFiniteNumbersMatchLegacyQuotedStrings() throws Exception {
    Schema schema = SchemaBuilder.record("NonFinite").namespace("com.example.parity").fields()
        .requiredFloat("floatNaN")
        .requiredFloat("floatPositiveInfinity")
        .requiredDouble("doubleNegativeInfinity")
        .endRecord();
    GenericRecord datum = record(schema,
        "floatNaN", Float.NaN,
        "floatPositiveInfinity", Float.POSITIVE_INFINITY,
        "doubleNegativeInfinity", Double.NEGATIVE_INFINITY);

    JsonNode content = assertParity(schema, datum);
    assertEquals("NaN", content.at("/floatNaN").textValue());
    assertEquals("Infinity", content.at("/floatPositiveInfinity").textValue());
    assertEquals("-Infinity", content.at("/doubleNegativeInfinity").textValue());
  }

  @Test
  @DisplayName("strings and map keys use the same JSON escaping as legacy Avro")
  void escapedStringsAndMapKeysMatch() throws Exception {
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema schema = recordSchema("EscapedStrings",
        field("text", Schema.create(Schema.Type.STRING)), field("attributes", mapSchema));
    Map<String, String> attributes = new LinkedHashMap<>();
    attributes.put("quote\"slash\\control\n", "tab\tbackspace\b");
    attributes.put("unicode---", "snowman---");
    GenericRecord datum = record(schema,
        "text", "quote\" slash\\ newline\n tab\t carriage\r unicode---",
        "attributes", attributes);

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("plain bytes retain the legacy ISO-8859-1 JSON string")
  void plainBytesMatchLegacyIso88591String() throws Exception {
    Schema schema = recordSchema("PlainBytes", field("payload", Schema.create(Schema.Type.BYTES)));
    byte[] raw = {0, 1, 31, 32, 65, 127, (byte) 128, (byte) 255};
    GenericRecord datum = record(schema, "payload", ByteBuffer.wrap(raw));

    JsonNode content = assertParity(schema, datum);
    assertTrue(content.at("/payload").isTextual());
    assertEquals(raw.length, content.at("/payload").textValue().length());
  }

  @Test
  @DisplayName("plain fixed retains the legacy signed-byte JSON array")
  void plainFixedMatchesLegacySignedByteArray() throws Exception {
    Schema fixed = Schema.createFixed("Token", null, "com.example.parity", 5);
    Schema schema = recordSchema("PlainFixed", field("token", fixed));
    GenericRecord datum = record(schema, "token",
        new GenericData.Fixed(fixed, new byte[] {0, 1, 127, (byte) 128, (byte) 255}));

    JsonNode content = assertParity(schema, datum);
    assertEquals("[0,1,127,-128,-1]", content.at("/token").toString());
  }

  @Test
  @DisplayName("enum and UUID logical values match legacy strings")
  void enumAndUuidMatch() throws Exception {
    Schema status = Schema.createEnum(
        "Status", null, "com.example.parity", List.of("ACTIVE", "INACTIVE"));
    Schema uuid = LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING));
    Schema schema = recordSchema("NamedStrings", field("status", status), field("id", uuid));
    GenericRecord datum = record(schema,
        "status", new GenericData.EnumSymbol(status, "ACTIVE"),
        "id", UUID.fromString("00000000-0000-4000-8000-000000000001").toString());

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("populated arrays, maps, and nested records match legacy JSON")
  void populatedContainersMatch() throws Exception {
    Schema item = SchemaBuilder.record("Item").namespace("com.example.parity").fields()
        .requiredString("name")
        .requiredInt("quantity")
        .endRecord();
    Schema schema = recordSchema("Containers",
        field("items", Schema.createArray(item)),
        field("counts", Schema.createMap(Schema.create(Schema.Type.LONG))),
        field("matrix", Schema.createArray(Schema.createArray(Schema.create(Schema.Type.INT)))));
    GenericRecord first = record(item, "name", "first", "quantity", 2);
    GenericRecord second = record(item, "name", "second", "quantity", 5);
    Map<String, Long> counts = new LinkedHashMap<>();
    counts.put("first", 2L);
    counts.put("second", 5L);
    GenericRecord datum = record(schema,
        "items", List.of(first, second),
        "counts", counts,
        "matrix", List.of(List.of(1, 2), List.of(3, 4)));

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("finite recursive records match legacy JSON")
  void finiteRecursiveRecordMatches() throws Exception {
    Schema node = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Node\",\"namespace\":\"com.example.parity\","
            + "\"fields\":[{\"name\":\"value\",\"type\":\"string\"},"
            + "{\"name\":\"next\",\"type\":[\"null\",\"Node\"],\"default\":null}]}" );
    GenericRecord tail = record(node, "value", "tail", "next", null);
    GenericRecord head = record(node, "value", "head", "next", tail);

    assertParity(node, head);
  }

  @Test
  @DisplayName("the full raw temporal logical-type family matches legacy values")
  void temporalLogicalTypesMatchRawLegacyValues() throws Exception {
    Schema date = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMillis = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicros = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMillis = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema timestampMicros = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema localTimestampMillis =
        LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema localTimestampMicros =
        LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema schema = recordSchema("TemporalValues",
        field("date", date),
        field("timeMillis", timeMillis),
        field("timeMicros", timeMicros),
        field("timestampMillis", timestampMillis),
        field("timestampMicros", timestampMicros),
        field("localTimestampMillis", localTimestampMillis),
        field("localTimestampMicros", localTimestampMicros));
    GenericRecord datum = record(schema,
        "date", (int) LocalDate.of(2026, 9, 22).toEpochDay(),
        "timeMillis", 45_296_789,
        "timeMicros", 45_296_789_123L,
        "timestampMillis", 1_795_000_000_123L,
        "timestampMicros", 1_795_000_000_123_456L,
        "localTimestampMillis", 1_795_000_000_123L,
        "localTimestampMicros", 1_795_000_000_123_456L);

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("duration logical fixed values match legacy signed-byte arrays")
  void durationLogicalTypeMatches() throws Exception {
    Schema duration = new Schema.Parser().parse(
        "{\"type\":\"fixed\",\"name\":\"DurationValue\","
            + "\"namespace\":\"com.example.parity\",\"size\":12,\"logicalType\":\"duration\"}");
    Schema schema = recordSchema("DurationRecord", field("duration", duration));
    byte[] raw = {1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, (byte) 128};
    GenericRecord datum = record(schema, "duration", new GenericData.Fixed(duration, raw));

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("ordinary decimal bytes and fixed values match legacy JSON numbers")
  void ordinaryDecimalsMatch() throws Exception {
    Schema decimalBytes = LogicalTypes.decimal(12, 4)
        .addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimalFixed = LogicalTypes.decimal(12, 4)
        .addToSchema(Schema.createFixed("FixedDecimal", null, "com.example.parity", 8));
    Schema schema = recordSchema("Decimals",
        field("bytesValue", decimalBytes), field("fixedValue", decimalFixed));
    Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
    BigDecimal bytesValue = new BigDecimal("90.1250");
    BigDecimal fixedValue = new BigDecimal("-42.5000");
    GenericRecord datum = record(schema,
        "bytesValue", conversion.toBytes(bytesValue, decimalBytes, decimalBytes.getLogicalType()),
        "fixedValue", conversion.toFixed(fixedValue, decimalFixed, decimalFixed.getLogicalType()));

    assertParity(schema, datum);
  }

  @Test
  @DisplayName("high-precision decimals match the legacy Jackson numeric precision")
  void highPrecisionDecimalMatchesLegacyPrecisionLoss() throws Exception {
    Schema decimal = LogicalTypes.decimal(30, 6).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema schema = recordSchema("HighPrecisionDecimal", field("amount", decimal));
    BigDecimal original = new BigDecimal("123456789012345678901234.123456");
    ByteBuffer encoded = new Conversions.DecimalConversion()
        .toBytes(original, decimal, decimal.getLogicalType());
    GenericRecord datum = record(schema, "amount", encoded);

    GenericRecord resolved = support.decodeLegacyDatum(schema, datum);
    JsonNode legacy = support.decodeLegacy(schema, datum);
    JsonNode compat = support.recordContent(schema, datum);

    assertInstanceOf(BigDecimal.class, resolved.get("amount"));
    assertEquals(original, resolved.get("amount"));
    assertEquals(legacy.toString(), compat.toString());
    assertTrue(original.compareTo(legacy.at("/amount").decimalValue()) != 0);
    assertEquals(original.doubleValue(), compat.at("/amount").doubleValue());
  }

  @Test
  @DisplayName("reader defaults for bytes and fixed values match legacy JSON")
  void binaryReaderDefaultsMatch() throws Exception {
    Schema writer = recordSchema("BinaryDefaults", field("id", Schema.create(Schema.Type.STRING)));
    Schema fixed = Schema.createFixed("DefaultFixed", null, "com.example.parity", 3);
    Schema reader = recordSchema("BinaryDefaults",
        field("id", Schema.create(Schema.Type.STRING)),
        fieldWithDefault("bytesDefault", Schema.create(Schema.Type.BYTES), "\u0000\u00FF"),
        fieldWithDefault("fixedDefault", fixed, "\u0001\u0080\u00FF"));
    GenericRecord datum = record(writer, "id", "defaults");

    JsonNode legacy = support.decodeLegacyWithReaderSchema(writer, reader, datum);
    JsonNode compat = support.recordContentWithReaderSchema(writer, reader, datum);

    assertEquals(legacy.toString(), compat.toString());
  }

  @Test
  @DisplayName("top-level primitive support is broader than the original record-only converter")
  void topLevelPrimitiveIsAnIntentionalExtension() throws Exception {
    Schema schema = Schema.create(Schema.Type.STRING);
    byte[] payload = support.serialize(schema, "top-level");
    Converter converter = support.tryCreateCompatConverter().orElseThrow();

    assertThrows(ClassCastException.class, () -> support.decodeLegacy(schema, "top-level"));
    SchemaAndValue converted = converter.toConnectData(AvroTestSupport.TOPIC, payload);
    assertEquals("top-level", converted.value());
  }

  private JsonNode assertParity(Schema schema, Object datum) throws Exception {
    JsonNode legacy = support.decodeLegacy(schema, datum);
    JsonNode compat = support.recordContent(schema, datum);
    assertEquals(legacy.toString(), compat.toString());
    return compat;
  }

  private static Schema recordSchema(String name, Schema.Field... fields) {
    Schema schema = Schema.createRecord(name, null, "com.example.parity", false);
    schema.setFields(List.of(fields));
    return schema;
  }

  private static Schema.Field field(String name, Schema schema) {
    return new Schema.Field(name, schema, null, (Object) null);
  }

  private static Schema.Field fieldWithDefault(String name, Schema schema, Object defaultValue) {
    return new Schema.Field(name, schema, null, defaultValue);
  }

  private static GenericRecord record(Schema schema, Object... fields) {
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < fields.length; i += 2) {
      record.put((String) fields[i], fields[i + 1]);
    }
    return record;
  }
}
