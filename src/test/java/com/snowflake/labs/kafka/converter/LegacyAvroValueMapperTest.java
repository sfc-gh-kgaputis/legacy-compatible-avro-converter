package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

class LegacyAvroValueMapperTest {

  // ------------------------------------------------------------------
  // Null handling
  // ------------------------------------------------------------------

  @Test
  void nullDatumWithNonNullSchemaReturnsNull() {
    assertNull(LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.STRING), null));
  }

  @Test
  void nullDatumWithNullSchemaReturnsNull() {
    assertNull(LegacyAvroValueMapper.toValue(null, null));
  }

  // ------------------------------------------------------------------
  // Scalar pass-through (INT, LONG, FLOAT, DOUBLE, BOOLEAN)
  // ------------------------------------------------------------------

  @Test
  void intPassesThroughRaw() {
    assertEquals(42, LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.INT), 42));
  }

  @Test
  void longPassesThroughRaw() {
    assertEquals(123456789L, LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.LONG), 123456789L));
  }

  @Test
  void floatPassesThroughRaw() {
    assertEquals(1.5f, LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.FLOAT), 1.5f));
  }

  @Test
  void doublePassesThroughRaw() {
    assertEquals(2.5, LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.DOUBLE), 2.5));
  }

  @Test
  void booleanPassesThroughRaw() {
    assertEquals(Boolean.TRUE, LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.BOOLEAN), true));
  }

  // ------------------------------------------------------------------
  // Temporal logical types must stay as raw epoch values (legacy contract).
  // avro.use.logical.type.converters=false is enforced at the converter layer;
  // here we verify the mapper itself never converts INT/LONG regardless of annotation.
  // ------------------------------------------------------------------

  @Test
  void dateLogicalTypePreservesRawEpochInt() {
    Schema schema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    int epochDays = (int) LocalDate.of(2024, 1, 15).toEpochDay();
    Object result = LegacyAvroValueMapper.toValue(schema, epochDays);
    assertEquals(epochDays, result, "date logical type must stay as raw epoch integer, not ISO string");
    assertFalse(result instanceof String, "date must not be converted to String");
  }

  @Test
  void timestampMillisLogicalTypePreservesRawEpochLong() {
    Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    long millis = LocalDate.of(2024, 1, 15).atTime(10, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    Object result = LegacyAvroValueMapper.toValue(schema, millis);
    assertEquals(millis, result, "timestamp-millis must stay as raw epoch long, not ISO string");
    assertFalse(result instanceof String, "timestamp-millis must not be converted to String");
  }

  @Test
  void timeMicrosLogicalTypePreservesRawLong() {
    Schema schema = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    long micros = 36_000_000_000L;
    assertEquals(micros, LegacyAvroValueMapper.toValue(schema, micros));
  }

  // ------------------------------------------------------------------
  // STRING and ENUM
  // ------------------------------------------------------------------

  @Test
  void stringReturnsJavaString() {
    Object result = LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.STRING), "hello");
    assertEquals("hello", result);
    assertInstanceOf(String.class, result);
  }

  @Test
  void enumReturnsSymbolString() {
    Schema enumSchema = Schema.createEnum("Status", null, null,
        Arrays.asList("ACTIVE", "INACTIVE", "PENDING"));
    Object result = LegacyAvroValueMapper.toValue(
        enumSchema, new GenericData.EnumSymbol(enumSchema, "ACTIVE"));
    assertEquals("ACTIVE", result);
  }

  // ------------------------------------------------------------------
  // BYTES and FIXED
  // ------------------------------------------------------------------

  @Test
  void decimalBytesReturnsDouble() {
    Schema schema = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    BigDecimal expected = new BigDecimal("12.34");
    ByteBuffer buf = ByteBuffer.wrap(expected.unscaledValue().toByteArray());
    Object result = LegacyAvroValueMapper.toValue(schema, buf);
    // KC v4 schema-less mode does not support BigDecimal; mapper returns double for JSON-number compatibility.
    assertInstanceOf(Double.class, result);
    assertEquals(expected.doubleValue(), (Double) result, 1e-9);
  }

  @Test
  void decimalBytesNegativeValueReturnsDouble() {
    Schema schema = LogicalTypes.decimal(20, 4).addToSchema(Schema.create(Schema.Type.BYTES));
    BigDecimal expected = new BigDecimal("-90.0000");
    ByteBuffer buf = ByteBuffer.wrap(expected.unscaledValue().toByteArray());
    Object result = LegacyAvroValueMapper.toValue(schema, buf);
    assertInstanceOf(Double.class, result);
    assertEquals(expected.doubleValue(), (Double) result, 1e-9);
  }

  @Test
  void plainBytesReturnsLegacyIso88591String() {
    Schema schema = Schema.create(Schema.Type.BYTES);
    byte[] bytes = {1, 2, 3, 4, (byte) 0xFF};
    Object result = LegacyAvroValueMapper.toValue(schema, ByteBuffer.wrap(bytes));
    assertInstanceOf(String.class, result);
    assertEquals(StandardCharsets.ISO_8859_1.decode(ByteBuffer.wrap(bytes)).toString(), result);
  }

  @Test
  void decimalFixedReturnsDouble() {
    Schema schema = LogicalTypes.decimal(4, 2).addToSchema(Schema.createFixed("D", null, null, 2));
    BigDecimal expected = new BigDecimal("9.99");
    byte[] raw = padLeft(expected.unscaledValue().toByteArray(), 2);
    Object result = LegacyAvroValueMapper.toValue(schema, new GenericData.Fixed(schema, raw));
    assertInstanceOf(Double.class, result);
    assertEquals(expected.doubleValue(), (Double) result, 1e-9);
  }

  @Test
  void plainFixedReturnsLegacySignedByteArray() {
    Schema schema = Schema.createFixed("F", null, null, 4);
    byte[] raw = {10, 20, -30, -1};
    Object result = LegacyAvroValueMapper.toValue(schema, new GenericData.Fixed(schema, raw));
    assertEquals(List.of(10, 20, -30, -1), result);
  }

  @Test
  void nonFiniteFloatAndDoubleRenderAsLegacyJsonStrings() {
    assertEquals("NaN", LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.FLOAT), Float.NaN));
    assertEquals("Infinity",
        LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.DOUBLE), Double.POSITIVE_INFINITY));
    assertEquals("-Infinity",
        LegacyAvroValueMapper.toValue(Schema.create(Schema.Type.DOUBLE), Double.NEGATIVE_INFINITY));
  }

  // ------------------------------------------------------------------
  // RECORD
  // ------------------------------------------------------------------

  @Test
  void simpleRecordReturnedAsLinkedHashMap() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"a\",\"type\":\"string\"},"
        + "{\"name\":\"b\",\"type\":\"int\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("a", "hello");
    record.put("b", 42);
    Object result = LegacyAvroValueMapper.toValue(schema, record);
    assertInstanceOf(LinkedHashMap.class, result);
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) result;
    assertEquals("hello", map.get("a"));
    assertEquals(42, map.get("b"));
  }

  @Test
  void recordFieldOrderIsPreservedInLinkedHashMap() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"z\",\"type\":\"string\"},"
        + "{\"name\":\"a\",\"type\":\"string\"},"
        + "{\"name\":\"m\",\"type\":\"string\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("z", "first");
    record.put("a", "second");
    record.put("m", "third");
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) LegacyAvroValueMapper.toValue(schema, record);
    List<String> keys = new ArrayList<>(result.keySet());
    assertEquals(Arrays.asList("z", "a", "m"), keys);
  }

  @Test
  void nestedRecordMappedRecursively() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":[{\"name\":\"inner\",\"type\":"
        + "{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"v\",\"type\":\"string\"}]}}]}");
    Schema innerSchema = schema.getField("inner").schema();
    GenericRecord inner = new GenericData.Record(innerSchema);
    inner.put("v", "nested");
    GenericRecord outer = new GenericData.Record(schema);
    outer.put("inner", inner);
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) LegacyAvroValueMapper.toValue(schema, outer);
    @SuppressWarnings("unchecked")
    Map<String, Object> innerMap = (Map<String, Object>) result.get("inner");
    assertEquals("nested", innerMap.get("v"));
  }

  @Test
  void nullFieldInRecordMapsToNullInResult() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"v\",\"type\":[\"null\",\"string\"],\"default\":null}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("v", null);
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) LegacyAvroValueMapper.toValue(schema, record);
    assertNull(result.get("v"));
  }

  // ------------------------------------------------------------------
  // ARRAY and MAP
  // ------------------------------------------------------------------

  @Test
  void arrayMappedToList() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));
    Object mapped = LegacyAvroValueMapper.toValue(schema, Arrays.asList("x", "y", "z"));
    assertInstanceOf(List.class, mapped);
    assertEquals(Arrays.asList("x", "y", "z"), mapped);
  }

  @Test
  void mapMappedToStringKeyedLinkedHashMap() {
    Schema schema = Schema.createMap(Schema.create(Schema.Type.INT));
    Map<String, Integer> input = new LinkedHashMap<>();
    input.put("a", 1);
    input.put("b", 2);
    Object result = LegacyAvroValueMapper.toValue(schema, input);
    assertInstanceOf(LinkedHashMap.class, result);
    assertEquals(1, ((Map<?, ?>) result).get("a"));
    assertEquals(2, ((Map<?, ?>) result).get("b"));
  }

  // ------------------------------------------------------------------
  // UNION — the core contract: active branch emitted directly, never wrapped
  // ------------------------------------------------------------------

  @Test
  void nullableUnionWithNullDatumReturnsNull() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    assertNull(LegacyAvroValueMapper.toValue(schema, null));
  }

  @Test
  void nullableUnionWithStringDatumReturnsUnwrappedString() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    Object result = LegacyAvroValueMapper.toValue(schema, "hello");
    assertEquals("hello", result);
    assertFalse(result instanceof Map, "nullable union must be unwrapped — no branch-wrapper map");
  }

  @Test
  void multiTypeUnionStringBranchIsUnwrapped() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT));
    Object result = LegacyAvroValueMapper.toValue(schema, "value");
    assertEquals("value", result);
    assertFalse(result instanceof Map, "multi-type union string must not be wrapped in branch map");
  }

  @Test
  void multiTypeUnionIntBranchIsUnwrapped() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING),
        Schema.create(Schema.Type.INT));
    Object result = LegacyAvroValueMapper.toValue(schema, 99);
    assertEquals(99, result);
    assertFalse(result instanceof Map, "multi-type union int must not be wrapped in branch map");
  }

  // ------------------------------------------------------------------
  // Null-schema fallback (inferFromContainer)
  // ------------------------------------------------------------------

  @Test
  void nullSchemaFallsBackToInferFromContainerForGenericRecord() {
    Schema schema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}");
    GenericRecord record = new GenericData.Record(schema);
    record.put("x", 99);
    Object result = LegacyAvroValueMapper.toValue(null, record);
    assertInstanceOf(Map.class, result);
    assertEquals(99, ((Map<?, ?>) result).get("x"));
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private static byte[] padLeft(byte[] src, int len) {
    if (src.length >= len) return src;
    byte[] padded = new byte[len];
    System.arraycopy(src, 0, padded, len - src.length, src.length);
    return padded;
  }
}
