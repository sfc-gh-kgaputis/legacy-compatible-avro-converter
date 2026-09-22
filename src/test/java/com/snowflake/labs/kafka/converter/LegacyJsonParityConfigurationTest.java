package com.snowflake.labs.kafka.converter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LegacyJsonParityConfigurationTest {

  private AvroTestSupport support;

  @BeforeEach
  void setUp() {
    support = new AvroTestSupport();
  }

  @Test
  void defaultRetainsCorrectedLegacyJsonRepresentations() throws Exception {
    Schema schema = compatibilitySchema("DefaultMode");
    byte[] raw = {0, 1, 127, (byte) 128, (byte) 255};
    GenericRecord datum = compatibilityRecord(schema, raw);

    @SuppressWarnings("unchecked")
    Map<String, Object> value = (Map<String, Object>) support.tryCreateCompatConverter()
        .orElseThrow()
        .toConnectData(AvroTestSupport.TOPIC, support.serialize(schema, datum))
        .value();

    assertInstanceOf(String.class, value.get("payload"));
    assertEquals(List.of(0, 1, 127, -128, -1), value.get("token"));
    assertEquals("NaN", value.get("metric"));
  }

  @Test
  void falseRestoresPublishedVersion100RepresentationsForBothOverloads() throws Exception {
    Schema schema = compatibilitySchema("Version100Mode");
    byte[] raw = {0, 1, 127, (byte) 128, (byte) 255};
    byte[] payload = support.serialize(schema, compatibilityRecord(schema, raw));
    Converter converter = version100Converter(false);

    assertVersion100Value(converter.toConnectData(AvroTestSupport.TOPIC, payload).value(), raw);
    assertVersion100Value(converter.toConnectData(
        AvroTestSupport.TOPIC, new RecordHeaders(), payload).value(), raw);
  }

  @Test
  void falseStringIsAccepted() throws Exception {
    Schema schema = compatibilitySchema("StringMode");
    byte[] raw = {1, -1, 2, -2, 3};
    byte[] payload = support.serialize(schema, compatibilityRecord(schema, raw));

    assertVersion100Value(version100Converter("FALSE")
        .toConnectData(AvroTestSupport.TOPIC, payload).value(), raw);
  }

  @Test
  void invalidFlagValuesFailConfiguration() {
    for (Object invalid : List.of("yes", 1, Map.of("enabled", false))) {
      Map<String, Object> config = new HashMap<>();
      config.put("schema.registry.url", AvroTestSupport.REGISTRY_URL);
      config.put(LegacyCompatibleAvroConverter.LEGACY_JSON_PARITY_ENABLED_CONFIG, invalid);

      ConfigException error = assertThrows(
          ConfigException.class,
          () -> new LegacyCompatibleAvroConverter().configure(config, false));
      assertTrue(error.getMessage().contains(
          LegacyCompatibleAvroConverter.LEGACY_JSON_PARITY_ENABLED_CONFIG));
    }
  }

  @Test
  void optOutAppliesToReaderSchemaDefaults() throws Exception {
    Schema writer = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"ReaderMode\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"}]}" );
    Schema reader = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"ReaderMode\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"payload\",\"type\":\"bytes\",\"default\":\"\\u0001\\u00ff\"}]}" );
    GenericRecord datum = new GenericData.Record(writer);
    datum.put("id", "reader");
    Map<String, Object> config = new HashMap<>();
    config.put(LegacyCompatibleAvroConverter.READER_SCHEMA_CONFIG, reader.toString());
    config.put(LegacyCompatibleAvroConverter.LEGACY_JSON_PARITY_ENABLED_CONFIG, false);
    Converter converter = support.compatConverterWith(config);

    @SuppressWarnings("unchecked")
    Map<String, Object> value = (Map<String, Object>) converter.toConnectData(
        AvroTestSupport.TOPIC, support.serialize(writer, datum)).value();
    assertArrayEquals(new byte[] {1, -1}, (byte[]) value.get("payload"));
  }

  @Test
  void optOutIsStableAcrossWriterSchemasAndConcurrentCalls() throws Exception {
    Schema first = compatibilitySchema("ConcurrentFirst");
    Schema second = compatibilitySchema("ConcurrentSecond");
    byte[] firstRaw = {1, 2, 3, 4, 5};
    byte[] secondRaw = {-1, -2, -3, -4, -5};
    byte[] firstPayload = support.serialize(first, compatibilityRecord(first, firstRaw));
    byte[] secondPayload = support.serialize(second, compatibilityRecord(second, secondRaw));
    Converter converter = version100Converter(false);
    ExecutorService executor = Executors.newFixedThreadPool(6);

    try {
      List<Future<?>> futures = new java.util.ArrayList<>();
      for (int index = 0; index < 60; index++) {
        final boolean useFirst = index % 2 == 0;
        futures.add(executor.submit(() -> assertVersion100Value(
            converter.toConnectData(
                AvroTestSupport.TOPIC, useFirst ? firstPayload : secondPayload).value(),
            useFirst ? firstRaw : secondRaw)));
      }
      for (Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private Converter version100Converter(Object configuredValue) {
    Map<String, Object> config = new HashMap<>();
    config.put(
        LegacyCompatibleAvroConverter.LEGACY_JSON_PARITY_ENABLED_CONFIG,
        configuredValue);
    return support.compatConverterWith(config);
  }

  private static Schema compatibilitySchema(String name) {
    return new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":["
            + "{\"name\":\"payload\",\"type\":\"bytes\"},"
            + "{\"name\":\"token\",\"type\":{\"type\":\"fixed\","
            + "\"name\":\"Token\",\"size\":5}},"
            + "{\"name\":\"metric\",\"type\":\"double\"}]}" );
  }

  private static GenericRecord compatibilityRecord(Schema schema, byte[] raw) {
    GenericRecord datum = new GenericData.Record(schema);
    datum.put("payload", ByteBuffer.wrap(raw));
    datum.put("token", new GenericData.Fixed(schema.getField("token").schema(), raw));
    datum.put("metric", Double.NaN);
    return datum;
  }

  private static void assertVersion100Value(Object converted, byte[] raw) {
    @SuppressWarnings("unchecked")
    Map<String, Object> value = (Map<String, Object>) converted;
    assertArrayEquals(raw, (byte[]) value.get("payload"));
    assertArrayEquals(raw, (byte[]) value.get("token"));
    assertInstanceOf(Double.class, value.get("metric"));
    assertEquals(Double.NaN, value.get("metric"));
  }
}
