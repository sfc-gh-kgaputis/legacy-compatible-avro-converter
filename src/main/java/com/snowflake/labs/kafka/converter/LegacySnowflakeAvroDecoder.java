package com.snowflake.labs.kafka.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

/** Reproduces the direct Avro-to-JSON decoding used by SnowflakeAvroConverter in KC v3. */
public final class LegacySnowflakeAvroDecoder {
  private static final byte CONFLUENT_MAGIC_BYTE = 0;

  private final SchemaRegistryClient schemaRegistry;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public LegacySnowflakeAvroDecoder(SchemaRegistryClient schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  public JsonNode decode(byte[] payload) throws IOException {
    return decode(payload, null);
  }

  public JsonNode decode(byte[] payload, Schema readerSchema) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(payload);
    if (buffer.remaining() < 5 || buffer.get() != CONFLUENT_MAGIC_BYTE) {
      throw new IOException("Payload is not in Confluent Avro wire format");
    }

    int schemaId = buffer.getInt();
    Schema writerSchema;
    try {
      writerSchema = schemaRegistry.getById(schemaId);
    } catch (Exception e) {
      throw new IOException("Unable to load writer schema " + schemaId, e);
    }

    byte[] avroData = new byte[buffer.remaining()];
    buffer.get(avroData);

    GenericData genericData = new GenericData();
    genericData.addLogicalTypeConversion(new Conversions.DecimalConversion());
    GenericDatumReader<Object> reader =
        new GenericDatumReader<>(
            writerSchema, readerSchema == null ? writerSchema : readerSchema, genericData);
    Decoder decoder =
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(avroData), null);
    Object datum = reader.read(null, decoder);
    return objectMapper.readTree(datum.toString());
  }
}