package com.snowflake.labs.kafka.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * Kafka Connect Converter that preserves the legacy SnowflakeAvroConverter RECORD_CONTENT
 * contract for KC v4 / Snowpipe Streaming high-performance architecture.
 *
 * <p>Design: delegate all Schema Registry operations (wire format framing, schema lookup,
 * caching, auth, TLS, rules, migrations, retries) to Confluent's {@link KafkaAvroDeserializer},
 * then bypass the lossy Avro-to-Connect mapping layer by applying {@link LegacyAvroValueMapper}
 * instead of {@code AvroData.toConnectData()}. Returns schema-less Java values (Map/List/scalar)
 * compatible with {@code snowflake.enable.schematization=false}.
 *
 * <p>Key configuration choices applied at converter startup:
 * <ul>
 *   <li>{@code avro.use.logical.type.converters=false} — temporal fields stay as raw epoch
 *       integers/longs, matching legacy RECORD_CONTENT behavior.
 *   <li>{@code specific.avro.reader=false} — always produce {@code GenericRecord}, not
 *       Avro-generated specific classes.
 * </ul>
 *
 * <p>The {@code fromConnectData} direction lazily delegates to a standard {@link AvroConverter}
 * to satisfy the full public {@link Converter} interface. This converter is sink-focused.
 *
 * <p>This class is intended for production use. Runtime code does not import Snowflake connector
 * internals; the KC dependency belongs in test scope.
 */
public final class LegacyCompatibleAvroConverter implements Converter {

    // null for production path; non-null only when injected by the package-private test constructor.
    private final SchemaRegistryClient injectedRegistry;
    private KafkaAvroDeserializer deserializer;
    private AvroConverter outboundDelegate;
    private Map<String, Object> effectiveConfigs;
    private boolean isKey;

    /** Production no-arg constructor. No SchemaRegistryClient is injected; the deserializer
     * resolves its own registry via the {@code schema.registry.url} configuration property. */
    public LegacyCompatibleAvroConverter() {
        this.injectedRegistry = null;
    }

    /**
     * Package-private constructor for testing. Wires a shared {@link SchemaRegistryClient} so
     * test code can pre-register schemas and look them up by ID without a real registry endpoint.
     * Production workers always use the no-arg constructor; the injected-registry path is
     * compile-time invisible outside this package.
     */
    LegacyCompatibleAvroConverter(SchemaRegistryClient registry) {
        this.injectedRegistry = registry;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;

        Map<String, Object> cfg = new HashMap<>(configs);
        // Suppress logical type converters so temporal fields stay as raw epoch int/long.
        cfg.put("avro.use.logical.type.converters", "false");
        // Always use generic (not Avro-generated specific) records.
        cfg.put("specific.avro.reader", "false");

        this.effectiveConfigs = cfg;
        this.deserializer = injectedRegistry != null
                ? new KafkaAvroDeserializer(injectedRegistry)
                : new KafkaAvroDeserializer();
        this.deserializer.configure(cfg, isKey);
    }

    /**
     * Converts a standard Confluent Avro wire-format payload (magic byte + schema ID in payload).
     * A null payload is treated as a tombstone and returns {@link SchemaAndValue#NULL}.
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return convert(topic, null, value);
    }

    /**
     * Header-aware variant: the Schema ID may be carried in Kafka record headers rather than
     * embedded in the payload wire prefix, as supported by KC v4.
     */
    @Override
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        return convert(topic, headers, value);
    }

    /** Delegates to a standard AvroConverter. This converter is sink-focused. */
    @Override
    public byte[] fromConnectData(String topic, org.apache.kafka.connect.data.Schema schema, Object value) {
        return outboundDelegate().fromConnectData(topic, schema, value);
    }

    @Override
    public byte[] fromConnectData(String topic, Headers headers,
            org.apache.kafka.connect.data.Schema schema, Object value) {
        return outboundDelegate().fromConnectData(topic, headers, schema, value);
    }

    private SchemaAndValue convert(String topic, Headers headers, byte[] value) {
        if (value == null) {
            return SchemaAndValue.NULL;
        }
        try {
            Object datum = headers != null
                    ? deserializer.deserialize(topic, headers, value)
                    : deserializer.deserialize(topic, value);
            if (datum == null) {
                return SchemaAndValue.NULL;
            }
            Schema avroSchema = extractSchema(datum);
            Object mapped = LegacyAvroValueMapper.toValue(avroSchema, datum);
            return new SchemaAndValue(null, mapped);
        } catch (Exception e) {
            throw new DataException("Failed to deserialize Avro payload for topic " + topic, e);
        }
    }

    /**
     * Extracts the Avro schema from a datum produced by KafkaAvroDeserializer.
     * GenericRecord and other GenericContainer types carry their schema directly.
     */
    private static Schema extractSchema(Object datum) {
        if (datum instanceof GenericContainer) {
            return ((GenericContainer) datum).getSchema();
        }
        return null; // primitive top-level datums: mapper falls back to inferFromContainer
    }

    private synchronized AvroConverter outboundDelegate() {
        if (outboundDelegate == null) {
            outboundDelegate = new AvroConverter();
            outboundDelegate.configure(effectiveConfigs, isKey);
        }
        return outboundDelegate;
    }
}
