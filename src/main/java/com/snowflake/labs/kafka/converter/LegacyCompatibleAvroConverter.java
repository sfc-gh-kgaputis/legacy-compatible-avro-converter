package com.snowflake.labs.kafka.converter;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericContainer;
import org.apache.kafka.common.config.ConfigException;
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
 *   <li>{@code reader.schema} — optional fixed Avro reader schema, parsed once during
 *       configuration and applied through Confluent's writer-to-reader resolution path.
 *   <li>{@code legacy.json.parity.enabled=true} — render plain bytes, plain fixed, and
 *       non-finite floating-point values like the original Snowflake converter. Set to
 *       {@code false} only to preserve this converter's published 1.0.0 representations.
 * </ul>
 *
 * <p>The {@code fromConnectData} direction lazily delegates to a standard {@link AvroConverter}
 * to satisfy the full public {@link Converter} interface. This converter is sink-focused.
 *
 * <p>This class is intended for production use. Runtime code does not import Snowflake connector
 * internals; the KC dependency belongs in test scope.
 */
public final class LegacyCompatibleAvroConverter implements Converter {

    public static final String READER_SCHEMA_CONFIG = "reader.schema";
    public static final String LEGACY_JSON_PARITY_ENABLED_CONFIG = "legacy.json.parity.enabled";

    // null for production path; non-null only when injected by the package-private test constructor.
    private final SchemaRegistryClient injectedRegistry;
    private KafkaAvroDeserializer deserializer;
    private AvroConverter outboundDelegate;
    private Map<String, Object> effectiveConfigs;
    private Schema readerSchema;
    private boolean legacyJsonParityEnabled;
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
        this.readerSchema = parseReaderSchema(configs.get(READER_SCHEMA_CONFIG));
        this.legacyJsonParityEnabled = parseLegacyJsonParityEnabled(
                configs.get(LEGACY_JSON_PARITY_ENABLED_CONFIG));

        Map<String, Object> cfg = new HashMap<>(configs);
        cfg.remove(READER_SCHEMA_CONFIG);
        cfg.remove(LEGACY_JSON_PARITY_ENABLED_CONFIG);
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
            Object datum;
            if (readerSchema == null) {
                datum = headers != null
                        ? deserializer.deserialize(topic, headers, value)
                        : deserializer.deserialize(topic, value);
            } else {
                datum = headers != null
                        ? deserializer.deserialize(topic, headers, value, readerSchema)
                        : deserializer.deserialize(topic, value, readerSchema);
            }
            if (datum == null) {
                return SchemaAndValue.NULL;
            }
            Schema avroSchema = extractSchema(datum);
            Object mapped = LegacyAvroValueMapper.toValue(
                    avroSchema, datum, legacyJsonParityEnabled);
            return new SchemaAndValue(null, mapped);
        } catch (Exception e) {
            String operation = readerSchema == null
                    ? "deserialize Avro payload"
                    : "resolve Avro writer schema to configured reader schema";
            throw new DataException("Failed to " + operation + " for topic " + topic, e);
        }
    }

    private static Schema parseReaderSchema(Object configuredValue) {
        if (configuredValue == null) {
            return null;
        }
        if (!(configuredValue instanceof String)) {
            throw new ConfigException(
                    READER_SCHEMA_CONFIG,
                    configuredValue,
                    "must be a string containing one valid Avro schema");
        }
        try {
            return new Schema.Parser().parse((String) configuredValue);
        } catch (SchemaParseException e) {
            ConfigException error = new ConfigException(
                    READER_SCHEMA_CONFIG,
                    configuredValue,
                    "must contain a valid Avro schema: " + e.getMessage());
            error.initCause(e);
            throw error;
        }
    }

    private static boolean parseLegacyJsonParityEnabled(Object configuredValue) {
        if (configuredValue == null) {
            return true;
        }
        if (configuredValue instanceof Boolean) {
            return (Boolean) configuredValue;
        }
        if (configuredValue instanceof String) {
            String text = ((String) configuredValue).trim();
            if ("true".equalsIgnoreCase(text)) {
                return true;
            }
            if ("false".equalsIgnoreCase(text)) {
                return false;
            }
        }
        throw new ConfigException(
                LEGACY_JSON_PARITY_ENABLED_CONFIG,
                configuredValue,
                "must be true or false");
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
