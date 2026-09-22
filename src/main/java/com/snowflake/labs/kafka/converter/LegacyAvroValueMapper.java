package com.snowflake.labs.kafka.converter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Maps an Avro datum to a legacy-compatible Java value (Map/List/scalar/null).
 *
 * <p>Reproduces the JSON structure that the legacy SnowflakeAvroConverter produced in KC v3:
 * union values are unwrapped (no Connect-style type-wrapper maps), logical types are represented
 * as raw epoch integers/longs or BigDecimal, and ordinary nested records are preserved as
 * LinkedHashMap with field-order preserved.
 *
 * <p>This class is stateless and thread-safe.
 */
public final class LegacyAvroValueMapper {

    private LegacyAvroValueMapper() {}

    /**
     * Converts an Avro datum deserialized against {@code schema} to a legacy-compatible Java
     * value. Returns {@code null} for null datums regardless of schema.
     *
     * @param schema the Avro schema of the datum (may be null for top-level non-record datums)
     * @param datum  the Avro datum to convert
     * @return a Java Map, List, String, Number, Boolean, or null
     */
    public static Object toValue(Schema schema, Object datum) {
        return toValue(schema, datum, true);
    }

    /**
     * Converts an Avro datum using either the original Snowflake JSON representation or the
     * representations published by version 1.0.0 of this converter.
     *
     * @param schema the Avro schema of the datum (may be null for top-level non-record datums)
     * @param datum the Avro datum to convert
     * @param legacyJsonParityEnabled whether to correct the three 1.0.0 representation differences
     * @return a Java Map, List, String, Number, byte[], Boolean, or null
     */
    public static Object toValue(
            Schema schema, Object datum, boolean legacyJsonParityEnabled) {
        if (datum == null) {
            return null;
        }
        if (schema == null) {
            return inferFromContainer(datum, legacyJsonParityEnabled);
        }

        Schema effective = resolveUnion(schema, datum);
        switch (effective.getType()) {
            case RECORD:
                return mapRecord((GenericRecord) datum, effective, legacyJsonParityEnabled);
            case ARRAY:
                return mapArray((Collection<?>) datum, effective, legacyJsonParityEnabled);
            case MAP:
                return mapMap((Map<?, ?>) datum, effective, legacyJsonParityEnabled);
            case BYTES:
                return mapBytes(datum, effective, legacyJsonParityEnabled);
            case FIXED:
                return mapFixed(datum, effective, legacyJsonParityEnabled);
            case STRING:
            case ENUM:
                return datum.toString();
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                if (legacyJsonParityEnabled
                        && (datum instanceof Float && !Float.isFinite((Float) datum)
                        || datum instanceof Double && !Double.isFinite((Double) datum))) {
                    return datum.toString();
                }
                return datum;
            case BOOLEAN:
                // Raw numeric and boolean values pass through unchanged.
                // Temporal logical types (date, time, timestamp) intentionally stay as raw
                // epoch integers/longs to preserve the legacy RECORD_CONTENT contract.
                return datum;
            case NULL:
                return null;
            default:
                return datum.toString();
        }
    }

    /** Resolves the active branch of a UNION schema for the given datum. */
    private static Schema resolveUnion(Schema schema, Object datum) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        int branch = GenericData.get().resolveUnion(schema, datum);
        return schema.getTypes().get(branch);
    }

    private static Map<String, Object> mapRecord(
            GenericRecord record, Schema schema, boolean legacyJsonParityEnabled) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Schema.Field field : schema.getFields()) {
            result.put(field.name(), toValue(
                    field.schema(), record.get(field.pos()), legacyJsonParityEnabled));
        }
        return result;
    }

    private static List<Object> mapArray(
            Collection<?> array, Schema schema, boolean legacyJsonParityEnabled) {
        List<Object> result = new ArrayList<>();
        for (Object element : array) {
            result.add(toValue(schema.getElementType(), element, legacyJsonParityEnabled));
        }
        return result;
    }

    private static Map<String, Object> mapMap(
            Map<?, ?> avroMap, Schema schema, boolean legacyJsonParityEnabled) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : avroMap.entrySet()) {
            result.put(entry.getKey().toString(), toValue(
                    schema.getValueType(), entry.getValue(), legacyJsonParityEnabled));
        }
        return result;
    }

    private static Object mapBytes(
            Object datum, Schema schema, boolean legacyJsonParityEnabled) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            byte[] bytes;
            if (datum instanceof ByteBuffer) {
                ByteBuffer buf = ((ByteBuffer) datum).duplicate();
                bytes = new byte[buf.remaining()];
                buf.get(bytes);
            } else {
                bytes = (byte[]) datum;
            }
            // KC v4 KafkaRecordConverter does not support BigDecimal in schema-less mode;
            // convert to double so the value passes through as a JSON number in RECORD_CONTENT.
            // Note: this trades BigDecimal precision for KC v4 compatibility. Values that
            // cannot be represented exactly as double (e.g., >15 significant digits) will
            // lose precision.
            return new BigDecimal(new BigInteger(bytes), decimal.getScale()).doubleValue();
        }
        if (datum instanceof ByteBuffer) {
            ByteBuffer buf = ((ByteBuffer) datum).duplicate();
            if (!legacyJsonParityEnabled) {
                byte[] bytes = new byte[buf.remaining()];
                buf.get(bytes);
                return bytes;
            }
            // GenericData.toString renders plain bytes as an escaped ISO-8859-1 JSON string.
            return StandardCharsets.ISO_8859_1.decode(buf).toString();
        }
        return datum;
    }

    private static Object mapFixed(
            Object datum, Schema schema, boolean legacyJsonParityEnabled) {
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
            byte[] bytes = ((GenericData.Fixed) datum).bytes();
            // See mapBytes note on double vs BigDecimal.
            return new BigDecimal(new BigInteger(bytes), decimal.getScale()).doubleValue();
        }
        if (datum instanceof GenericData.Fixed) {
            if (!legacyJsonParityEnabled) {
                return ((GenericData.Fixed) datum).bytes();
            }
            // GenericData.Fixed.toString renders Arrays.toString(byte[]), a signed integer array.
            List<Integer> bytes = new ArrayList<>();
            for (byte value : ((GenericData.Fixed) datum).bytes()) {
                bytes.add((int) value);
            }
            return bytes;
        }
        return datum;
    }

    /**
     * Fallback when the schema is unavailable; infers the mapping from the runtime Java type.
     * Works for top-level non-record datums that implement GenericContainer.
     */
    private static Object inferFromContainer(Object datum, boolean legacyJsonParityEnabled) {
        if (datum instanceof GenericRecord) {
            GenericRecord record = (GenericRecord) datum;
            return mapRecord(record, record.getSchema(), legacyJsonParityEnabled);
        }
        if (datum instanceof GenericContainer) {
            return toValue(
                    ((GenericContainer) datum).getSchema(), datum, legacyJsonParityEnabled);
        }
        if (datum instanceof Collection) {
            List<Object> result = new ArrayList<>();
            for (Object e : (Collection<?>) datum) {
                result.add(inferFromContainer(e, legacyJsonParityEnabled));
            }
            return result;
        }
        if (datum instanceof Map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) datum).entrySet()) {
                result.put(e.getKey().toString(), inferFromContainer(
                        e.getValue(), legacyJsonParityEnabled));
            }
            return result;
        }
        return datum instanceof CharSequence ? datum.toString() : datum;
    }
}
