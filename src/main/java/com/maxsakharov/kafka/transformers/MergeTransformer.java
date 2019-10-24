package com.maxsakharov.kafka.transformers;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MergeTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELDS_CONFIG = "fields";
    private static final String FIELDS_DOC = "Fields to merge";
    private static final String MERGED_FIELD_NAME_CONFIG = "merged.name";
    private static final String MERGED_FIELD_NAME_DOC = "Fields are merge and put into field with this name";
    private static final String MERGED_FIELD_SEPARATOR_CONFIG = "merged.separator";
    private static final String MERGED_FIELD_SEPARATOR_DOC = "Merged fieldsToMerge will be separated by this value";

    private Set<String> fieldsToMerge;
    private String mergedField;
    private String separator;
    private Cache<Schema, Schema> schemaUpdateCache;


    public ConfigDef config() {
        return new ConfigDef()
            .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, FIELDS_DOC)
            .define(MERGED_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, MERGED_FIELD_NAME_DOC)
            .define(MERGED_FIELD_SEPARATOR_CONFIG, ConfigDef.Type.STRING, ",", ConfigDef.Importance.LOW, MERGED_FIELD_SEPARATOR_DOC);
    }

    public void configure(Map<String, ?> configs) {
        SimpleConfig config = new SimpleConfig(config(), configs);
        this.fieldsToMerge = new HashSet<>(config.getList(FIELDS_CONFIG));
        this.mergedField = config.getString(MERGED_FIELD_NAME_CONFIG);
        this.separator = config.getString(MERGED_FIELD_SEPARATOR_CONFIG);
        this.schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    public R apply(R record) {
        return record.valueSchema() == null ? this.applySchemaless(record) : this.applyWithSchema(record);
    }

    private R applySchemaless(R record) {
        Map<String, Object> value = Requirements.requireMap(record.value(), "field merge");
        Map<String, Object> updatedValue = new HashMap<>(value);

        List<String> values = new ArrayList<>();
        populateValues(values, value, null);
        String mergedFieldVal = values.stream().collect(Collectors.joining(separator));

        updatedValue.put(mergedField, mergedFieldVal);

        return this.newRecord(record, null, updatedValue);
    }

    private void populateValues(List<String> values, Map<String, Object> fields, String prefix) {
        for (String fieldName : fields.keySet()) {
            String currPrefix = (prefix == null ? "" : prefix + ".") + fieldName;
            if (fields.get(fieldName) instanceof Map) {
                populateValues(values, (Map<String, Object>) fields.get(fieldName), currPrefix);
            } else if (fieldsToMerge.contains(currPrefix)) {
                values.add(String.valueOf(fields.get(fieldName)));
            }
        }
    }

    private R applyWithSchema(R record) {
        Struct value = Requirements.requireStruct(record.value(), "field merge");
        org.apache.kafka.connect.data.Schema updatedSchema = this.schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = this.makeUpdatedSchema(value.schema(), mergedField);
            this.schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        Struct updatedValue = new Struct(updatedSchema);

        for (Field field : record.valueSchema().fields()) {
            Object fieldValue = value.get(field.name());
            updatedValue.put(field.name(), fieldValue);
        }

        List<String> values = new ArrayList<>();
        populateValues(values, record.valueSchema().fields(), updatedValue, null);
        String mergedFieldVal = values.stream().collect(Collectors.joining(separator));

        updatedValue.put(mergedField, strip(mergedFieldVal));

        return this.newRecord(record, updatedSchema, updatedValue);
    }

    private void populateValues(List<String> values, List<Field> fields, Struct value, String prefix) {
        for (Field field : fields) {
            String currPrefix = (prefix == null ? "" : prefix + ".") + field.name();
            if (field.schema().type() == SchemaBuilder.struct().type()) {
                populateValues(values, field.schema().fields(), (Struct) value.get(field.name()), currPrefix);
            } else if (fieldsToMerge.contains(currPrefix)) {
                values.add(String.valueOf(value.get(field.name())));
            }
        }
    }

    private org.apache.kafka.connect.data.Schema makeUpdatedSchema(org.apache.kafka.connect.data.Schema schema, String newField) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        builder.field(newField, Schema.STRING_SCHEMA);

        return builder.build();
    }

    public void close() {
        this.schemaUpdateCache = null;
    }

    private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

    private String strip(String val) {
        if (val.endsWith(separator))
            val = val.substring(0, val.length() - separator.length());
        return val;
    }

}
