package com.maxsakharov.kafka.transformers;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MergeTransformerTest {

    @Test
    public void shouldMergeValuesWithSchema() {
         MergeTransformer<SinkRecord> transformer = new MergeTransformer<>();

         Schema keySchema = SchemaBuilder.string();

         Schema valSchema = SchemaBuilder.struct();
         ((SchemaBuilder) valSchema).field("a", Schema.FLOAT64_SCHEMA);
         ((SchemaBuilder) valSchema).field("b", Schema.FLOAT64_SCHEMA);

         Struct value = new Struct(valSchema);
         value.put("a", 34.101);
         value.put("b", -118.343);

         SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

         Map<String, String> config = new HashMap<>();
         config.put("fields", "a,b");
         config.put("merged.name", "merged_a_b");

         transformer.configure(config);
         SinkRecord result = transformer.apply(record);

         Struct resStruct = (Struct) result.value();

         assertEquals(34.101, resStruct.get("a"));
         assertEquals(-118.343, resStruct.get("b"));
         assertEquals("34.101,-118.343", resStruct.get("merged_a_b"));
   }

   @Test
   public void shouldMergeValuesWithSchemaAndNestedFields() {
      MergeTransformer<SinkRecord> transformer = new MergeTransformer<>();

      Schema keySchema = SchemaBuilder.string();


      Schema nestedSchema = SchemaBuilder.struct();
      ((SchemaBuilder) nestedSchema).field("a", Schema.FLOAT64_SCHEMA);
      ((SchemaBuilder) nestedSchema).field("b", Schema.FLOAT64_SCHEMA);

      Schema valSchema = SchemaBuilder.struct();
      ((SchemaBuilder) valSchema).field("nested", nestedSchema);
      ((SchemaBuilder) valSchema).field("c", Schema.STRING_SCHEMA);


      Struct nested = new Struct(nestedSchema);
      nested.put("a", 34.101);
      nested.put("b", -118.343);

      Struct value = new Struct(valSchema);
      value.put("nested", nested);
      value.put("c", "c_val");

      SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

      Map<String, String> config = new HashMap<>();
      config.put("fields", "nested.a,nested.b");
      config.put("merged.name", "merged_a_b");

      transformer.configure(config);
      SinkRecord result = transformer.apply(record);

      Struct resStruct = (Struct) result.value();

      assertEquals(34.101, (((Struct)resStruct.get("nested")).get("a")));
      assertEquals(-118.343, (((Struct)resStruct.get("nested")).get("b")));
      assertEquals("c_val", resStruct.get("c"));
      assertEquals("34.101,-118.343", resStruct.get("merged_a_b"));
   }

    @Test
    public void shouldMergeNullValueWithSchema() {
        MergeTransformer<SinkRecord> transformer = new MergeTransformer<>();

        Schema keySchema = SchemaBuilder.string();

        Schema valSchema = SchemaBuilder.struct();
        ((SchemaBuilder) valSchema).field("a", Schema.OPTIONAL_FLOAT64_SCHEMA);
        ((SchemaBuilder) valSchema).field("b", Schema.FLOAT64_SCHEMA);

        Struct value = new Struct(valSchema);
        value.put("a", null);
        value.put("b", 123.01);

        SinkRecord record = new SinkRecord("test", 1, keySchema, "key", valSchema, value, 100);

        Map<String, String> config = new HashMap<>();
        config.put("fields", "a,b");
        config.put("merged.name", "merged_a_b");

        transformer.configure(config);
        SinkRecord result = transformer.apply(record);

        Struct resStruct = (Struct) result.value();

        assertEquals(null, resStruct.get("a"));
        assertEquals(123.01, resStruct.get("b"));
        assertEquals("123.01", resStruct.get("merged_a_b"));
    }

   @Test
   public void shouldMergeValuesWithMap() {
      MergeTransformer<SinkRecord> transformer = new MergeTransformer<>();

      Map<String, Object> value = new HashMap<>();
      value.put("a", 34.101);
      value.put("b", -118.343);

      SinkRecord record = new SinkRecord("test", 1, null, "key", null, value, 100);

      Map<String, String> config = new HashMap<>();
      config.put("fields", "a,b");
      config.put("merged.name", "merged_a_b");

      transformer.configure(config);
      SinkRecord result = transformer.apply(record);

      Map<String, Object> res = (Map<String, Object>) result.value();

      assertEquals(34.101, res.get("a"));
      assertEquals(-118.343, res.get("b"));
      assertEquals("34.101,-118.343", res.get("merged_a_b"));
   }

   @Test
   public void shouldMergeValuesWithMapAndNestedFields() {

      MergeTransformer<SinkRecord> transformer = new MergeTransformer<>();

      Map<String, Object> value = new HashMap<>();

      Map<String, Object> nested = new HashMap<>();
      nested.put("a", 34.101);
      nested.put("b", -118.343);

      value.put("nested", nested);
      value.put("c", "c_val");

      SinkRecord record = new SinkRecord("test", 1, null, "key", null, value, 100);

      Map<String, String> config = new HashMap<>();
      config.put("fields", "nested.a,nested.b");
      config.put("merged.name", "merged_a_b");

      transformer.configure(config);
      SinkRecord result = transformer.apply(record);

      Map<String, Object> res = (Map<String, Object>) result.value();

      assertEquals(34.101, (((Map<String, Object>)res.get("nested")).get("a")));
      assertEquals(-118.343, (((Map<String, Object>)res.get("nested")).get("b")));
      assertEquals("c_val", res.get("c"));
      assertEquals("34.101,-118.343", res.get("merged_a_b"));
   }
}