### kafka-connect-merge-transformer

Merge several fields into the new one

### Examples

##### Simple merge

```
"transforms": "merge",
"transforms.merge.type": "com.maxsakharov.kafka.transformers.MergeTransformer",
"transforms.merge.fields": "foo,bar",
"transforms.merge.merged.name": "foo_bar_merge"
```

As result of the transformation new field will be created `foo_bar_merge=foo,var` and added to the resulting message

##### Nested fields merge

```
"transforms": "merge",
"transforms.merge.type": "com.maxsakharov.kafka.transformers.MergeTransformer",
"transforms.merge.fields": "nested.foo,nested.bar",
"transforms.merge.merged.name": "foo_bar_merge"
```

As result of the transformation nested fields will be merged into new field `foo_bar_merge=foo,var` and added to the resulting message