# OBSOLETE: Legacy Issue Documentation

## 🎯 Issue No Longer Applicable
This issue related to the old persistent TableIndexer implementation with Delta table storage. **The current TableIndexer is completely stateless and this error can no longer occur.**

## ✅ Current TableIndexer (v2.0+)

### **Stateless Design**
The TableIndexer now operates entirely in-memory with no persistent storage:

```python
# CURRENT: Stateless, no catalog/schema parameters
from tool__table_indexer import TableIndexer

indexer = TableIndexer(df)  # Simple constructor, no persistence
result = indexer.customer("customer_name")
mapping_df = result["mapping"]         # In-memory mapping
indexed_df = result["focal_indexed"]   # DataFrame with index columns
```

### **No More Storage Issues**
- **No Delta tables**: All operations are in-memory
- **No catalog management**: No persistent storage means no metastore dependencies
- **No Unity Catalog concerns**: Works identically everywhere
- **Polish-compatible**: Lowercase naming aligns with standardization tools

## 🔧 Current Usage (v2.0+)

### **Stateless Entity Indexing**
```python
from tool__table_indexer import TableIndexer
from tool__table_polisher import polish

# Polish-compatible workflow
polished_df = polish(raw_df)
indexer = TableIndexer(polished_df)

# Basic indexing (returns dict with mapping and indexed DataFrame)
customer_result = indexer.customer("keyp__customer")
plant_result = indexer.plant("plant_location")
material_result = indexer.material("material_code")

# Access results
customer_mapping = customer_result["mapping"]      # [customer, customer_index]
final_df = indexer.indexed_df                      # Original DF + all index__ columns
```

### **Advanced Features**
```python
# Controlled mapping expansion
result = indexer.customer("customer_code",
                         existing_mapping_df=active_customers,
                         append_new_entities=False)  # Only index existing

# Conflict-free batch saves
for kind, mapping in {
    "customer": customer_result["mapping"],
    "plant": plant_result["mapping"]
}.items():
    mapping.write.format("delta").mode("overwrite").saveAsTable(f"catalog.schema.map__{kind}s")
```

## 🏗️ Key Improvements (v2.0+)

1. **Stateless Operation**: No persistent storage, all in-memory
2. **Polish Integration**: Lowercase `index__` prefix aligns with standardization
3. **Leading Zero Normalization**: Automatic cleanup while preserving single '0'
4. **Schema Conflict Prevention**: Entity-specific mapping columns (`customer_index`, `plant_index`)
5. **Append Control**: `append_new_entities` parameter prevents inactive entity indexing
6. **Idempotent Operations**: Running multiple times produces consistent results
7. **Explicit Data Types**: All indices consistently cast to `long`

## 📊 Benefits

- ✅ **No storage dependencies**: Works identically in all environments
- ✅ **No metastore issues**: No catalog or schema management required
- ✅ **Polish compatibility**: Seamless integration with standardization workflow
- ✅ **Performance**: In-memory operations, no Delta table overhead
- ✅ **Reliability**: No race conditions or MERGE operation complexity

## 📚 Documentation

See the updated guides:
- **[Tooling Guide](resources/guide__tooling.md)** - Complete TableIndexer documentation
- **[CLAUDE.md](CLAUDE.md)** - Usage patterns and examples

The legacy storage-based TableIndexer and its associated issues are now completely obsolete! 🎉
