# Demand Planning Agent - Tooling Guide

## Philosophy: Unix-like Tool Composition

This demand planning system follows the Unix philosophy of building small, focused tools that do one thing well and can be composed together to create powerful workflows. Each tool is designed to be:

- **Single-purpose**: Each tool has a clear, focused responsibility
- **Composable**: Tools can be combined to create complex workflows
- **Predictable**: Consistent interfaces and behavior across tools
- **Extensible**: New tools can be added without changing existing ones

## Core Tools


### 1. `tool__workstation` - Universal Session Management
**Purpose**: Centralized Spark session orchestration with full environment compatibility

**Key Functions**:
- `get_spark(config_preset="auto")` - Get/create Spark session with auto-detection
- `is_spark_active()` - Environment-aware session status check
- `spark_health_check()` - Comprehensive session diagnostics
- `inject_spark_for_databricks()` - Manual Databricks session injection

**Environment Support**:
- **Auto-detection**: Automatically detects local vs Databricks environments
- **Databricks compatibility**: Uses existing sessions without interference
- **Java auto-setup**: Finds and configures Java 17+ for local development
- **Session safety**: Prevents stopping managed Databricks sessions

**Usage Pattern**:
```python
from tool__workstation import get_spark
spark = get_spark("auto")  # Auto-detects environment
spark = get_spark("local_delta")  # Force local with Delta Lake
```

**Design Principle**: Universal session management that works seamlessly across all Spark environments.

### 2. `tool__dag_chainer` - DataFrame Workflow Management
**Purpose**: Chain and manage DataFrames in workflows with inspection capabilities

**Key Functions**:
- `DagChain()` - Create workflow chain container
- `.dag__<name> = df` - Add DataFrames to chain
- `.trace(shape=True)` - View all DataFrames in chain
- `.look(idx)` - Inspect specific DataFrame
- `.pick(idx)` - Get DataFrame by index
- `.write_to_path(path, idx)` - Write DataFrame to Delta

**Usage Pattern**:
```python
from tool__dag_chainer import DagChain
chain = DagChain()
chain.dag__raw_data = df_source
chain.dag__clean_data = process(chain.dag__raw_data)
chain.look()  # Inspect latest DataFrame
```

**Design Principle**: Provides workflow orchestration while maintaining visibility into data transformations.

### 3. `tool__table_polisher` - Data Standardization
**Purpose**: Consistent DataFrame standardization for medallion architecture

**Key Functions**:
- `polish(df)` - Complete DataFrame standardization

**Standardization Rules**:
- Column names: lowercase, special chars → underscores
- Key columns (keyP__/keyF__): trim, lowercase, strip zeros, null → "na"
- Column ordering: key columns, *_code columns, others (alphabetical within groups)

**Usage Pattern**:
```python
from tool__table_polisher import polish
df_standardized = polish(df_raw)
```

**Design Principle**: Ensures consistent data format across all pipeline stages.

### 4. `tool__table_indexer` - Stateless Entity Indexing
**Purpose**: Stateless entity indexing for dimensional modeling with Polish compatibility

**Key Functions**:
- `TableIndexer(df_entities)` - Create stateless indexer instance
- `.customer(zsource_col, customer_code_col, existing_mapping_df=None, append_new_entities=True)` - Index customers with composite key
- `.plant(zsource_col, plant_code_col, existing_mapping_df=None, append_new_entities=True)` - Index plants with composite key
- `.material(zsource_col, material_code_col, existing_mapping_df=None, append_new_entities=True)` - Index materials with composite key
- `.index(source_entity_cols, entity_kind, existing_mapping_df=None, append_new_entities=True)` - Generic indexing

**Key Features**:
- **Four-layer architecture**: `base_df` (original), `indexed_df` (progressive), `filtered_indexed_df` (quality-assured), `focal_indexed` (snapshot)
- **Stateless operation**: No persistent storage, uses in-memory mappings
- **Composite key support**: Required zsource + entity_code parameters for proper hierarchy
- **Leading zero normalization**: Strips leading zeros while preserving single '0'
- **Polish compatibility**: Lowercase `index__` prefix aligns with Polish conventions
- **Append control**: `append_new_entities=False` prevents inactive entity indexing
- **Entity-specific mappings**: `customer_index`, `plant_index`, etc. prevent schema conflicts
- **Column-concatenation naming**: Index columns show all source columns used (e.g., `index__zsource_customer_code`)
- **Quality assurance**: `filtered_indexed_df` automatically filters out rows with NULL indices
- **Idempotent operations**: Running multiple times produces consistent results
- **Explicit data types**: All indices consistently cast to `long`

**Usage Patterns**:
```python
from tool__table_indexer import TableIndexer
from tool__table_polisher import polish

# Polish-compatible workflow
polished_df = polish(raw_df)  # Standardize first
indexer = TableIndexer(polished_df)

# Basic indexing with composite key (required)
result = indexer.customer("zsource", "customer_code")
mapping_df = result["mapping"]        # [customer, customer_index]
indexed_df = result["focal_indexed"]  # Original DF + index__zsource_customer_code

# Controlled expansion (dimension tables)
result = indexer.customer("zsource", "customer_code",
                         existing_mapping_df=active_customers,
                         append_new_entities=False)  # Only index existing

# Access different views
complete_data = indexer.indexed_df          # All rows (may contain NULLs)
clean_data = indexer.filtered_indexed_df    # Only fully-indexed rows (quality-assured)
```

**Index Column Naming** (Polish-compatible):
- Single column: `customer_name` → Output: `index__customer_name`
- Composite key: `zsource, customer_code` → Output: `index__zsource_customer_code`
- All source columns concatenated: shows exactly which columns formed the composite key

**Mapping Output Schema**:
- Customer mappings: `[customer, customer_index]`
- Plant mappings: `[plant, plant_index]`
- Material mappings: `[material, material_index]`
- Enables conflict-free batch saves to Delta tables

**Design Principle**: Stateless, composable entity indexing that integrates seamlessly with Polish standardization and prevents common pitfalls like schema conflicts and duplicate columns. The four-layer architecture separates concerns: data preservation (`base_df`), progressive indexing (`indexed_df`), quality assurance (`filtered_indexed_df`), and operation snapshots (`focal_indexed`).

## Tool Composition Patterns

### Pattern 1: Universal Linear Pipeline
Sequential tool application that works everywhere:

```python
# Initialize - auto-detects local vs Databricks
spark = get_spark("auto")
chain = DagChain()

# Compose: Import → Standardize → Chain → Write
chain.dag__gold_data = polish(
    spark.read.option("header", "true").csv("data.csv")
)
chain.write_to_path("gold/output", 0)
```

### Pattern 2: Multi-Stage Processing with Indexing
Using chain to manage complex transformations with entity indexing:

```python
chain = DagChain()

# Stage 1: Raw import
chain.dag__raw = spark.read.csv("source.csv")

# Stage 2: Standardization
chain.dag__standardized = polish(chain.dag__raw)

# Stage 3: Entity indexing
indexer = TableIndexer(chain.dag__standardized)
customer_result = indexer.customer("customer_name")
material_result = indexer.material("material_code")
chain.dag__with_indices = indexer.indexed_df  # Contains all index columns

# Stage 4: Business logic with indexed entities
chain.dag__enriched = (
    chain.dag__with_indices
    .withColumn("demand_category", F.when(F.col("demand_qty") > 1000, "high").otherwise("normal"))
)

# Stage 5: Output
chain.write_to_path("gold/enriched_demand", -1)  # Write latest (-1 index)
```

### Pattern 3: Parallel Processing with Shared Entity Mappings
Multiple chains with consistent entity indexing using shared mappings:

```python
# Separate chains for different data sources
chain__demand = DagChain()
chain__supply = DagChain()
chain__forecast = DagChain()

# Process each stream with standardization
chain__demand.dag__clean = polish(spark.read.csv("demand.csv"))
chain__supply.dag__clean = polish(spark.read.csv("supply.csv"))

# Create master entity mappings from primary data source
master_indexer = TableIndexer(chain__demand.dag__clean)
customer_mapping = master_indexer.customer("zsource", "customer_code")["mapping"]
plant_mapping = master_indexer.plant("zsource", "plant_code")["mapping"]

# Apply consistent indexing to both streams using shared mappings
demand_indexer = TableIndexer(chain__demand.dag__clean)
demand_result = demand_indexer.customer("zsource", "customer_code", existing_mapping_df=customer_mapping)
chain__demand.dag__indexed = demand_indexer.filtered_indexed_df  # Quality-assured data only

supply_indexer = TableIndexer(chain__supply.dag__clean)
supply_result = supply_indexer.customer("zsource", "customer_code", existing_mapping_df=customer_mapping)
chain__supply.dag__indexed = supply_indexer.filtered_indexed_df  # Quality-assured data only

# Save mappings for reuse (conflict-free batch save)
for kind, mapping in {
    "customer": customer_mapping,
    "plant": plant_mapping
}.items():
    mapping.write.format("delta").mode("overwrite").saveAsTable(f"catalog.schema.map__{kind}s")

# Combine streams with consistent entity indices
combined_df = chain__demand.pick(-1).union(chain__supply.pick(-1))
chain__forecast.dag__combined = combined_df
```

## Naming Conventions

### Tool Names
- Format: `tool__<purpose>`
- Examples: `tool__workstation`, `tool__dag_chainer`, `tool__table_polisher`

### Chain Names
- Format: `chain__<domain>_<layer>`
- Examples: `chain__gold_demand`, `chain__silver_inventory`, `chain__bronze_raw`

### DAG Names (within chains)
- Format: `dag__<action>_<description>`
- Examples: `dag__import_demand_from_csv`, `dag__enrich_with_forecast`, `dag__aggregate_monthly`

### File Paths
- Format: `resources/<layer>/<domain>_<description>`
- Examples: `resources/gold/demand_data`, `resources/silver/inventory_cleaned`

## Workflow Examples

### Universal Ingestion Workflow
```python
from tool__workstation import get_spark
from tool__dag_chainer import DagChain
from tool__table_polisher import polish

# Initialize - works everywhere
spark = get_spark("auto")
chain__gold_demand = DagChain()

# Import and standardize
chain__gold_demand.dag__import_csv = polish(
    spark.read.option("header", "true").csv("data.csv")
)

# Inspect
chain__gold_demand.look()

# Write to gold
chain__gold_demand.write_to_path("resources/gold/demand_data", 0)
```

### Complex Transformation Workflow with Entity Indexing
```python
# Multi-stage transformation with persistent entity indexing
chain__processing = DagChain()

# Stage 1: Import multiple sources
chain__processing.dag__raw_demand = spark.read.csv("demand.csv")
chain__processing.dag__raw_supply = spark.read.csv("supply.csv")

# Stage 2: Standardize
chain__processing.dag__clean_demand = polish(chain__processing.dag__raw_demand)
chain__processing.dag__clean_supply = polish(chain__processing.dag__raw_supply)

# Stage 3: Apply entity indexing for consistent joining
indexer_demand = TableIndexer(chain__processing.dag__clean_demand)
indexer_supply = TableIndexer(chain__processing.dag__clean_supply)

chain__processing.dag__indexed_demand = indexer_demand.customer("customer_code")
chain__processing.dag__indexed_supply = indexer_supply.customer("customer_code")

# Stage 4: Join on indexed entities (more efficient)
demand_df = chain__processing.dag__indexed_demand
supply_df = chain__processing.dag__indexed_supply
chain__processing.dag__joined = demand_df.join(supply_df, "Index__customer_code")

# Stage 5: Business calculations
chain__processing.dag__calculated = (
    chain__processing.dag__joined
    .withColumn("variance", F.col("actual") - F.col("forecast"))
    .withColumn("accuracy", F.abs(F.col("variance")) / F.col("forecast"))
)

# Inspect and write
chain__processing.trace(shape=True)
chain__processing.write_to_path("resources/gold/demand_analysis", -1)
```

## Agent Guidelines

### For Human Developers
1. **Start with workstation**: Always initialize session management first
2. **Use chains for visibility**: Chain DataFrames to maintain workflow visibility
3. **Polish early**: Apply standardization as early as possible in pipelines
4. **Name descriptively**: Use clear, semantic names for chains and DAGs
5. **Inspect frequently**: Use `.look()` and `.trace()` to verify transformations

### For AI Agents
1. **Import pattern**: Always start with tool imports and session initialization
2. **Chain assignment**: Use semantic naming for chain variables and DAG attributes
3. **Composition over complexity**: Prefer combining simple tools over complex single functions
4. **Error handling**: Tools provide consistent error handling and session management
5. **Output verification**: Use inspection methods to validate transformations

## Extending the Toolset

### Adding New Tools
1. **Follow naming**: `tool__<purpose>.py`
2. **Single responsibility**: Focus on one clear function
3. **Consistent interface**: Similar parameter patterns and return types
4. **Integration ready**: Work with existing session management and chains
5. **Documentation**: Include usage examples and composition patterns

### Tool Integration Checklist
- [ ] Uses workstation for session management (if needed)
- [ ] Returns DataFrame or compatible types for chaining
- [ ] Handles errors gracefully
- [ ] Follows naming conventions
- [ ] Includes usage examples
- [ ] Works in both local and Databricks environments

## Best Practices

### Performance
- **Session reuse**: Use workstation to avoid session overhead
- **Lazy evaluation**: Leverage Spark's lazy evaluation through chains
- **Resource management**: Clean up sessions when workflows complete

### Debugging
- **Use inspection**: `.look()`, `.trace()`, and health checks for debugging
- **Chain visibility**: Keep intermediate steps in chains for troubleshooting
- **Consistent logging**: Tools provide consistent error reporting

### Maintainability
- **Modular design**: Keep tools focused and chains organized
- **Semantic naming**: Use descriptive names for workflows and transformations
- **Documentation**: Comment complex compositions and business logic

## Tool Reference Quick Guide

| Tool | Primary Use | Key Method | Output |
|------|-------------|------------|---------|
| `workstation` | Session management | `get_spark()` | SparkSession |
| `dag_chainer` | Workflow orchestration | `DagChain()` | Chain container |
| `table_polisher` | Data standardization | `polish()` | Standardized DataFrame |
| `table_indexer` | Entity indexing | `TableIndexer()` | DataFrame with index columns |

| Chain Method | Purpose | Example |
|--------------|---------|---------|
| `.dag__name = df` | Add DataFrame | `chain.dag__clean = polish(df)` |
| `.look(idx)` | Inspect data | `chain.look()` # latest |
| `.trace(shape=True)` | View all | `chain.trace(True)` # with counts |
| `.pick(idx)` | Get DataFrame | `df = chain.pick(0)` # first |
| `.write_to_path()` | Save Delta | `chain.write_to_path("gold/data", -1)` |

---

*This guide enables both human developers and AI agents to effectively use and compose tools for demand planning workflows.*