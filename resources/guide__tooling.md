# Demand Planning Agent - Tooling Guide

## Philosophy: Unix-like Tool Composition

This demand planning system follows the Unix philosophy of building small, focused tools that do one thing well and can be composed together to create powerful workflows. Each tool is designed to be:

- **Single-purpose**: Each tool has a clear, focused responsibility
- **Composable**: Tools can be combined to create complex workflows
- **Predictable**: Consistent interfaces and behavior across tools
- **Extensible**: New tools can be added without changing existing ones

## Core Tools


### 1. `tool__workstation` - Session Management
**Purpose**: Centralized Spark session orchestration and lifecycle management

**Key Functions**:
- `get_spark(config_preset)` - Get/create Spark session
- `is_spark_active()` - Check session status
- `stop_spark()` / `restart_spark()` - Session lifecycle
- `spark_health_check()` - Session diagnostics

**Usage Pattern**:
```python
from tool__workstation import get_spark
spark = get_spark("local_delta")  # Creates session with Delta Lake
```

**Design Principle**: Single source of truth for Spark configuration across all workflows.

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

### 4. `tool__table_indexer` - Entity Indexing with Persistence
**Purpose**: Persistent, consistent indexing for entities with Delta table management

**Key Functions**:
- `TableIndexer(df_entities, catalog, schema)` - Create indexer instance
- `.customer(column_name)` - Index customer entities
- `.plant(column_name)` - Index plant entities
- `.material(column_name)` - Index material entities

**Key Features**:
- Persistent index mapping using Delta tables
- Race condition safe MERGE operations
- Automatic catalog/schema creation
- Consecutive index assignment preserving existing mappings

**Usage Pattern**:
```python
from tool__table_indexer import TableIndexer
indexer = TableIndexer(df_clean)
df_indexed = indexer.customer("customer_name")
```

**Index Column Naming**:
- Input: `customer_name` → Output: `Index__customer_name`
- Input: `FK__plant` → Output: `Index__plant`
- Input: `material_code` → Output: `Index__material_code`

**Delta Table Structure**:
- Tables: `{catalog}.{schema}.mapping__active_{entity}s`
- Default: `test_catalog.supply_chain.mapping__active_customers`
- Columns: `index` (LONG), `{entity}` (STRING)

**Design Principle**: "Like assigning jersey numbers to players" - consistent, persistent entity identification across all workflows.

## Tool Composition Patterns

### Pattern 1: Linear Pipeline
Sequential tool application for data processing:

```python
# Initialize
spark = get_spark("local_delta")
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
chain.dag__indexed_customers = indexer.customer("customer_name")
chain.dag__indexed_materials = indexer.material("material_code")

# Stage 4: Business logic with indexed entities
chain.dag__enriched = (
    chain.dag__indexed_materials
    .withColumn("demand_category", F.when(F.col("demand_qty") > 1000, "high").otherwise("normal"))
)

# Stage 5: Output
chain.write_to_path("gold/enriched_demand", -1)  # Write latest (-1 index)
```

### Pattern 3: Parallel Processing with Shared Entity Indexing
Multiple chains for different data streams with consistent entity indexing:

```python
# Separate chains for different data sources
chain__demand = DagChain()
chain__supply = DagChain()
chain__forecast = DagChain()

# Process each stream with standardization
chain__demand.dag__clean = polish(spark.read.csv("demand.csv"))
chain__supply.dag__clean = polish(spark.read.csv("supply.csv"))

# Apply consistent entity indexing across streams
indexer_demand = TableIndexer(chain__demand.dag__clean)
indexer_supply = TableIndexer(chain__supply.dag__clean)

# Both use the same persistent mapping tables
chain__demand.dag__indexed = indexer_demand.customer("customer_name")
chain__supply.dag__indexed = indexer_supply.customer("customer_name")

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

### Simple Ingestion Workflow
```python
from tool__workstation import get_spark
from tool__dag_chainer import DagChain
from tool__table_polisher import polish

# Initialize
spark = get_spark("local_delta")
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