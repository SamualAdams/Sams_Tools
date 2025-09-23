# Case Study: Tools for Order Data Engineering

This case study shows how a modular toolset—**DagChain**, **Table Polisher**, **Table Indexer**, and **Databricks DQEngine**—turns raw order data into a clean, indexed, QA-checked **gold** table in PySpark + Databricks.

---

## Setup & Imports

> Assumes you’re in Databricks with a default `spark` session.

```python
# Optional: if needed in your workspace
# %pip install databricks-labs-dqx databricks-sdk

from pyspark.sql import functions as F

from shared.utils.import_entity_from_blob import import_entity_from_blob
from shared.tool__table_polisher import polish
from shared.tool__dag_chainer import DagChain
from shared.tool__table_indexer import TableIndexer

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# spark: provided by Databricks notebooks
```

---

## 1) Ingest Raw Orders

We declare the first node of our DAG: ingesting raw orders from blob storage and dropping non-analytic metadata.

```python
chain__orders = DagChain()

chain__orders.dag__import_raw_orders = (
    import_entity_from_blob(target_path="fact", target_name="orders_raw")
    .drop("@odata.etag", "ItemInternalId")
)
```

---

## 2) Standardize Column Names

Rename ERP-style fields to business-friendly names using **Table Polisher**.

```python
chain__orders.dag__rename_columns = polish(
    chain__orders.pick(-1).withColumnsRenamed({
        "DOCNUM": "Reference_Document_Code",
        "ITEMNUM": "Reference_Document_Item_Code",
        "MATERIAL_CODE": "Material_Code",
        "DELIVERY_DATE": "Confirmed_Delivery_Date",
        "PLANNED_DATE": "Planned_Goods_Issue_Date",
        "ACTUAL_DATE": "Actual_Goods_Issue_Date",
        "ENTRY_DATE": "Order_Entry_Date",
        "REQ_DELIV_DATE": "Requested_Delivery_Date",
        "CUSTOMER_CODE": "Customer_Code",
        "PLANT_CODE": "Plant_Code",
        "SOURCE_SYS": "source_system",
    })
)
```

---

## 3) Index Core Entities

Create stable indices for **customer**, **material**, and **plant** with **Table Indexer**, referencing canonical mappings.

```python
indexer = TableIndexer(chain__orders.pick(-1))

customer__index = indexer.customer(
    zsource_col="source_system",
    customer_code_col="customer_code",
    existing_mapping_df=spark.table("demo_catalog.masterdata.customer_mapping"),
    append_new_entities=True,
)

material__index = indexer.material(
    zsource_col="source_system",
    material_code_col="material_code",
    existing_mapping_df=spark.table("demo_catalog.masterdata.material_mapping"),
    append_new_entities=True,
)

plant__index = indexer.plant(
    zsource_col="source_system",
    plant_code_col="plant_code",
    existing_mapping_df=spark.table("demo_catalog.masterdata.plant_mapping"),
    append_new_entities=True,
)
```

Persist/refresh the active mapping tables:

```python
for kind, mapping in {
    "customer": customer__index["mapping"],
    "material": material__index["mapping"],
    "plant": plant__index["mapping"],
}.items():
    (mapping.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"demo_catalog.masterdata.{kind}_mapping"))
```

---

## 4) Keep Only Fully Indexed Rows

Block partials from leaking downstream by using `filtered_indexed_df`, then normalize index column names.

```python
chain__orders.dag__indexed = (
    indexer.filtered_indexed_df.withColumnsRenamed({
        "index__source_system_customer_code": "index__customer",
        "index__source_system_material_code": "index__material",
        "index__source_system_plant_code": "index__plant",
    })
)
```

---

## 5) Add Derived Business Dates

Compute **Latest\_Valid\_Date** using a clear precedence and a numeric day offset for convenience.

```python
chain__orders.dag__add_lv_date = (
    chain__orders.pick(-1).withColumns({
        "Latest_Valid_Date": F.coalesce(
            F.col("Actual_Goods_Issue_Date"),
            F.col("Planned_Goods_Issue_Date"),
            F.col("Confirmed_Delivery_Date"),
            F.col("Requested_Delivery_Date"),
        ),
        "LV_Date_Days": F.datediff(F.to_date(F.col("Latest_Valid_Date")), F.lit("1970-01-01")),
    })
)
```

---

## 6) Apply YAML-Driven Data Quality

Run **Databricks DQEngine** checks loaded from a workspace YAML file.

```python
dq_engine = DQEngine(WorkspaceClient())

checks = dq_engine.load_checks_from_workspace_file(
    workspace_path="/Workspace/demo_user/data_engineering/orders_quality_checks.yml"
)

chain__orders.dag__qa_checked = dq_engine.apply_checks_by_metadata(
    chain__orders.pick(), checks
)
```

---

## 7) Publish the Gold Table

Write the final, clean, indexed, QA-checked dataset to your analytics schema.

```python
(chain__orders.pick(-1)
    .write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("demo_catalog.analytics.orders_gold"))
```

---

## Why This Works (and Scales)

* **DagChain** = composable steps and easy inspection.
* **Polisher** = consistent semantics across sources.
* **Indexer** = universal entity keys → reliable joins/reporting.
* **Filtered rows** = no half-indexed junk sneaking into gold.
* **DQEngine** = auditable, declarative quality gates.

---

## YAML Companion Spec

Below is a YAML representation of the pipeline.
It mirrors the Markdown/code walkthrough, but in a declarative form that an agent can parse, modify, or regenerate PySpark from.

```yaml
pipeline:
  name: orders_gold_pipeline
  catalog: demo_catalog
  schema: analytics

  steps:
    - id: ingest_raw
      description: Ingest raw orders from blob storage
      tool: import_entity_from_blob
      params:
        target_path: fact
        target_name: orders_raw
      drops:
        - "@odata.etag"
        - "ItemInternalId"

    - id: rename_columns
      description: Standardize ERP-style codes into business-friendly names
      tool: table_polisher
      mappings:
        DOCNUM: Reference_Document_Code
        ITEMNUM: Reference_Document_Item_Code
        MATERIAL_CODE: Material_Code
        DELIVERY_DATE: Confirmed_Delivery_Date
        PLANNED_DATE: Planned_Goods_Issue_Date
        ACTUAL_DATE: Actual_Goods_Issue_Date
        ENTRY_DATE: Order_Entry_Date
        REQ_DELIV_DATE: Requested_Delivery_Date
        CUSTOMER_CODE: Customer_Code
        PLANT_CODE: Plant_Code
        SOURCE_SYS: source_system

    - id: index_entities
      description: Create stable indices for core entities
      tool: table_indexer
      entities:
        customer:
          source_column: customer_code
          mapping_table: masterdata.customer_mapping
        material:
          source_column: material_code
          mapping_table: masterdata.material_mapping
        plant:
          source_column: plant_code
          mapping_table: masterdata.plant_mapping

    - id: filter_indexed
      description: Keep only fully indexed rows and normalize index names
      tool: table_indexer.filtered_indexed_df

    - id: derive_lv_date
      description: Compute latest valid delivery date
      logic: coalesce(Actual_Goods_Issue_Date, Planned_Goods_Issue_Date, Confirmed_Delivery_Date, Requested_Delivery_Date)

    - id: quality_checks
      description: Apply YAML-driven quality checks
      tool: dqengine
      checks_file: /Workspace/demo_user/data_engineering/orders_quality_checks.yml

    - id: publish
      description: Write out the gold table for analytics
      output_table: analytics.orders_gold
      mode: overwrite
```
