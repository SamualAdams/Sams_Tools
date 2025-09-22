"""
Entity Indexing Tool for Demand Planning Workflows

Provides persistent, consistent indexing for entities (customers, plants, materials)
with automatic Delta table management and race condition handling.
"""

from pyspark.sql import functions as F, Window
from pyspark.sql.utils import AnalysisException
import logging
from tool__workstation import get_spark, is_spark_active

logger = logging.getLogger(__name__)

def _is_databricks_environment() -> bool:
    """Detect if running in Databricks environment."""
    import os
    return ("DATABRICKS_RUNTIME_VERSION" in os.environ or
            "DB_CLUSTER_ID" in os.environ or
            any("databricks" in str(v).lower() for v in os.environ.values()))

class TableIndexer:
    """
    Persistent entity indexer with Delta table management.

    Like assigning jersey numbers to players - you have a roster of players
    who need unique numbers assigned consistently across games.

    Features:
    - Persistent index mapping using Delta tables
    - Race condition safe MERGE operations
    - Automatic catalog/schema management
    - Integration with workstation session management
    """

    def __init__(self, df_entities, catalog=None, schema=None):
        """
        Initialize with DataFrame containing entities that need indices.

        Args:
            df_entities: DataFrame containing entities that need indices
            catalog: Catalog name for Delta tables (auto-detected if None)
            schema: Schema name for Delta tables (auto-detected if None)
        """
        if not is_spark_active():
            raise RuntimeError("No active Spark session. Use workstation to start one.")

        self.df_entities = df_entities
        self.is_databricks = _is_databricks_environment()

        # Auto-detect appropriate catalog and schema based on environment
        self.catalog, self.schema = self._detect_catalog_and_schema(catalog, schema)

        # Ensure catalog and schema exist at initialization
        self._ensure_catalog_and_schema_exist()

    @property
    def spark(self):
        """Get the current Spark session from workstation."""
        if not is_spark_active():
            raise RuntimeError("No active Spark session. Use workstation to start one.")
        return get_spark()

    def _detect_catalog_and_schema(self, catalog, schema):
        """Auto-detect appropriate catalog and schema based on environment."""
        if catalog is not None and schema is not None:
            return catalog, schema

        if self.is_databricks:
            # In Databricks, try to use existing catalogs or fall back to default
            try:
                spark = self.spark
                # Try to get current catalog
                current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]

                # If using default catalog, try to find or create a suitable schema
                if current_catalog == "spark_catalog":
                    schema = schema or "default"
                    catalog = current_catalog
                    logger.info(f"Using Databricks default catalog: {catalog}.{schema}")
                else:
                    # Using Unity Catalog or custom catalog
                    schema = schema or "supply_chain"
                    catalog = catalog or current_catalog
                    logger.info(f"Using Databricks catalog: {catalog}.{schema}")

                return catalog, schema

            except Exception as e:
                logger.warning(f"Could not detect Databricks catalog, using defaults: {e}")
                return "spark_catalog", "default"
        else:
            # Local environment - use test catalog
            return catalog or "test_catalog", schema or "supply_chain"

    def _get_mapping_table_name(self, entity_kind):
        """Generate standardized mapping table name for entity kind."""
        return f"{self.catalog}.{self.schema}.mapping__active_{entity_kind}s"

    def _ensure_catalog_and_schema_exist(self):
        """Ensure catalog and schema exist before any operations."""
        try:
            # In Databricks, handle catalog creation more carefully
            if self.is_databricks:
                # For spark_catalog, don't try to create it (it already exists)
                if self.catalog != "spark_catalog":
                    try:
                        self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
                        logger.info(f"Catalog ensured: {self.catalog}")
                    except Exception as e:
                        logger.warning(f"Could not create catalog {self.catalog}, using existing: {e}")

                # Create schema if it doesn't exist
                try:
                    self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
                    logger.info(f"Schema ensured: {self.catalog}.{self.schema}")
                except Exception as e:
                    logger.warning(f"Could not create schema {self.catalog}.{self.schema}: {e}")
                    # In Databricks, if we can't create custom schema, fall back to default
                    if self.schema != "default":
                        logger.info("Falling back to default schema")
                        self.schema = "default"
            else:
                # Local environment - create both catalog and schema
                self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")
                logger.info(f"Catalog ensured: {self.catalog}")

                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.catalog}.{self.schema}")
                logger.info(f"Schema ensured: {self.catalog}.{self.schema}")

        except AnalysisException as e:
            if self.is_databricks:
                # In Databricks, if all else fails, use spark_catalog.default
                logger.warning(f"Falling back to spark_catalog.default due to: {e}")
                self.catalog = "spark_catalog"
                self.schema = "default"
            else:
                logger.error(f"Failed to create catalog/schema: {e}")
                raise RuntimeError(f"Cannot initialize TableIndexer: {e}") from e

    def _table_exists(self, table_name):
        """
        Check if a table exists - single source of truth.
        Returns True if table exists, False otherwise.
        """
        try:
            # Parse the table name
            parts = table_name.split(".")
            if len(parts) != 3:
                raise ValueError(f"Table name must be fully qualified: catalog.schema.table, got: {table_name}")

            catalog, schema, table = parts

            # Set the catalog context
            original_catalog = self.spark.catalog.currentCatalog()
            try:
                self.spark.catalog.setCurrentCatalog(catalog)

                # Check if schema exists
                schemas = [db.name for db in self.spark.catalog.listDatabases()]
                if schema not in schemas:
                    logger.debug(f"Schema {schema} not found in catalog {catalog}")
                    return False

                # Check if table exists in the schema
                tables = [t.name for t in self.spark.catalog.listTables(schema)]
                exists = table in tables

                logger.debug(f"Table {table_name} exists: {exists}")
                return exists

            finally:
                # Restore original catalog
                self.spark.catalog.setCurrentCatalog(original_catalog)

        except AnalysisException as e:
            logger.warning(f"Error checking table existence for {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error checking table {table_name}: {e}")
            return False

    def _create_mapping_table_ddl(self, table_name, normalized_col_name):
        """
        Create mapping table using SQL DDL for better concurrency handling.
        """
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            index LONG NOT NULL,
            {normalized_col_name} STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """

        try:
            self.spark.sql(create_table_sql)
            logger.info(f"Mapping table created or confirmed: {table_name}")
        except AnalysisException as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            raise

    def _get_or_create_index_mapping(self, table_name, normalized_col_name):
        """Read existing index mapping or create empty one if doesn't exist."""

        # Create table using DDL (handles concurrent creation better)
        self._create_mapping_table_ddl(table_name, normalized_col_name)

        # Now read the table (it definitely exists)
        try:
            df = self.spark.table(table_name)
            logger.info(f"Successfully loaded mapping table: {table_name}")
            return df
        except AnalysisException as e:
            logger.error(f"Failed to read table {table_name} after creation: {e}")
            raise

    def _merge_new_indices(self, new_entities_with_index, table_name, normalized_entity):
        """Safely merge new entities using Delta MERGE to avoid race conditions."""

        if new_entities_with_index.rdd.isEmpty():
            logger.info(f"No new entities to merge into {table_name}")
            return

        logger.info(f"Starting merge operation for {table_name}")

        # Create a temporary view for the new entities
        temp_view_name = f"temp_new_{normalized_entity}s_{id(self)}"

        try:
            new_entities_with_index.createOrReplaceTempView(temp_view_name)
            logger.debug(f"Created temporary view: {temp_view_name}")

            # Use Delta MERGE to atomically insert only new entities
            merge_sql = f"""
            MERGE INTO {table_name} AS target
            USING {temp_view_name} AS source
            ON target.{normalized_entity} = source.{normalized_entity}
            WHEN NOT MATCHED THEN
                INSERT ({normalized_entity}, index)
                VALUES (source.{normalized_entity}, source.index)
            """

            logger.debug(f"Executing MERGE SQL")
            result = self.spark.sql(merge_sql)

            # Log merge statistics if available
            merge_stats = result.collect()
            if merge_stats:
                logger.info(f"MERGE completed for {table_name}: {merge_stats}")
            else:
                logger.info(f"MERGE completed successfully for {table_name}")

        except AnalysisException as e:
            logger.error(f"MERGE operation failed for {table_name}: {e}")
            raise
        finally:
            # Always clean up temporary view
            try:
                self.spark.catalog.dropTempView(temp_view_name)
                logger.debug(f"Cleaned up temporary view: {temp_view_name}")
            except Exception as e:
                logger.warning(f"Failed to drop temp view {temp_view_name}: {e}")

    def _create_index_column_name(self, source_entity_col):
        """Create standardized index column name from input column name."""
        index_col_name = source_entity_col.replace("FK__", "Index__").replace("PK__", "Index__")
        if not index_col_name.startswith("Index__"):
            index_col_name = f"Index__{source_entity_col}"
        return index_col_name

    def _assign_indices(self, df_existing_index_map, source_entity_col, normalized_entity, index_col="index"):
        """
        Internal method to assign consecutive indices while preserving existing mappings.

        Returns (updated_index_map, new_entities_with_index, indexed_entities).
        """
        # Normalize and get distinct entities from input, filter out nulls/empties
        input_entities = (
            self.df_entities
            .select(F.upper(F.trim(F.col(source_entity_col).cast("string"))).alias(normalized_entity))
            .distinct()
            .filter(F.col(normalized_entity).isNotNull() & (F.col(normalized_entity) != ""))
        )

        # Normalize existing mapping for consistent comparison
        normalized_existing_map = (
            df_existing_index_map
            .select(
                F.upper(F.trim(F.col(normalized_entity).cast("string"))).alias(normalized_entity),
                F.col(index_col)
            )
        )

        # Find entities present in input but not yet in existing mapping
        new_entities = input_entities.join(
            normalized_existing_map.select(normalized_entity),
            on=[normalized_entity],
            how="left_anti"
        )

        # Get max index from existing mapping (handle empty case)
        max_index_row = normalized_existing_map.agg(F.coalesce(F.max(index_col), F.lit(0))).first()
        max_index = max_index_row[0] if max_index_row else 0

        # Assign consecutive indices to new entities
        new_entities_with_index = (
            new_entities
            .withColumn("_rn", F.row_number().over(Window.orderBy(normalized_entity)))
            .withColumn(index_col, F.lit(max_index) + F.col("_rn"))
            .drop("_rn")
        )

        # Union existing + new for the complete updated mapping
        updated_index_map = (
            normalized_existing_map
            .unionByName(new_entities_with_index)
            .orderBy(index_col)
        )

        # Create standardized index column name
        index_col_name = self._create_index_column_name(source_entity_col)

        # Join original input with updated mapping to add index column
        df_indexed_entities = (
            self.df_entities
            .join(
                updated_index_map.select(
                    F.col(normalized_entity).alias(source_entity_col + "_join"),
                    F.col(index_col).alias(index_col_name)
                ),
                on=[F.upper(F.trim(F.col(source_entity_col).cast("string"))) == F.col(source_entity_col + "_join")],
                how="left"
            )
            .drop(source_entity_col + "_join")
        )

        return updated_index_map, new_entities_with_index, df_indexed_entities

    def _assign_indices_with_persistence(self, entity_kind, source_entity_col, normalized_entity, index_col="index"):
        """
        Complete index assignment workflow with Delta table persistence.

        Uses Delta MERGE to prevent race conditions and data loss.
        Returns indexed_entities DataFrame.
        """

        # Get table name for this entity kind
        table_name = self._get_mapping_table_name(entity_kind)

        # Read existing mapping or create empty one
        df_existing_index_map = self._get_or_create_index_mapping(table_name, normalized_entity)

        # Perform the index assignment
        updated_index_map, new_entities_with_index, df_indexed_entities = self._assign_indices(
            df_existing_index_map, source_entity_col, normalized_entity, index_col
        )

        # Merge new entities if there are any
        self._merge_new_indices(new_entities_with_index, table_name, normalized_entity)

        return df_indexed_entities

    def customer(self, source_entity_col):
        """
        Persisted indexing for customers with automatic Delta table persistence.
        Returns indexed entities DataFrame.
        """
        return self._assign_indices_with_persistence("customer", source_entity_col, "customer")

    def plant(self, source_entity_col):
        """
        Persisted indexing for plants with automatic Delta table persistence.
        Returns indexed entities DataFrame.
        """
        return self._assign_indices_with_persistence("plant", source_entity_col, "plant")

    def material(self, source_entity_col):
        """
        Persisted indexing for materials with automatic Delta table persistence.
        Returns indexed entities DataFrame.
        """
        return self._assign_indices_with_persistence("material", source_entity_col, "material")