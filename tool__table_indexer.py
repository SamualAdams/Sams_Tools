"""Stateless entity indexer for demand planning Spark DataFrames."""

from pyspark.sql import functions as F, Window
from tool__workstation import is_spark_active

class TableIndexer:
    """
    Stateless entity indexer for demand planning entities (customer, plant, material).
    Assigns stable, consecutive indices to unique, normalized entities.
    No persistence is performed; the original DataFrame is preserved as ``base_df``
    and all newly created index columns accumulate on ``indexed_df``.
    """
    def __init__(self, df_entities):
        """
        Initialize with DataFrame containing entities that need indices.

        Args:
            df_entities: Source DataFrame containing entities to index.
                Stored as ``base_df`` (never mutated) while ``indexed_df`` keeps
                the progressively indexed results.
        """
        if not is_spark_active():
            raise RuntimeError("No active Spark session. Use workstation to start one.")
        self.base_df = df_entities
        self.indexed_df = df_entities

    def _create_index_column_name(self, source_entity_col):
        """Create standardized index column name from input column name (Polish-compatible)."""
        index_col_name = source_entity_col.replace("FK__", "index__").replace("PK__", "index__")
        if not index_col_name.startswith("index__"):
            index_col_name = f"index__{source_entity_col}"
        return index_col_name

    def _normalize_entity_value(self, col):
        """
        Normalize entity values with consistent transformations:
        1. Cast to string
        2. Trim whitespace
        3. Convert to uppercase
        4. Strip leading zeros (preserves single '0')

        Args:
            col: PySpark column to normalize
        Returns:
            PySpark column expression with all normalizations applied
        """
        # Start with basic normalization
        normalized = F.upper(F.trim(col.cast("string")))
        # Strip leading zeros, but preserve single '0' using positive lookahead
        normalized = F.regexp_replace(normalized, r"^0+(?=\d)", "")
        return normalized

    def _create_composite_key(self, col_list):
        """
        Create normalized composite key from multiple columns.

        Args:
            col_list: List of column names to concatenate
        Returns:
            PySpark column expression with normalized composite key
        """
        normalized_cols = [self._normalize_entity_value(F.col(col)) for col in col_list]
        return F.concat_ws("|", *normalized_cols)

    def index(self, source_entity_cols, entity_kind, existing_mapping_df=None, append_new_entities=True):
        """
        Statelessly assign stable, consecutive indices to unique, normalized entities.
        Args:
            source_entity_cols: List of column names for composite key or single column name string
            entity_kind: Kind of entity ('customer', 'plant', 'material', etc.)
            existing_mapping_df: Optional mapping DataFrame to extend with new entities
            append_new_entities: If True (default), append new entities to existing mapping.
                If False, only index entities that exist in existing_mapping_df.
        Returns:
            dict with:
              - "mapping": DataFrame of mapping (columns: [index, normalized_entity])
              - "focal_indexed": input DataFrame with an index column joined
        Note:
            ``base_df`` remains unchanged; ``indexed_df`` is updated in-place with
            every call so additional entity kinds compound on the progressively
            indexed DataFrame.
        """
        normalized_entity = entity_kind
        index_col = f"{entity_kind}_index"

        # Handle both single column and composite key scenarios
        if isinstance(source_entity_cols, str):
            # Single column (backwards compatibility)
            primary_col = source_entity_cols
            normalized_expr = self._normalize_entity_value(F.col(source_entity_cols))
        else:
            # Composite key (list of columns)
            primary_col = source_entity_cols[0]  # Use first column for naming
            normalized_expr = self._create_composite_key(source_entity_cols)

        normalized_join_col = f"{primary_col}__normalized"
        # Normalize and deduplicate entities
        input_entities = (
            self.indexed_df
            .select(normalized_expr.alias(normalized_entity))
            .distinct()
            .filter(F.col(normalized_entity).isNotNull() & (F.col(normalized_entity) != ""))
        )

        if existing_mapping_df is not None:
            existing_mapping = (
                existing_mapping_df
                .select(
                    self._normalize_entity_value(F.col(normalized_entity)).alias(normalized_entity),
                    F.col(index_col).cast("long").alias(index_col)
                )
                .filter(F.col(normalized_entity).isNotNull() & (F.col(normalized_entity) != ""))
            ).dropDuplicates([normalized_entity])

            if append_new_entities:
                # Current behavior: append new entities to existing mapping
                max_index_row = existing_mapping.agg(F.max(index_col)).first()
                max_index = max_index_row[0] if max_index_row and max_index_row[0] is not None else 0

                new_entities = input_entities.join(
                    existing_mapping.select(normalized_entity),
                    on=normalized_entity,
                    how="left_anti"
                )

                if new_entities.limit(1).count() == 0:
                    mapping = existing_mapping
                else:
                    new_entities_with_index = (
                        new_entities
                        .withColumn(
                            index_col,
                            (F.row_number().over(Window.orderBy(normalized_entity)) + F.lit(max_index)).cast("long")
                        )
                    )
                    mapping = existing_mapping.unionByName(new_entities_with_index)
            else:
                # New behavior: only index entities that exist in existing mapping
                mapping = existing_mapping
        else:
            mapping = (
                input_entities
                .withColumn(index_col, F.row_number().over(Window.orderBy(normalized_entity)).cast("long"))
            )

        # Standardized index column name for joining
        index_col_name = self._create_index_column_name(primary_col)

        # Drop existing index column if present (prevents duplicates on re-run)
        self.indexed_df = self.indexed_df.drop(index_col_name)

        # Join index back to original DataFrame
        focal_indexed = (
            self.indexed_df
            .join(
                mapping.select(
                    F.col(normalized_entity).alias(normalized_join_col),
                    F.col(index_col).alias(index_col_name)
                ),
                on=[normalized_expr == F.col(normalized_join_col)],
                how="left"
            )
            .drop(normalized_join_col)
        )
        self.indexed_df = focal_indexed
        return {
            "mapping": mapping.orderBy(index_col),
            "focal_indexed": focal_indexed
        }

    def customer(self, zsource_col, customer_code_col, existing_mapping_df=None, append_new_entities=True):
        """
        Stateless indexing for customers with required composite key.

        Args:
            zsource_col: Name of the column containing data source identifiers (primary hierarchy)
            customer_code_col: Name of the column containing customer codes (within zsource)
            existing_mapping_df: Optional existing customer mapping DataFrame
            append_new_entities: If True (default), append new customers to existing mapping.
                If False, only index customers that exist in existing_mapping_df.

        Updates ``indexed_df`` in-place with the customer index column while
        returning the mapping and latest DataFrame snapshot.

        Note: Customer uniqueness hierarchy: zsource > customer_code (customers exist within zsource).
        """
        return self.index([zsource_col, customer_code_col], "customer", existing_mapping_df=existing_mapping_df, append_new_entities=append_new_entities)

    def plant(self, zsource_col, plant_code_col, existing_mapping_df=None, append_new_entities=True):
        """
        Stateless indexing for plants with required composite key.

        Args:
            zsource_col: Name of the column containing data source identifiers (primary hierarchy)
            plant_code_col: Name of the column containing plant codes (within zsource)
            existing_mapping_df: Optional existing plant mapping DataFrame
            append_new_entities: If True (default), append new plants to existing mapping.
                If False, only index plants that exist in existing_mapping_df.

        Updates ``indexed_df`` in-place with the plant index column while
        returning the mapping and latest DataFrame snapshot.

        Note: Plant uniqueness hierarchy: zsource > plant_code (plants exist within zsource).
        """
        return self.index([zsource_col, plant_code_col], "plant", existing_mapping_df=existing_mapping_df, append_new_entities=append_new_entities)

    def material(self, zsource_col, material_code_col, existing_mapping_df=None, append_new_entities=True):
        """
        Stateless indexing for materials with required composite key.

        Args:
            zsource_col: Name of the column containing data source identifiers (primary hierarchy)
            material_code_col: Name of the column containing material codes (within zsource)
            existing_mapping_df: Optional existing material mapping DataFrame
            append_new_entities: If True (default), append new materials to existing mapping.
                If False, only index materials that exist in existing_mapping_df.

        Updates ``indexed_df`` in-place with the material index column while
        returning the mapping and latest DataFrame snapshot.

        Note: Material uniqueness hierarchy: zsource > material_code (materials exist within zsource).
        """
        return self.index([zsource_col, material_code_col], "material", existing_mapping_df=existing_mapping_df, append_new_entities=append_new_entities)


if __name__ == "__main__":
    from pyspark.sql import Row
    from tool__workstation import SparkWorkstation

    workstation = SparkWorkstation()
    spark = workstation.start_session("local_delta")

    demo_df = spark.createDataFrame(
        [
            Row(customer_name="Acme", plant_location="Plant-001", material_code="SKU-001", zsource="SAP"),
            Row(customer_name="ACME", plant_location="Plant-1", material_code="SKU-002", zsource="SAP"),
            Row(customer_name="Zenith", plant_location="Plant-009", material_code="SKU-003", zsource="Oracle"),
            Row(customer_name="Nova Retail", plant_location="Plant-042", material_code="SKU-000900", zsource="SAP"),
            Row(customer_name=" acme ", plant_location="Plant-0001", material_code="0", zsource="Legacy"),
        ]
    )

    # Demo data with additional customers not in fact data
    customer_dim_df = spark.createDataFrame(
        [
            Row(customer_code="ACME", zsource="SAP", status="Active"),
            Row(customer_code="ZENITH", zsource="Oracle", status="Active"),
            Row(customer_code="NOVA RETAIL", zsource="SAP", status="Active"),
            Row(customer_code="Inactive Corp", zsource="Legacy", status="Inactive"),  # This shouldn't be indexed
            Row(customer_code="Old Client", zsource="Legacy", status="Inactive"),     # This shouldn't be indexed
        ]
    )

    # Test 1: Basic indexing (original behavior)
    indexer = TableIndexer(demo_df)

    print("=== Test 1: Basic Customer Indexing (append_new_entities=True) ===")
    customer_result = indexer.customer("zsource", "customer_name")
    customer_result["mapping"].orderBy("customer_index").show()
    print("Fact table customers indexed:", customer_result["mapping"].count())
    print("✓ Mapping columns:", customer_result["mapping"].columns)
    print("✓ Composite key format: zsource|customer_name (hierarchy: zsource > customer)")

    # Test 2: Create existing mapping from fact table customers
    fact_customer_mapping = customer_result["mapping"]

    # Test 3: Index customer dimension with append_new_entities=True (default)
    print("\n=== Test 2: Index Customer Dimension with append_new_entities=True ===")
    dim_indexer = TableIndexer(customer_dim_df)
    dim_result_append = dim_indexer.customer("zsource", "customer_code", existing_mapping_df=fact_customer_mapping, append_new_entities=True)
    dim_result_append["mapping"].orderBy("customer_index").show()
    print("Total customers after append:", dim_result_append["mapping"].count(), "(includes inactive customers)")

    # Test 4: Index customer dimension with append_new_entities=False
    print("\n=== Test 3: Index Customer Dimension with append_new_entities=False ===")
    dim_indexer2 = TableIndexer(customer_dim_df)
    dim_result_no_append = dim_indexer2.customer("zsource", "customer_code", existing_mapping_df=fact_customer_mapping, append_new_entities=False)
    dim_result_no_append["mapping"].orderBy("customer_index").show()
    print("Total customers without append:", dim_result_no_append["mapping"].count(), "(only customers from fact table)")

    print("\n=== Test 4: Final Indexed Customer Dimension (no append) ===")
    dim_indexer2.indexed_df.show()
    print("Note: Inactive customers get NULL indices because they don't exist in fact table mapping")

    print("\n=== Test 5: Plant and Material Indexing ===")
    plant_result = indexer.plant("zsource", "plant_location")
    plant_result["mapping"].orderBy("plant_index").show()
    print("✓ Plant mapping columns:", plant_result["mapping"].columns)

    material_result = indexer.material("zsource", "material_code")
    material_result["mapping"].orderBy("material_index").show()
    print("✓ Material mapping columns:", material_result["mapping"].columns)

    print("\n=== Final Indexed Fact DataFrame ===")
    indexer.indexed_df.show()

    print("\n=== Test 6: Duplicate Prevention - Running Customer Indexing Again ===")
    print("Columns before re-run:", len(indexer.indexed_df.columns))
    print("Column names:", indexer.indexed_df.columns)

    # Re-run customer indexing - should not create duplicates
    customer_result_rerun = indexer.customer("zsource", "customer_name")

    print("Columns after re-run:", len(indexer.indexed_df.columns))
    print("Column names:", indexer.indexed_df.columns)
    print("✓ No duplicate index__customer_name columns!" if indexer.indexed_df.columns.count("index__customer_name") == 1 else "✗ Duplicate columns detected!")

    print("\n=== Final DataFrame After Re-indexing ===")
    indexer.indexed_df.show()

    print("\n=== Test 7: Schema Conflict Prevention ===")
    print("Entity-specific index column names prevent schema conflicts:")
    print(f"Customer mapping schema: {customer_result['mapping'].columns}")
    print(f"Plant mapping schema: {plant_result['mapping'].columns}")
    print(f"Material mapping schema: {material_result['mapping'].columns}")
    print("✓ Each mapping has unique index column name - safe for looped saving!")

    print("\n=== Test 8: Polish Integration Test ===")
    from tool__table_polisher import polish

    # Create test data that needs polishing
    raw_df = spark.createDataFrame([
        Row(KeyP__Customer="  001", Plant_Location="Plant-001", Material_Code="SKU-001"),
        Row(KeyP__Customer="002", Plant_Location="Plant-1", Material_Code="SKU-002"),
    ])

    # Polish first (standardize to Polish conventions)
    polished_df = polish(raw_df)
    print("After Polish standardization:")
    polished_df.show()
    print("Polished columns:", polished_df.columns)

    # Add zsource column for demonstration
    polished_df = polished_df.withColumn("zsource", F.lit("DEMO"))

    # Index the polished data
    polish_indexer = TableIndexer(polished_df)
    polish_customer_result = polish_indexer.customer("zsource", "keyp__customer")

    print("\nAfter TableIndexer (Polish-compatible naming):")
    polish_indexer.indexed_df.show()
    print("Final columns:", polish_indexer.indexed_df.columns)
    print("✓ Index columns use lowercase 'index__' prefix - consistent with Polish conventions!")
