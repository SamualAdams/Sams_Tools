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
        """Create standardized index column name from input column name."""
        index_col_name = source_entity_col.replace("FK__", "Index__").replace("PK__", "Index__")
        if not index_col_name.startswith("Index__"):
            index_col_name = f"Index__{source_entity_col}"
        return index_col_name

    def index(self, source_entity_col, entity_kind, existing_mapping_df=None):
        """
        Statelessly assign stable, consecutive indices to unique, normalized entities.
        Args:
            source_entity_col: Name of the column to index (e.g., 'customer', 'plant', etc.).
            entity_kind: Kind of entity ('customer', 'plant', 'material', etc.)
            existing_mapping_df: Optional mapping DataFrame to extend with new entities
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
        index_col = "index"
        normalized_expr = F.upper(F.trim(F.col(source_entity_col).cast("string")))
        normalized_join_col = f"{source_entity_col}__normalized"
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
                    F.upper(F.trim(F.col(normalized_entity).cast("string"))).alias(normalized_entity),
                    F.col(index_col).cast("long").alias(index_col)
                )
                .filter(F.col(normalized_entity).isNotNull() & (F.col(normalized_entity) != ""))
            ).dropDuplicates([normalized_entity])

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
                        F.row_number().over(Window.orderBy(normalized_entity)) + F.lit(max_index)
                    )
                )
                mapping = existing_mapping.unionByName(new_entities_with_index)
        else:
            mapping = (
                input_entities
                .withColumn(index_col, F.row_number().over(Window.orderBy(normalized_entity)))
            )

        # Standardized index column name for joining
        index_col_name = self._create_index_column_name(source_entity_col)
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

    def customer(self, source_entity_col, existing_mapping_df=None):
        """
        Stateless indexing for customers.

        Updates ``indexed_df`` in-place with the customer index column while
        returning the mapping and latest DataFrame snapshot.
        """
        return self.index(source_entity_col, "customer", existing_mapping_df=existing_mapping_df)

    def plant(self, source_entity_col, existing_mapping_df=None):
        """
        Stateless indexing for plants.

        Updates ``indexed_df`` in-place with the plant index column while
        returning the mapping and latest DataFrame snapshot.
        """
        return self.index(source_entity_col, "plant", existing_mapping_df=existing_mapping_df)

    def material(self, source_entity_col, existing_mapping_df=None):
        """
        Stateless indexing for materials.

        Updates ``indexed_df`` in-place with the material index column while
        returning the mapping and latest DataFrame snapshot.
        """
        return self.index(source_entity_col, "material", existing_mapping_df=existing_mapping_df)


if __name__ == "__main__":
    from pyspark.sql import Row
    from tool__workstation import SparkWorkstation

    workstation = SparkWorkstation()
    spark = workstation.start_session("local_delta")

    demo_df = spark.createDataFrame(
        [
            Row(customer_name="Acme", plant_location="Plant-1", material_code="SKU-001"),
            Row(customer_name="ACME", plant_location="Plant-1", material_code="SKU-002"),
            Row(customer_name="Zenith", plant_location="Plant-9", material_code="SKU-003"),
            Row(customer_name="Nova Retail", plant_location="Plant-42", material_code="SKU-900"),
        ]
    )

    indexer = TableIndexer(demo_df)

    print("=== Plant Mapping ===")
    plant_result = indexer.plant("plant_location")
    plant_result["mapping"].orderBy("index").show()

    print("=== Material Mapping ===")
    material_result = indexer.material("material_code")
    material_result["mapping"].orderBy("index").show()

    print("=== Customer Mapping ===")
    customer_result = indexer.customer("customer_name")
    customer_result["mapping"].orderBy("index").show()

    print("=== Final Indexed DataFrame ===")
    indexer.indexed_df.show()
