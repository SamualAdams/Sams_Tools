"""
DataFrame standardization utility for medallion architecture.

Single polish() function that standardizes column names and key columns.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _lowercase_columns(df: DataFrame) -> DataFrame:
    """Ensure all column names are lowercase."""
    for name in df.columns:
        lc = name.lower()
        if lc != name:
            df = df.withColumnRenamed(name, lc)
    return df


def _reorder_columns(df: DataFrame) -> DataFrame:
    """Reorder columns: keyP__/keyF__, *_code, others (each group alphabetical)."""
    cols_lower_map = {c.lower(): c for c in df.columns}
    key_cols = sorted([cols_lower_map[c] for c in cols_lower_map 
                      if c.startswith("keyp__") or c.startswith("keyf__")])
    code_cols = sorted([cols_lower_map[c] for c in cols_lower_map 
                       if c.endswith("_code") and cols_lower_map[c] not in key_cols])
    other_cols = sorted([c for c in df.columns if c not in key_cols + code_cols])
    return df.select(key_cols + code_cols + other_cols)


def polish(df: DataFrame) -> DataFrame:
    """
    Standardize DataFrame column names and key column values.
    
    Performs complete standardization:
    - Standardizes column names (lowercase, replace special chars with underscores)
    - Cleans keyP__/keyF__ column values (trim, lowercase, strip leading zeros, fill nulls with 'na')
    - Reorders columns: keyP__/keyF__, *_code, others (each group alphabetical)
    
    Args:
        df: Input Spark DataFrame
        
    Returns:
        Fully standardized DataFrame
    """
    
    # Build complete column rename mapping to avoid conflicts
    rename_mapping = {}
    for name in df.columns:
        new_col = (
            name.strip()
            .replace(" ", "_").replace(",", "_").replace(";", "_")
            .replace("{", "_").replace("}", "_").replace("(", "_").replace(")", "_")
            .replace("\n", "_").replace("\t", "_").replace("=", "_").replace("-", "_")
            .strip("_").lower()
        )
        if new_col != name:
            rename_mapping[name] = new_col
    
    # Apply all renames at once
    for old_name, new_name in rename_mapping.items():
        df = df.withColumnRenamed(old_name, new_name)
    
    # Standardize keyP__/keyF__ column values using current column names
    for name in df.columns:
        col_lower = name.lower()
        if ("keyp__" in col_lower) or ("keyf__" in col_lower):
            # Cast to string first to ensure string operations work
            df = df.withColumn(name, F.col(name).cast("string"))
            df = df.withColumn(name, F.lower(F.trim(F.col(name))))
            df = df.withColumn(name, F.regexp_replace(F.col(name), r"^0+", ""))
            df = df.withColumn(name, F.when(F.col(name).isNull(), "na").otherwise(F.col(name)))
    
    # Final standardization: ensure lowercase columns and proper ordering
    df = _lowercase_columns(df)
    return _reorder_columns(df)


if __name__ == "__main__":
    from pyspark.sql import Row
    from tool__workstation import SparkWorkstation

    workstation = SparkWorkstation()
    spark = workstation.start_session("local_delta")

    demo_df = spark.createDataFrame(
        [
            Row(KeyP__Customer="  0001", Product_Code="SKU-001", Sales=15),
            Row(KeyP__Customer="0002", Product_Code="SKU-002", Sales=21),
        ]
    )

    polished = polish(demo_df)
    polished.show()
