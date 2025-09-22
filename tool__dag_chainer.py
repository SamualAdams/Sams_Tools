from pyspark.sql import SparkSession
from tool__workstation import get_spark, is_spark_active, SparkWorkstation

class DagChain:
    """
    DagChain - DataFrame workflow management with centralized Spark session.
    
    Manages a collection of DataFrames (dags) with operations for tracing,
    viewing, and writing to Delta tables. Integrates with SparkWorkstation
    for consistent session management.
    """
    
    def __init__(self, dags: dict = {}, ensure_spark: bool = True):
        """
        Initialize DagChain with optional DataFrames.

        Args:
            dags: Dictionary of DataFrames to initialize with
            ensure_spark: Whether to ensure Spark session is active
        """
        self._workstation = SparkWorkstation()
        self._display_cache = None

        if ensure_spark and not is_spark_active():
            self._workstation.start_session("local_delta")
        
        for name, dag in dags.items():
            setattr(self, name, dag)
        
        self._spark = None  # Will be lazily loaded
    
    @property
    def spark(self) -> SparkSession:
        """Get the current Spark session from workstation."""
        if not is_spark_active():
            raise RuntimeError("No active Spark session. Use workstation to start one.")
        return get_spark()
    
    def ensure_session(self, config_preset: str = "local_delta") -> SparkSession:
        """Ensure a Spark session is active, creating one if necessary."""
        if not is_spark_active():
            return self._workstation.start_session(config_preset)
        return get_spark()

    def _refresh_list(self):
        self._dags = [k for k in vars(self) if k.startswith("dag__")]

    def trace(self, shape: bool = False):
        self._refresh_list()
        print("Attributes starting with 'dag__':")
        if shape:
            for i, k in enumerate(self._dags, start=0):  # 0-based
                df = getattr(self, k)
                print(f"  {i} - {k[len('dag__'):]} ({len(df.columns)}, {df.count()})")
            print()
        else:
            for i, k in enumerate(self._dags, start=0):
                print(f"  {i} - {k[len('dag__'):]}")
            print()

    def _resolve_index(self, idx: int) -> int:
        n = len(self._dags)
        resolved = idx if idx >= 0 else n + idx
        if resolved < 0 or resolved >= n:
            raise IndexError(f"Index {idx} out of range (valid: -{n}..{n-1}).")
        return resolved

    def look(self, idx: int = -1, rows: int = 20):
        """Display a dag by index (0-based, negatives allowed)."""
        self._refresh_list()
        resolved = self._resolve_index(idx)
        dag_name = self._dags[resolved]
        dag = getattr(self, dag_name)

        print(f"{resolved} - {dag_name[len('dag__'):]}")
        self._universal_display(dag, rows)
        print()
    
    def _databricks_display(self):
        """Return the Databricks-native display callable when available."""
        if self._display_cache is not None:
            return self._display_cache

        display_fn = None
        if getattr(self._workstation, "_is_databricks", False):
            try:
                import __main__  # Databricks injects display into notebook globals
                candidate = getattr(__main__, "display", None)
                if callable(candidate):
                    display_fn = candidate
            except Exception:
                display_fn = None

            if display_fn is None:
                try:
                    import builtins
                    candidate = getattr(builtins, "display", None)
                    if callable(candidate) and "IPython.display" not in getattr(candidate, "__module__", ""):
                        display_fn = candidate
                except Exception:
                    display_fn = None

        self._display_cache = display_fn
        return display_fn

    def _universal_display(self, df, rows: int = 20):
        """Render DataFrames using Databricks display when available, otherwise fall back to show."""
        display_fn = self._databricks_display()

        if display_fn is not None:
            try:
                display_df = df if rows is None else df.limit(rows)
                display_fn(display_df)
                return
            except Exception:
                # Reset cache so future calls can fall back gracefully
                self._display_cache = None

        # Fallback works for local development and when Databricks display is unavailable
        df.show(rows, truncate=False)

    def pick(self, idx: int = -1):
        """Return a dag by index (0-based, negatives allowed) without display/print."""
        self._refresh_list()
        resolved = self._resolve_index(idx)
        return getattr(self, self._dags[resolved])

    def write(self, table_name: str, chain_index: int = -1, mode: str = "overwrite", 
              wipe: bool = False, path: str = None, catalog: str = None):
        """
        Write a DataFrame from the chain to Delta format.
        
        Args:
            table_name: Name of the table/file
            chain_index: Index of DataFrame in chain to write
            mode: Write mode (overwrite, append, etc.)
            wipe: Whether to drop existing table first
            path: File path for Delta files (if None, uses table)
            catalog: Catalog prefix (default: test_catalog.supply_chain)
        """
        self.ensure_session()
        
        # Get the DataFrame to write
        df_to_write = self.pick(chain_index)
        
        if catalog is None:
            catalog = "test_catalog.supply_chain"
        
        full_table_name = f"{catalog}.{table_name}"
        
        # Handle table wiping
        if wipe:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            except Exception:
                pass
        
        # Write the DataFrame
        writer = df_to_write.write.format("delta").mode(mode).option("overwriteSchema", "true")
        
        if path:
            writer.save(path)
        else:
            writer.saveAsTable(full_table_name)
    
    def write_to_path(self, path: str, chain_index: int = -1, mode: str = "overwrite"):
        """
        Write a DataFrame from the chain to a Delta file path.
        
        Args:
            path: Delta file path
            chain_index: Index of DataFrame in chain to write
            mode: Write mode (overwrite, append, etc.)
        """
        self.write(table_name="", chain_index=chain_index, mode=mode, path=path)
    
    def session_info(self) -> dict:
        """Get information about the current Spark session."""
        return self._workstation.get_session_info()
    
    def health_check(self) -> dict:
        """Perform a health check on the Spark session."""
        return self._workstation.health_check()


if __name__ == "__main__":
    from pyspark.sql import functions as F

    chain = DagChain()
    spark = chain.ensure_session("local_delta")

    demo = spark.createDataFrame([
        ("Acme", "Widget", 10),
        ("Acme", "Widget", 5),
        ("Zenith", "Gadget", 7),
    ], ["customer", "sku", "units"])

    chain.dag__raw = demo
    chain.dag__by_customer = chain.dag__raw.groupBy("customer").agg(F.sum("units").alias("total_units"))

    chain.trace(shape=True)
    chain.look(-1)
