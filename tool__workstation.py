"""
Demand Planning Workstation - Centralized Spark Session Management

This module provides a centralized way to manage Spark sessions across all
demand planning tools and workflows. It implements a singleton pattern to
ensure consistent session configuration and resource management.
"""

import os
import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from delta import configure_spark_with_delta_pip


class SparkWorkstation:
    """
    Centralized Spark session manager for demand planning workflows.
    
    Features:
    - Singleton pattern for session management
    - Delta Lake integration
    - Configurable session parameters
    - Health checking and validation
    - Environment detection
    """
    
    _instance: Optional['SparkWorkstation'] = None
    _spark: Optional[SparkSession] = None
    
    def __new__(cls) -> 'SparkWorkstation':
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._config_presets = self._get_config_presets()
        self._is_initialized = False
    
    @classmethod
    def get_session(cls, config_preset: str = "local_delta") -> SparkSession:
        """
        Get the current Spark session, creating one if necessary.
        
        Args:
            config_preset: Configuration preset to use ('local_delta', 'local_basic', 'cluster')
            
        Returns:
            SparkSession: The active Spark session
        """
        workstation = cls()
        if workstation._spark is None or workstation._spark.sparkContext._jsc is None:
            workstation.start_session(config_preset)
        return workstation._spark
    
    @classmethod
    def is_active(cls) -> bool:
        """Check if a Spark session is currently active."""
        workstation = cls()
        try:
            return (workstation._spark is not None and 
                   workstation._spark.sparkContext._jsc is not None)
        except Exception:
            return False
    
    def start_session(self, config_preset: str = "local_delta", custom_config: Optional[Dict[str, Any]] = None) -> SparkSession:
        """
        Start a new Spark session with the specified configuration.

        Args:
            config_preset: Configuration preset to use
            custom_config: Additional custom configurations

        Returns:
            SparkSession: The newly created Spark session
        """
        if self._spark is not None:
            self.stop_session()

        # Set JAVA_HOME if it's not properly set
        if not os.environ.get("JAVA_HOME") or "JavaAppletPlugin" in os.environ.get("JAVA_HOME", ""):
            # Try to find proper Java installation (prioritize Java 17+ for PySpark 4.0)
            java_paths = [
                "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home",
                "/Library/Java/JavaVirtualMachines/openjdk-17.jdk/Contents/Home",
                "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home",
                "/Library/Java/JavaVirtualMachines/openjdk-11.jdk/Contents/Home"
            ]
            for java_path in java_paths:
                if os.path.exists(java_path):
                    os.environ["JAVA_HOME"] = java_path
                    self.logger.info(f"Set JAVA_HOME to: {java_path}")
                    break

        # Get base configuration
        config = self._config_presets.get(config_preset, self._config_presets["local_delta"])

        # Apply custom configurations if provided
        if custom_config:
            config.update(custom_config)
        
        # Create SparkSession builder
        builder = SparkSession.builder.appName(config.pop("app_name", "DemandPlanningWorkstation"))
        
        # Apply configurations
        for key, value in config.items():
            builder = builder.config(key, value)
        
        # Configure with Delta Lake if needed
        if "delta" in config_preset:
            builder = configure_spark_with_delta_pip(builder)
        
        # Create session
        self._spark = builder.getOrCreate()
        self._spark.sparkContext.setLogLevel("WARN")
        
        self._is_initialized = True
        
        return self._spark
    
    def stop_session(self) -> None:
        """Stop the current Spark session and clean up resources."""
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            self._is_initialized = False
        else:
            self.logger.warning("No active Spark session to stop")
    
    def restart_session(self, config_preset: str = "local_delta") -> SparkSession:
        """Restart the Spark session with the specified configuration."""
        self.stop_session()
        return self.start_session(config_preset)
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check on the current Spark session.
        
        Returns:
            Dict containing health status information
        """
        health_status = {
            "session_active": False,
            "session_id": None,
            "app_name": None,
            "master": None,
            "version": None,
            "delta_enabled": False,
            "error": None
        }
        
        try:
            if self._spark is not None:
                health_status["session_active"] = True
                health_status["session_id"] = self._spark.sparkContext.applicationId
                health_status["app_name"] = self._spark.sparkContext.appName
                health_status["master"] = self._spark.sparkContext.master
                health_status["version"] = self._spark.version
                
                # Test basic functionality
                test_df = self._spark.range(1)
                test_count = test_df.count()
                
                # Check if Delta is available
                try:
                    from delta.tables import DeltaTable
                    health_status["delta_enabled"] = True
                except ImportError:
                    health_status["delta_enabled"] = False
                    
        except Exception as e:
            health_status["error"] = str(e)
            self.logger.error(f"Health check failed: {e}")
        
        return health_status
    
    def get_session_info(self) -> Dict[str, Any]:
        """Get detailed information about the current session."""
        if not self.is_active():
            return {"status": "No active session"}
        
        health = self.health_check()
        session_info = {
            "status": "Active",
            "session_details": health,
            "configurations": dict(self._spark.sparkContext.getConf().getAll()),
            "default_parallelism": self._spark.sparkContext.defaultParallelism,
            "total_cores": self._spark.sparkContext._jsc.sc().totalCores(),
        }
        
        return session_info
    
    def create_test_dataframe(self) -> DataFrame:
        """Create a simple test DataFrame to verify session functionality."""
        if not self.is_active():
            raise RuntimeError("No active Spark session. Call start_session() first.")
        
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        from pyspark.sql import functions as F
        
        # Create test data
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        
        data = [
            (1, "test_a", 100),
            (2, "test_b", 200),
            (3, "test_c", 300)
        ]
        
        df = self._spark.createDataFrame(data, schema)
        df = df.withColumn("timestamp", F.current_timestamp())
        
        return df
    
    def _get_config_presets(self) -> Dict[str, Dict[str, Any]]:
        """Get predefined configuration presets for different environments."""
        return {
            "local_delta": {
                "app_name": "DemandPlanningAgent_Delta",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.driver.memory": "1g",
                "spark.executor.memory": "1g",
                "spark.driver.host": "localhost",
                "spark.sql.warehouse.dir": "file:///tmp/spark-warehouse"
            },
            "local_basic": {
                "app_name": "DemandPlanningAgent_Basic",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.driver.memory": "1g",
                "spark.executor.memory": "1g",
                "spark.driver.host": "localhost",
                "spark.sql.warehouse.dir": "file:///tmp/spark-warehouse"
            },
            "local_performance": {
                "app_name": "DemandPlanningAgent_Performance",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.driver.memory": "4g",
                "spark.executor.memory": "4g",
                "spark.driver.maxResultSize": "2g"
            }
        }


# Convenience functions for easy access
def get_spark(config_preset: str = "local_delta") -> SparkSession:
    """Get the current Spark session (convenience function)."""
    return SparkWorkstation.get_session(config_preset)

def is_spark_active() -> bool:
    """Check if Spark session is active (convenience function)."""
    return SparkWorkstation.is_active()

def stop_spark() -> None:
    """Stop the current Spark session (convenience function)."""
    workstation = SparkWorkstation()
    workstation.stop_session()

def restart_spark(config_preset: str = "local_delta") -> SparkSession:
    """Restart Spark session (convenience function)."""
    workstation = SparkWorkstation()
    return workstation.restart_session(config_preset)

def spark_health_check() -> Dict[str, Any]:
    """Perform Spark health check (convenience function)."""
    workstation = SparkWorkstation()
    return workstation.health_check()

def get_spark_info() -> Dict[str, Any]:
    """Get Spark session info (convenience function)."""
    workstation = SparkWorkstation()
    return workstation.get_session_info()




if __name__ == "__main__":
    # Demo usage
    print("🚀 Demand Planning Workstation Demo")
    print("=" * 50)
    
    # Start session
    spark = get_spark("local_delta")
    print(f"✅ Spark session started: {spark.version}")
    
    # Health check
    health = spark_health_check()
    print(f"📊 Health check: {health['session_active']}")
    
    # Create test DataFrame
    workstation = SparkWorkstation()
    test_df = workstation.create_test_dataframe()
    print(f"🧪 Test DataFrame created with {test_df.count()} rows")
    test_df.show()
    
    # Session info
    info = get_spark_info()
    print(f"ℹ️  Session info: {info['status']}")
    
    print("\n✅ Workstation demo completed successfully!")
