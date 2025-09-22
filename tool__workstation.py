"""
Demand Planning Workstation - Databricks Compatible Version

This module provides a centralized way to manage Spark sessions across all
demand planning tools and workflows with full Databricks compatibility.
"""

import os
import logging
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def _is_databricks_environment() -> bool:
    """Detect if running in Databricks environment."""
    # Check environment variables
    if ("DATABRICKS_RUNTIME_VERSION" in os.environ or 
        "DB_CLUSTER_ID" in os.environ or
        any("databricks" in str(v).lower() for v in os.environ.values())):
        return True
    
    # Check if we're in a notebook environment with global spark
    try:
        import builtins
        if hasattr(builtins, 'spark'):
            return True
    except:
        pass
    
    # Check globals for spark (common in Databricks notebooks)
    try:
        import sys
        frame = sys._getframe(1)
        while frame:
            if 'spark' in frame.f_globals and hasattr(frame.f_globals['spark'], 'version'):
                return True
            frame = frame.f_back
    except:
        pass
    
    return False


def _is_databricks_connect() -> bool:
    """Detect if using Databricks Connect (remote client)."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            # Check if this is a Connect session by looking for Connect-specific attributes
            return hasattr(spark, '_client') or 'connect' in str(type(spark)).lower()
    except:
        pass
    return False


class SparkWorkstation:
    """
    Centralized Spark session manager for demand planning workflows.
    
    Features:
    - Singleton pattern for session management
    - Delta Lake integration
    - Databricks and local environment compatibility
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
        self._is_databricks = _is_databricks_environment()
        self._is_databricks_connect = _is_databricks_connect()
    
    @classmethod
    def get_session(cls, config_preset: str = "auto") -> SparkSession:
        """
        Get the current Spark session, creating one if necessary.
        
        Args:
            config_preset: Configuration preset to use ('auto', 'local_delta', 'local_basic', 'databricks')
            
        Returns:
            SparkSession: The active Spark session
        """
        workstation = cls()
        
        # Auto-detect environment if preset is 'auto'
        if config_preset == "auto":
            if workstation._is_databricks:
                config_preset = "databricks"
            else:
                config_preset = "local_delta"
        
        # In Databricks, always try to refresh the session reference
        if workstation._is_databricks:
            # Always try to get fresh session in Databricks
            existing_session = workstation._get_existing_databricks_session()
            if existing_session is not None:
                workstation._spark = existing_session
                workstation._is_initialized = True
                return existing_session
        
        # For local or if Databricks session not found, check if current session is active
        if workstation._spark is None or not workstation._is_session_active():
            workstation.start_session(config_preset)
        return workstation._spark
    
    def _get_existing_databricks_session(self) -> Optional[SparkSession]:
        """Try multiple ways to get existing Databricks Spark session."""
        
        # Method 1: Get active session
        try:
            existing_session = SparkSession.getActiveSession()
            if existing_session is not None:
                self.logger.info("Found existing Databricks session via getActiveSession()")
                return existing_session
        except Exception as e:
            self.logger.debug(f"getActiveSession() failed: {e}")
        
        # Method 2: Try to get global spark from builtins
        try:
            import builtins
            if hasattr(builtins, 'spark') and builtins.spark is not None:
                self.logger.info("Found existing Databricks session via builtins.spark")
                return builtins.spark
        except Exception as e:
            self.logger.debug(f"builtins.spark failed: {e}")
        
        # Method 3: Check calling frame's globals for 'spark'
        try:
            import sys
            frame = sys._getframe(2)  # Go up to caller's frame
            while frame:
                if 'spark' in frame.f_globals:
                    spark_obj = frame.f_globals['spark']
                    if hasattr(spark_obj, 'version') and hasattr(spark_obj, 'sql'):
                        self.logger.info("Found existing Databricks session via frame globals")
                        return spark_obj
                frame = frame.f_back
        except Exception as e:
            self.logger.debug(f"Frame globals search failed: {e}")
        
        # Method 4: Try __main__ globals
        try:
            import __main__
            if hasattr(__main__, 'spark') and __main__.spark is not None:
                self.logger.info("Found existing Databricks session via __main__.spark")
                return __main__.spark
        except Exception as e:
            self.logger.debug(f"__main__.spark failed: {e}")
        
        self.logger.warning("Could not find existing Databricks session")
        return None
    
    def _is_session_active(self) -> bool:
        """Check if session is active without accessing problematic attributes."""
        if self._spark is None:
            return False
        
        try:
            # Use a safe method that works in all environments
            self._spark.sql("SELECT 1").collect()
            return True
        except Exception:
            return False
    
    @classmethod
    def is_active(cls) -> bool:
        """Check if a Spark session is currently active."""
        workstation = cls()
        return workstation._is_session_active()
    
    def start_session(self, config_preset: str = "auto", custom_config: Optional[Dict[str, Any]] = None) -> SparkSession:
        """
        Start a new Spark session with the specified configuration.

        Args:
            config_preset: Configuration preset to use
            custom_config: Additional custom configurations

        Returns:
            SparkSession: The newly created Spark session
        """
        
        # Auto-detect if needed
        if config_preset == "auto":
            config_preset = "databricks" if self._is_databricks else "local_delta"
        
        # In Databricks, we should only use existing sessions, never create new ones
        if self._is_databricks:
            existing_session = self._get_existing_databricks_session()
            if existing_session is not None:
                self._spark = existing_session
                self._is_initialized = True
                self.logger.info("Using existing Databricks Spark session")
                return self._spark
            else:
                raise RuntimeError(
                    "Running in Databricks environment but could not find existing Spark session. "
                    "Make sure you're running in a Databricks notebook where 'spark' is available, "
                    "or try restarting your cluster."
                )
        
        # Stop existing session if any (only for local)
        if self._spark is not None and not self._is_databricks:
            self.stop_session()

        # For local environments, set JAVA_HOME if needed
        if not self._is_databricks:
            self._setup_java_home()

        # Get base configuration
        config = self._config_presets.get(config_preset, self._config_presets["local_delta"])

        # Apply custom configurations if provided
        if custom_config:
            config.update(custom_config)
        
        # Only create new sessions in local environments
        if not self._is_databricks:
            # Create SparkSession builder
            builder = SparkSession.builder.appName(config.pop("app_name", "DemandPlanningWorkstation"))
            
            # Apply configurations
            for key, value in config.items():
                builder = builder.config(key, value)
            
            # Configure with Delta Lake if needed
            if "delta" in config_preset:
                try:
                    from delta import configure_spark_with_delta_pip
                    builder = configure_spark_with_delta_pip(builder)
                except ImportError:
                    self.logger.warning("Delta Lake not available, continuing without Delta configuration")
            
            self._spark = builder.getOrCreate()
            
            # Set log level (safe way)
            try:
                self._spark.sparkContext.setLogLevel("WARN")
            except Exception:
                # Ignore if can't set log level
                pass
            
            self._is_initialized = True
            self.logger.info(f"Spark session started in local environment")
        
        return self._spark
    
    def _setup_java_home(self):
        """Set up JAVA_HOME for local environments."""
        if not os.environ.get("JAVA_HOME") or "JavaAppletPlugin" in os.environ.get("JAVA_HOME", ""):
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
    
    def stop_session(self) -> None:
        """Stop the current Spark session and clean up resources."""
        if self._spark is not None and not self._is_databricks:
            # Only stop session in local environment
            try:
                self._spark.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping session: {e}")
            self._spark = None
            self._is_initialized = False
        elif self._is_databricks:
            self.logger.warning("Cannot stop Spark session in Databricks environment")
    
    def restart_session(self, config_preset: str = "auto") -> SparkSession:
        """Restart the Spark session with the specified configuration."""
        if not self._is_databricks:
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
            "environment": "local",
            "error": None
        }
        
        try:
            if self._spark is not None:
                health_status["session_active"] = True
                health_status["version"] = self._spark.version
                health_status["environment"] = "databricks" if self._is_databricks else "local"
                
                # Safe way to get app name
                try:
                    health_status["app_name"] = self._spark.conf.get("spark.app.name")
                except Exception:
                    health_status["app_name"] = "unknown"
                
                # Safe way to get session info without accessing sparkContext
                if not self._is_databricks:
                    try:
                        health_status["session_id"] = self._spark.sparkContext.applicationId
                        health_status["master"] = self._spark.sparkContext.master
                    except Exception:
                        health_status["session_id"] = "unknown"
                        health_status["master"] = "unknown"
                else:
                    health_status["session_id"] = "databricks_managed"
                    health_status["master"] = "databricks"
                
                # Test basic functionality
                try:
                    test_df = self._spark.range(1)
                    test_count = test_df.count()
                    
                    # Check if Delta is available
                    try:
                        # Try to create a simple Delta table operation
                        self._spark.sql("SELECT 1 as test").write.format("delta").mode("overwrite").option("path", "/tmp/delta_test").save()
                        health_status["delta_enabled"] = True
                    except Exception:
                        health_status["delta_enabled"] = False
                        
                except Exception as e:
                    health_status["error"] = f"Basic functionality test failed: {str(e)}"
            else:
                health_status["error"] = "No active Spark session"
                    
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
            "environment": "databricks" if self._is_databricks else "local"
        }
        
        # Add detailed info only for local sessions
        if not self._is_databricks and self._spark:
            try:
                session_info["configurations"] = dict(self._spark.sparkContext.getConf().getAll())
                session_info["default_parallelism"] = self._spark.sparkContext.defaultParallelism
            except Exception:
                session_info["configurations"] = "unavailable_in_databricks_serverless"
        
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
            },
            "databricks": {
                "app_name": "DemandPlanningAgent_Databricks"
                # Databricks manages most configurations automatically
            }
        }


# Convenience functions for easy access
def get_spark(config_preset: str = "auto") -> SparkSession:
    """Get the current Spark session (convenience function)."""
    # In Databricks, just use getOrCreate - simple and reliable
    if _is_databricks_environment():
        return SparkSession.builder.getOrCreate()
    
    # Only use workstation complexity for local environments
    return SparkWorkstation.get_session(config_preset)

def is_spark_active() -> bool:
    """Check if Spark session is active (convenience function)."""
    # In Databricks, check the session directly
    if _is_databricks_environment():
        try:
            spark = SparkSession.builder.getOrCreate()
            spark.sql("SELECT 1").collect()
            return True
        except:
            return False
    
    # For local, use workstation
    return SparkWorkstation.is_active()

def stop_spark() -> None:
    """Stop the current Spark session (convenience function)."""
    workstation = SparkWorkstation()
    workstation.stop_session()

def restart_spark(config_preset: str = "auto") -> SparkSession:
    """Restart Spark session (convenience function)."""
    workstation = SparkWorkstation()
    return workstation.restart_session(config_preset)

def spark_health_check() -> Dict[str, Any]:
    """Perform Spark health check (convenience function)."""
    # In Databricks, simple health check
    if _is_databricks_environment():
        try:
            spark = SparkSession.builder.getOrCreate()
            # Test basic functionality
            spark.sql("SELECT 1").collect()
            
            return {
                "session_active": True,
                "session_id": "databricks_managed",
                "app_name": spark.conf.get("spark.app.name", "databricks"),
                "master": "databricks",
                "version": spark.version,
                "delta_enabled": True,  # Always available in Databricks
                "environment": "databricks",
                "error": None
            }
        except Exception as e:
            return {
                "session_active": False,
                "session_id": None,
                "app_name": None,
                "master": None,
                "version": None,
                "delta_enabled": False,
                "environment": "databricks",
                "error": str(e)
            }
    
    # For local, use workstation
    workstation = SparkWorkstation()
    return workstation.health_check()

def get_spark_info() -> Dict[str, Any]:
    """Get Spark session info (convenience function)."""
    workstation = SparkWorkstation()
    return workstation.get_session_info()

def inject_spark_for_databricks(spark_session: SparkSession) -> None:
    """
    Inject an existing Spark session for Databricks compatibility.
    
    Use this when the automatic detection fails but you have access to the spark session.
    
    Args:
        spark_session: The existing Spark session to use
    """
    workstation = SparkWorkstation()
    workstation._spark = spark_session
    workstation._is_initialized = True
    workstation._is_databricks = True
    workstation.logger.info("Manually injected Databricks Spark session")


if __name__ == "__main__":
    # Demo usage
    print("🚀 Demand Planning Workstation Demo")
    print("=" * 50)
    
    # Detect environment
    is_db = _is_databricks_environment()
    print(f"🏗️  Environment: {'Databricks' if is_db else 'Local'}")
    
    # In Databricks, try to inject the global spark if available
    if is_db:
        try:
            # Try to get spark from the calling environment
            import sys
            frame = sys._getframe()
            while frame:
                if 'spark' in frame.f_globals and hasattr(frame.f_globals['spark'], 'version'):
                    print("🔍 Found spark in globals, injecting...")
                    inject_spark_for_databricks(frame.f_globals['spark'])
                    break
                frame = frame.f_back
        except Exception as e:
            print(f"⚠️  Could not auto-inject spark: {e}")
    
    # Start session
    try:
        spark = get_spark("auto")  # Auto-detect environment
        print(f"✅ Spark session started: {spark.version}")
        
        # Health check
        health = spark_health_check()
        print(f"📊 Health check: {health['session_active']}")
        print(f"🌍 Environment: {health['environment']}")
        print(f"📦 Delta enabled: {health['delta_enabled']}")
        
        # Create test DataFrame
        workstation = SparkWorkstation()
        test_df = workstation.create_test_dataframe()
        print(f"🧪 Test DataFrame created with {test_df.count()} rows")
        
        print("\n✅ Workstation demo completed successfully!")
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        if is_db:
            print("💡 In Databricks, try: inject_spark_for_databricks(spark)")
        else:
            print("💡 Make sure Java is properly configured for local environment")
