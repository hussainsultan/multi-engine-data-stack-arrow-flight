from typing import Any, Dict 

from xorq.flight.client import FlightClient

from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig
from dbt.adapters.duckdb.utils import TargetConfig
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("DuckDB")


class Plugin(BasePlugin):
    """
    An experimental dbt-duckdb plugin for connecting to xorq Flight servers with Iceberg tables.
    """
    
    def initialize(self, plugin_config: Dict[str, Any]):
        self._config = plugin_config
        try:
            self._client = FlightClient(
                host=self._config["host"],
                port=self._config["port"],
            )
            logger.info(f"Connected to Flight server at {self._config['host']}:{self._config['port']}")
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Flight server: {e}")

    def load(self, source_config: SourceConfig):
        table_name = None
        if "table_name" in source_config.meta:
            table_name = source_config.meta["table_name"]
        elif "table_name" in source_config:
            table_name = source_config["table_name"]
        
        if not table_name:
            table_name = source_config.identifier
        
        logger.info(f"Loading data from Iceberg table: {table_name}")
        
        try:
            import xorq as xo
            import pyarrow as pa
            # FIXME: avoid hardcoding and interop with sources.yml
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("value", pa.string(), nullable=False),
                ]
            )
            
            table_ref = xo.table(name=table_name, schema=schema)

            result = self._client.execute_query(table_ref.as_table())
            
            return result
        except Exception as e:
            logger.error(f"Error loading data from Flight server: {e}")
            raise RuntimeError(f"Failed to load data from Flight server: {e}")

    def store(self, target_config: TargetConfig):
        if not target_config.location:
            raise ValueError("Target config does not have a location")
        
        table_name = target_config.relation.identifier
        location_path = target_config.location.path
        
        logger.info(f"Storing data to Iceberg table: {table_name} from {location_path}")
        pass

    def default_materialization(self):
        return "table"
