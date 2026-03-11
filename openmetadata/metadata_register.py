"""
OpenMetadata Registration Workflow for DEA Project.
This module handles the registration of all metadata entities in OpenMetadata for the
DEA (Data Envelopment Analysis) project. It creates and manages:
- Datalake source tables (BigQuery basedosdados);
- Storage services (GCS bronze, silver and gold layers);
- Gold dataset in PostgreSQL DataWarehouse (Neon);
- Dashboard service (PowerBI);
- Lineage relationships between all entities.
The script reads a YAML configuration file and uses validation schemas
to ensure metadata matches actual data structures.
"""

import yaml
import logging
import os
from typing import Dict, Any, List
import pandera as pa
from etl.diagnostics.data_validation import gold_schema

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import AuthProvider

from metadata.generated.schema.entity.data.table import Column as OMColumn
from metadata.generated.schema.entity.data.table import DataType
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest

from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.api.services.createStorageService import CreateStorageServiceRequest
from metadata.generated.schema.api.services.createDashboardService import CreateDashboardServiceRequest

from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.entity.services.dashboardService import DashboardServiceType

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("metadata").setLevel(logging.WARNING)

# --------------------------------------------------
# Validation fetching
# --------------------------------------------------

def pandera_to_om_columns(schema: pa.DataFrameSchema) -> List[OMColumn]:
    """
    Convert a Pandera DataFrameSchema to a list of OpenMetadata Column objects.
    Maps Pandera data types to OpenMetadata DataType enums and creates Column
    objects with appropriate type information.
    Args:
        schema: Pandera DataFrameSchema containing column definitions
    Returns:
        List[OMColumn]: List of OpenMetadata Column objects ready for table creation
    Raises:
        Exception: If conversion fails due to unexpected schema structure
    """
    try:
        dtype_map = {"int64": DataType.INT,
                     "int32": DataType.INT,
                     "float64": DataType.FLOAT,
                     "float32": DataType.FLOAT,
                     "object": DataType.STRING,
                     "bool": DataType.BOOLEAN,
                  }

        columns = []
        for name, col in schema.columns.items():
            dtype = str(col.dtype)
            columns.append(OMColumn(name = name,
                                    dataType = dtype_map.get(dtype, DataType.STRING),
                                    description = None
                                )
                        )
        return columns
    except Exception as e:
        logger.error(f"Error converting pandera schema to OpenMetadata columns: {e}")
        raise

# --------------------------------------------------
# OpenMetadata Client
# --------------------------------------------------
class OMClient:
    """
    OpenMetadata API client instance for API calls wrapper.
    Handles authentication and a configured client instance
    for interacting with the OpenMetadata server.
    """

    def __init__(self, config_path: str = "configs/metadata_registry.yaml") -> None:
        """
        Initialize OpenMetadata client with configuration from YAML file.
        Args:
            config_path: Path to YAML configuration file with OpenMetadata connection settings
        Raises:
            ValueError: If OPENMETADATA_JWT_TOKEN environment variable is not set
            Exception: If connection to OpenMetadata server fails
        """
        try:
            with open(config_path) as f:
                config = yaml.safe_load(f)
            token = os.getenv("OPENMETADATA_JWT_TOKEN")
            if not token:
                raise ValueError("OPENMETADATA_JWT_TOKEN missing")

            connection = OpenMetadataConnection(hostPort = f"{config['host']}/api",
                                                authProvider = AuthProvider.openmetadata,
                                                securityConfig = OpenMetadataJWTClientConfig(jwtToken = token),
                                            )
            self.client = OpenMetadata(connection)
        except Exception as e:
            logger.error(f"Error initializing OpenMetadata client: {e}")
            raise

# --------------------------------------------------
# Registry Loader
# --------------------------------------------------
class Registry:
    """
    YAML configuration registry loader.
    Loads and provides access to the metadata dictionary registry YAML file containing
    definitions for services, databases, schemas, containers, tables, and lineage.
    """

    def __init__(self, path) -> None:
        """
        YAML configuration
        Args:
            path: Path to YAML configuration file
        """
        with open(path) as f:
            self.registry = yaml.safe_load(f)

    def get(self, section: str) -> Dict[str, Any]:
        """
        Get a section from the registry.
        Args:
            section: Section name to retrieve (e.g., 'services', 'tables')
        Returns:
            Dictionary containing the requested section, or empty dict if not found
        """
        return self.registry.get(section, {})

# --------------------------------------------------
# Metadata Bootstrap
# --------------------------------------------------
class MetadataBootstrap:
    """
    Metadata registration orchestrator.
    Coordinates the creation of all OpenMetadata entities including services,
    databases, schemas, containers, tables, and lineage relationships.
    OpenMetadata client for API calls; 
    Registry instance with YAML configuration;
    Dictionary tracking all created entities by their keys.
    """

    def __init__(self, 
                 client: OpenMetadata, 
                 registry: Registry) -> None:
        """
        Initialize bootstrap with client and registry.
        Args:
            client: OpenMetadata client instance
            registry: Registry instance with loaded configuration
        """
        self.client = client
        self.registry = registry
        self.entities = {}

    # --------------------------------------------------
    # SERVICES
    # --------------------------------------------------

    def create_services(self) -> None:
        """
        Create services defined in the registry.
        Processes database, storage, and dashboard service groups 
        and creates them in OpenMetadata
        Raises:
            ValueError: If unknown service group is encountered
            Exception: If service creation fails
        """
        try:
            services = self.registry.get("services")
            for group_name, group in services.items():
                for key, svc in group.items():
                    if group_name == "database":
                        request = CreateDatabaseServiceRequest(name = svc["name"],
                                                            serviceType = DatabaseServiceType[svc["type"]],
                                                            description = svc.get("description"),
                                                            )

                    elif group_name == "storage":
                        request = CreateStorageServiceRequest(name = svc["name"],
                                                            serviceType = StorageServiceType[svc["type"]],
                                                            description = svc.get("description"),
                                                            connection = svc.get("connection")
                                                            )

                    elif group_name == "dashboard":
                        request = CreateDashboardServiceRequest(name = svc["name"],
                                                                serviceType = DashboardServiceType[svc["type"]],
                                                                description = svc.get("description"),
                                                            )

                    else:
                        raise ValueError(f"Unknown service group: {group_name}")
                    service = self.client.create_or_update(request)
                    self.entities[key] = service
        except Exception as e:
            logger.error(f"Error creating services: {e}")
            raise

    # --------------------------------------------------
    # CREATE BIGQUERY SOURCE TABLES
    # --------------------------------------------------
    def create_source_tables(self) -> None:
        """
        Create BigQuery source tables defined in the registry.
        Creates the source tables in the BigQuery service
        Without columns initially, as they are populated by ETL process.
        Raises:
            Exception: If table creation fails
        """
        try:
            tables = self.registry.get("tables") or {}
            services = self.registry.get("services") or {}
            db_services = services.get("database") or {}

            for key, t in tables.items():
                service_name = db_services[t["service"]]["name"]
                schema_fqn = f"{service_name}.{t['database']}.{t['schema']}"
                request = CreateTableRequest(name = t["name"],
                                            databaseSchema = schema_fqn,
                                            description = t.get("description"),
                                            columns = []
                                        )
                table = self.client.create_or_update(request)
                self.entities[key] = table
        except Exception as e:
            logger.error(f"Error creating source tables: {e}")
            raise

    # --------------------------------------------------
    # CONTAINERS
    # --------------------------------------------------
    def create_containers(self) -> None:
        """
        Create GCS containers for medallion layers.
        Creates layers containers representing
        different stages in medallion architecture.
        Raises:
            Exception: If container creation fails
        """
        try:
            containers = self.registry.get("containers")
            services = self.registry.get("services").get("storage")
            for key, c in containers.items():
                service_key = c["service"]
                service_name = services[service_key]["name"]
                request = CreateContainerRequest(name = key,
                                                 service = service_name,
                                                 description = c.get("description"),
                                            )
                container = self.client.create_or_update(request)
                self.entities[key] = container
        except Exception as e:
            logger.error(f"Error creating containers: {e}")
            raise

    # --------------------------------------------------
    # TABLES
    # --------------------------------------------------
    def create_tables(self) -> None:
        """
        Create the gold dataset table in PostgreSQL.
        Final gold dataset with complete schema from validation.
        Raises:
            Exception: If gold table creation fails
        """
        try:
            services = self.registry.get("services") or {}
            db_services = services.get("database") or {}
            postgres_service = db_services.get("postgres_warehouse", {}).get("name")
            if not postgres_service:
                logger.error("Postgres service not found")
                return
            db_fqn = f"{postgres_service}.analytics"
            schema_fqn = f"{db_fqn}.public"
            columns = pandera_to_om_columns(gold_schema)
            request = CreateTableRequest(name = "gold_dataset",
                                         databaseSchema = schema_fqn,
                                         columns = columns,
                                         description = "DEA efficiency scores and analysis"
                                    )
            gold_table = self.client.create_or_update(request)
            self.entities["gold_dataset"] = gold_table
        except Exception as e:
            logger.error(f"Error creating PostgreSQL table: {e}")
            raise

    # --------------------------------------------------
    # Dashboard
    # --------------------------------------------------
    def create_dashboard(self) -> None:
        """
        Create PowerBI dashboard entity.
        Consume data from PostgreSQL gold dataset for visualization and analysis.
        Raises:
            Exception: If dashboard creation fails
        """
        try:
            dashboards = self.registry.get("dashboards")
            services = self.registry.get("services").get("dashboard")

            for key, d in dashboards.items():
                service_key = d["service"]
                service_name = services[service_key]["name"]
                request = CreateDashboardRequest(name = key,
                                                 service = service_name,
                                                 description = d.get("description"),
                                            )
                dashboard = self.client.create_or_update(request)
                self.entities[key] = dashboard
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            raise

    # --------------------------------------------------
    # LINEAGE
    # --------------------------------------------------
    def create_lineage(self) -> None:
        """
        Create lineage relationships between all entities.
        Establishes the complete data flow from source tables through containers
        to datawarehouse and finally to dashboard.
        Raises:
            Exception: If lineage creation fails
        """
        try:
            edges = self.registry.get("lineage")
            for edge in edges:
                src = self.entities.get(edge["from"])
                tgt = self.entities.get(edge["to"])
                if not src or not tgt:
                    logger.warning(f"Skipping lineage {edge}")
                    continue

                src_type = src.__class__.__name__.lower()
                tgt_type = tgt.__class__.__name__.lower()

                request = AddLineageRequest(edge={
                                "fromEntity": EntityReference(id = src.id,
                                                              type = src_type
                                                            ),
                                "toEntity": EntityReference(id = tgt.id,
                                                            type = tgt_type
                                                        ),
                                                }
                                        )
                self.client.add_lineage(request)
        except Exception as e:
            logger.error(f"Error creating lineage: {e}")
            raise

# ---------------------------------------------------------------------
# Main Workflow Function
# ---------------------------------------------------------------------
def metadata_setup(config_path: str = "configs/metadata_registry.yaml") -> None:
    """
    Orchestrate the complete OpenMetadata registration workflow.
    1. Loads configuration from YAML file
    2. Initializes OpenMetadata client
    3. Creates all services (BigQuery, PostgreSQL, GCS, PowerBI)
    4. Creates GCS containers for medallion layers
    5. Creates BigQuery source tables
    6. Creates PostgreSQL gold table with complete schema
    7. Creates PowerBI dashboard entity
    8. Establishes lineage relationships between all entities
    Args:
        config_path: Path to YAML configuration file with OpenMetadata settings
    Raises:
        Exception: If any critical step in the workflow fails
    """
    try:
        # Load configuration
        registry = Registry(config_path)
        # Initialize OpenMetadata client
        om_client = OMClient(config_path)
        # Create metadata bootstrap instance
        bootstrap = MetadataBootstrap(om_client.client, 
                                      registry)
        # Execute all creation steps in order
        bootstrap.create_services()
        bootstrap.create_containers()
        bootstrap.create_source_tables()
        bootstrap.create_tables()
        bootstrap.create_dashboard()
        bootstrap.create_lineage()
        print("Metadata completed")
    except Exception as e:
        logger.error(f"Metadata registration failed: {e}")
        raise

if __name__ == "__main__":
    metadata_setup()