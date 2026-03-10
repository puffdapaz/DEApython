import yaml
import logging
import os
import pandera as pa
from etl.diagnostics.data_validation import (schemas,
                                             silver_schema,
                                             gold_schema
                                        )

from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection, AuthProvider

from metadata.generated.schema.entity.services.databaseService import DatabaseServiceType
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.entity.data.table import Table

from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest

from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.type.entityReference import EntityReference

from metadata.generated.schema.api.services.createStorageService import CreateStorageServiceRequest
from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
from metadata.generated.schema.api.services.createDashboardService import CreateDashboardServiceRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest

from metadata.generated.schema.entity.services.storageService import StorageServiceType
from metadata.generated.schema.entity.services.pipelineService import PipelineServiceType
from metadata.generated.schema.entity.services.dashboardService import DashboardServiceType
from metadata.generated.schema.entity.data.table import Column as OMColumn
from metadata.generated.schema.entity.data.table import DataType
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------
# Validation fetching
# --------------------------------------------------

def pandera_to_om_columns(schema: pa.DataFrameSchema):

    dtype_map = {
        "int64": DataType.INT,
        "int32": DataType.INT,
        "float64": DataType.FLOAT,
        "float32": DataType.FLOAT,
        "object": DataType.STRING,
        "bool": DataType.BOOLEAN,
    }

    columns = []

    for name, col in schema.columns.items():

        dtype = str(col.dtype)

        columns.append(
            OMColumn(
                name=name,
                dataType=dtype_map.get(dtype, DataType.STRING),
                description=None
            )
        )

    return columns

# --------------------------------------------------
# OpenMetadata Client
# --------------------------------------------------

class OMClient:

    def __init__(self, config_path="configs/metadata_registry.yaml"):

        with open(config_path) as f:
            config = yaml.safe_load(f)

        token = os.getenv("OPENMETADATA_JWT_TOKEN")
        if not token:
            raise ValueError("OPENMETADATA_JWT_TOKEN missing")

        connection = OpenMetadataConnection(
            hostPort=f"{config['host']}/api",
            authProvider=AuthProvider.openmetadata,
            securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
        )

        self.client = OpenMetadata(connection)


# --------------------------------------------------
# Registry Loader
# --------------------------------------------------

class Registry:

    def __init__(self, path):

        with open(path) as f:
            self.registry = yaml.safe_load(f)

    def get(self, section):
        return self.registry.get(section, {})


# --------------------------------------------------
# Metadata Bootstrap
# --------------------------------------------------

class MetadataBootstrap:

    def __init__(self, client, registry):

        self.client = client
        self.registry = registry
        self.entities = {}

    # --------------------------------------------------
    # SERVICES
    # --------------------------------------------------

    def create_services(self):

        services = self.registry.get("services")

        for group_name, group in services.items():

            for key, svc in group.items():

                logger.info(f"Creating service: {svc['name']}")

                if group_name == "database":

                    request = CreateDatabaseServiceRequest(
                        name=svc["name"],
                        serviceType=DatabaseServiceType[svc["type"]],
                        description=svc.get("description"),
                    )

                elif group_name == "storage":

                    request = CreateStorageServiceRequest(
                        name=svc["name"],
                        serviceType=StorageServiceType[svc["type"]],
                        description=svc.get("description"),
                        connection=svc.get("connection")
                    )

                elif group_name == "pipeline":

                    request = CreatePipelineServiceRequest(
                        name=svc["name"],
                        serviceType=PipelineServiceType[svc["type"]],
                        description=svc.get("description"),
                    )

                elif group_name == "dashboard":

                    request = CreateDashboardServiceRequest(
                        name=svc["name"],
                        serviceType=DashboardServiceType[svc["type"]],
                        description=svc.get("description"),
                    )

                else:
                    raise ValueError(f"Unknown service group: {group_name}")

                service = self.client.create_or_update(request)

                self.entities[key] = service

    # --------------------------------------------------
    # DATABASES
    # --------------------------------------------------

    def create_databases(self):

        databases = self.registry.get("databases")
        services = self.registry.get("services").get("database")

        for key, db in databases.items():

            service_name = services[db["service"]]["name"]

            logger.info(f"Creating database: {db['name']}")

            request = CreateDatabaseRequest(
                name=db["name"],
                service=service_name,
                description=db.get("description"),
            )

            database = self.client.create_or_update(request)

            self.entities[key] = database    

    # --------------------------------------------------
    # SCHEMAS
    # --------------------------------------------------

    def create_schemas(self):

        schemas = self.registry.get("schemas")
        databases = self.registry.get("databases")

        for key, s in schemas.items():

            database_name = databases[s["database"]]["name"]
            service_name = self.registry.get("services")["database"][s["service"]]["name"]

            fqn = f"{service_name}.{database_name}"

            logger.info(f"Creating schema: {s['name']}")

            request = CreateDatabaseSchemaRequest(
                name=s["name"],
                database=fqn,
                description=s.get("description"),
            )

            schema = self.client.create_or_update(request)

            self.entities[key] = schema

    # --------------------------------------------------
    # CONTAINERS
    # --------------------------------------------------

    def create_containers(self):

        containers = self.registry.get("containers")

        services = self.registry.get("services").get("storage")

        for key, c in containers.items():

            logger.info(f"Creating container: {key}")

            service_key = c["service"]

            service_name = services[service_key]["name"]

            request = CreateContainerRequest(
                name=key,
                service=service_name,
                description=c.get("description"),
            )

            container = self.client.create_or_update(request)

            self.entities[key] = container

    # --------------------------------------------------
    # TABLES
    # --------------------------------------------------

    def create_tables(self):
        """Create ONLY the gold table in PostgreSQL"""
        
        services = self.registry.get("services") or {}
        db_services = services.get("database") or {}
        
        # Get Postgres service
        postgres_service = db_services.get("postgres_warehouse", {}).get("name")
        if not postgres_service:
            logger.error("Postgres service not found")
            return
        
        # Ensure database and schema exist
        db_fqn = f"{postgres_service}.analytics"
        schema_fqn = f"{db_fqn}.public"
        
        # Create gold table from gold_schema
        columns = pandera_to_om_columns(gold_schema)
        
        request = CreateTableRequest(
            name="gold_dataset",
            databaseSchema=schema_fqn,
            columns=columns,
            description="DEA efficiency scores and analysis"
        )
        
        try:
            gold_table = self.client.create_or_update(request)
            self.entities["gold_dataset"] = gold_table
            logger.info(f"✅ Created gold table with {len(columns)} columns")
        except Exception as e:
            logger.error(f"Failed to create gold table: {e}")

    # --------------------------------------------------
    # Dashboard
    # --------------------------------------------------
    
    def create_dashboard(self):

        dashboards = self.registry.get("dashboards")
        services = self.registry.get("services").get("dashboard")

        for key, d in dashboards.items():

            logger.info(f"Creating dashboard: {key}")

            service_key = d["service"]
            service_name = services[service_key]["name"]

            request = CreateDashboardRequest(
                name=key,
                service=service_name,
                description=d.get("description"),
            )

            dashboard = self.client.create_or_update(request)

            self.entities[key] = dashboard

    # --------------------------------------------------
    # REGISTER EXTERNAL ENTITIES
    # --------------------------------------------------

    def resolve_external_entities(self):

        tables = self.registry.get("tables")

        if not tables:
            logger.info("No tables defined in registry")
            return

        services = self.registry.get("services") or {}
        db_services = services.get("database") or {}

        for key, t in tables.items():

            service_name = db_services.get(t["service"], {}).get("name")

            if not service_name:
                logger.warning(f"Service not found for table {key}")
                continue

            fqn = f"{service_name}.{t['database']}.{t['schema']}.{t['name']}"

            try:

                entity = self.client.get_by_name(
                    entity=Table,
                    fqn=fqn
                )

                if entity:
                    logger.info(f"Resolved table: {fqn}")
                    self.entities[key] = entity

            except Exception:

                logger.warning(f"Table not found yet: {fqn}")

    # --------------------------------------------------
    # LINEAGE
    # --------------------------------------------------

    def create_lineage(self):
        edges = self.registry.get("lineage")

        for edge in edges:
            src = self.entities.get(edge["from"])
            tgt = self.entities.get(edge["to"])

            if not src or not tgt:
                logger.warning(f"Skipping lineage {edge}")
                continue

            # Use lowercase for entity types
            src_type = src.__class__.__name__.lower()
            tgt_type = tgt.__class__.__name__.lower()

            request = AddLineageRequest(
                edge={
                    "fromEntity": EntityReference(
                        id=src.id,
                        type=src_type
                    ),
                    "toEntity": EntityReference(
                        id=tgt.id,
                        type=tgt_type
                    ),
                }
            )

            self.client.add_lineage(request)
            logger.info(f"Lineage created: {edge['from']} -> {edge['to']}")

    # --------------------------------------------------
    # RUN
    # --------------------------------------------------

    def run(self):

        logger.info("Bootstrapping metadata")

        self.create_services()
        self.create_containers()
        self.create_databases()
        self.create_schemas()
        self.create_tables()
        self.create_dashboard()
        self.resolve_external_entities()
        self.create_lineage()

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    registry = Registry("configs/metadata_registry.yaml")
    
    om = OMClient("configs/metadata_registry.yaml")

    bootstrap = MetadataBootstrap(om.client, registry)

    bootstrap.run()


if __name__ == "__main__":
    main()