"""
One-stop script to register your DEA project in OpenMetadata.
Fully dynamic - reads both YAML files and registers everything.
"""
import yaml
import logging
import os
from metadata.generated.schema.api.data.createContainer import CreateContainerRequest
from metadata.generated.schema.api.data.createDashboard import CreateDashboardRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest
from metadata.generated.schema.api.data.createPipeline import CreatePipelineRequest
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest
from metadata.generated.schema.api.services.createDashboardService import CreateDashboardServiceRequest
from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
from metadata.generated.schema.api.services.createPipelineService import CreatePipelineServiceRequest
from metadata.generated.schema.api.services.createStorageService import CreateStorageServiceRequest
from metadata.generated.schema.entity.data.container import Container
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.services.connections.dashboard.powerBIConnection import PowerBIConnection
from metadata.generated.schema.entity.services.connections.database.bigQueryConnection import BigQueryConnection
from metadata.generated.schema.entity.services.connections.database.postgresConnection import PostgresConnection
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import AuthProvider
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import OpenMetadataConnection
from metadata.generated.schema.entity.services.connections.pipeline.airflowConnection import AirflowConnection
from metadata.generated.schema.entity.services.connections.storage.gcsConnection import GcsConnection
from metadata.generated.schema.entity.services.dashboardService import DashboardConnection
from metadata.generated.schema.entity.services.dashboardService import DashboardService
from metadata.generated.schema.entity.services.databaseService import DatabaseConnection
from metadata.generated.schema.entity.services.databaseService import DatabaseService
from metadata.generated.schema.entity.services.pipelineService import PipelineConnection
from metadata.generated.schema.entity.services.pipelineService import PipelineService
from metadata.generated.schema.entity.services.storageService import StorageConnection
from metadata.generated.schema.entity.services.storageService import StorageService
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import OpenMetadataJWTClientConfig
from metadata.generated.schema.type.entityLineage import EntitiesEdge
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata

# Set Up Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class MetadataManager:
    """OpenMetadata connection manager"""
    def __init__(self, config_path: str = "configs/openmetadata.yaml"):
        try:
            with open(config_path, "r") as f:
                self.config = yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Error loading configs: {e}")
            raise

        # token
        token = os.getenv("OPENMETADATA_JWT_TOKEN")
        if not token:
            raise ValueError("OPENMETADATA_JWT_TOKEN not set in environment")

        # Connection to OpenMetadata
        server_config = OpenMetadataConnection(
            hostPort = f"{self.config['host']}/api",
            authProvider = AuthProvider.openmetadata,
            securityConfig = OpenMetadataJWTClientConfig(jwtToken = token)
        )
        try:
            self.client = OpenMetadata(server_config)
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise

    def ensure_postgres_schema(self, 
                               service_name: str, 
                               database_name: str, 
                               schema_name: str):
        db_fqn = f"{service_name}.{database_name}"
        schema_fqn = f"{db_fqn}.{schema_name}"

        # Database
        db = self.client.get_by_name(Database, 
                                     db_fqn)
        if not db:
            db = self.client.create(
                CreateDatabaseRequest(name = database_name,
                                      service = service_name
                                    )
                                )

        # Schema
        schema = self.client.get_by_name(DatabaseSchema, 
                                         schema_fqn)
        if not schema:
            self.client.create(
                CreateDatabaseSchemaRequest(name = schema_name,
                                            database = EntityReference(id = db.id,
                                                                     type = "database"
                                                                )
                                                            )
                                                        )

            # Re-fetch
            schema = self.client.get_by_name(DatabaseSchema, 
                                             schema_fqn)
            if not schema:
                raise RuntimeError(f"Schema {schema_fqn} was not created")
        return schema

    def get_or_create_service(self, service_type: str, service_key: str, service_config: dict):
        """Get or create a service from config"""
        service_map = {"bigquery": DatabaseService,
                       "postgres": DatabaseService,
                       "gcs": StorageService,
                       "pipeline": PipelineService,
                       "powerbi": DashboardService
                     }

        type_map = {"bigquery": "database",
                    "postgres": "database",
                    "gcs": "storage",
                    "pipeline": "pipeline",
                    "powerbi": "dashboard"
                 }
        
        # Service types map
        service_type_enum_map = {"bigquery": "BigQuery",
                                 "postgres": "Postgres",
                                 "gcs": "GCS",
                                 "pipeline": "Airflow",
                                 "powerbi": "PowerBI"
                            }

        service_class = service_map.get(service_config['type'])
        service_type_name = type_map.get(service_config['type'])

        if not service_class:
            raise ValueError(f"Unknown service type: {service_config['type']}")

        existing = self.client.get_by_name(service_class, service_config['name'])
        if existing:
            logger.info(f"Service exists: {service_config['name']}")
            return existing

        create_map = {"database": CreateDatabaseServiceRequest,
                      "storage": CreateStorageServiceRequest,
                      "pipeline": CreatePipelineServiceRequest,
                      "dashboard": CreateDashboardServiceRequest
                   }

        # Connection config based on service type
        connection_config = self._build_connection_config(service_config)
        enum_service_type = service_type_enum_map.get(service_config['type'])
        if not enum_service_type:
            raise ValueError(f"No enum mapping for service type: {service_config['type']}")
        # Wrap connection per service type
        if service_type_name == "database":
            connection_wrapper = DatabaseConnection(config = connection_config)
        elif service_type_name == "storage":
            connection_wrapper = StorageConnection(config = connection_config)
        elif service_type_name == "pipeline":
            connection_wrapper = PipelineConnection(config = connection_config)
        elif service_type_name == "dashboard":
            connection_wrapper = DashboardConnection(config = connection_config)
        else:
            raise ValueError(f"Unsupported service type: {service_type_name}")

        create_request = create_map[service_type_name](
            name = service_config['name'],
            serviceType = enum_service_type,
            connection = connection_wrapper,
            description = service_config.get('description', '')
        )
        new_service = self.client.create(create_request)
        return new_service

    def _build_connection_config(self, 
                                 service_config):
        """Return a properly typed connection object for OpenMetadata"""
        if service_config['type'] == 'bigquery':
            return BigQueryConnection(type = "BigQuery",
                                      credentials = {"gcpConfig": {"type": "service_account",
                                                                 "projectId": "basedosdados",
                                                                }
                                                }
                                )
        elif service_config['type'] == 'postgres':
            return PostgresConnection(type = "Postgres",
                                      hostPort = f"{os.getenv('NEON_HOST')}:{os.getenv('NEON_PORT', '5432')}",
                                      username = os.getenv("NEON_USER"),
                                      authType = {"password": os.getenv("NEON_PASSWORD")},
                                      database = "neondb"
                                )
        elif service_config['type'] == 'gcs':
            return GcsConnection(type = "GCS",
                                 credentials = {"gcpConfig": {"type": "service_account",}
                                            }
                            )
        elif service_config['type'] == 'pipeline':
            return AirflowConnection(type = "Airflow",
                                     hostPort = "http://localhost:8080"
                                    )
        elif service_config['type'] == 'powerbi':
            return PowerBIConnection(type = "PowerBI",
                                     clientId = os.getenv("POWERBI_CLIENT_ID", ""),
                                     clientSecret = os.getenv("POWERBI_CLIENT_SECRET", ""),
                                     tenantId = os.getenv("POWERBI_TENANT_ID", "")
            )
        else:
            raise ValueError(f"Unknown service type: {service_config['type']}")

    def create_container(self, 
                         container_name: str, 
                         storage_service, 
                         layer: str, 
                         description: str = ""):
        """Create or get a container for a GCS layer"""
        if hasattr(storage_service.name, "root"):
            service_name = storage_service.name.root
        else:
            service_name = str(storage_service.name)
        container_fqn = f"{service_name}.{container_name}"
        existing = self.client.get_by_name(Container, container_fqn)
        if existing:
            return existing

        # Otherwise create it
        container = CreateContainerRequest(name = container_name,
                                           displayName = f"{layer.capitalize()} Layer",
                                           description = description,
                                           service = service_name,
                                           dataModel = {"isPartitioned": False, "columns": []}
                                        )
        new_container = self.client.create(container)
        return new_container

    def create_table(self, 
                     table_name: str, 
                     schema_fqn: str, 
                     columns: list,
                     tags: list = None, 
                     description: str = ""):
        """Create or update a table inside a database schema"""
        om_columns = [
            {
                "name": col["name"],
                "dataType": col["type"],
                "description": col.get("description", "")
            }
            for col in columns
        ]

        table_request = CreateTableRequest(name = table_name,
                                           displayName = table_name.replace("_", " ").title(),
                                           description = description or f"DEA table: {table_name}",
                                           columns = om_columns,
                                           tableType = "Regular",
                                           databaseSchema = schema_fqn,
                                           tags = [{"tagFQN": tag} for tag in (tags or [])]
                                        )
        created_table = self.client.create_or_update(table_request)
        return created_table

    def create_pipeline(self, 
                        pipeline_name: str, 
                        pipeline_service_id, 
                        tasks: list, 
                        description: str = ""):
        """Create a pipeline from config"""
        pipeline = CreatePipelineRequest(name = pipeline_name,
                                         displayName = pipeline_name.replace('_', ' ').title(),
                                         description = description,
                                         service = EntityReference(id = pipeline_service_id, 
                                                                  type = "pipelineService"),
                                         tasks = [{"name": task, 
                                                   "taskType": "Python"} for task in tasks]
                                    )
        new_pipeline = self.client.create(pipeline)
        return new_pipeline

    def create_dashboard(self, 
                         dashboard_name: str, 
                         dashboard_service_id, 
                         description: str = ""):
        """Create a dashboard"""
        dashboard = CreateDashboardRequest(name = dashboard_name,
                                           displayName = dashboard_name.replace('_', ' ').title(),
                                           description = description,
                                           service = EntityReference(id = dashboard_service_id, 
                                                                     type = "dashboardService"),
                                           charts = []
                                        )
        new_dashboard = self.client.create(dashboard)
        return new_dashboard

    def add_lineage(self, 
                    from_entity, 
                    to_entity, 
                    description: str = ""):
        """Add lineage between entities"""

        lineage = AddLineageRequest(edge = EntitiesEdge(fromEntity = EntityReference(id = from_entity.id, 
                                                                                     type = from_entity.__class__.__name__.lower()),
                                                      toEntity = EntityReference(id = to_entity.id, 
                                                                                 type = to_entity.__class__.__name__.lower()),
                                                      description = description
                                                    )
                                                )
        self.client.add_lineage(lineage)

def register_project():
    """Dynamically register DEA project from YAML configs"""
    # Load  YAML files
    try:
        with open("configs/openmetadata.yaml", "r") as f:
            om_config = yaml.safe_load(f)
        with open("configs/lineage.yaml", "r") as f:
            lineage_config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Error loading configs: {e}")
        raise

    # Connect to OpenMetadata
    om = MetadataManager()

    # Create ALL services from openmetadata.yaml
    services = {}

    # Create services with proper existence checks
    for service_key, service_info in om_config['services'].items():
        service_type = service_info['type']
        service_name = service_info['name']

        if service_type in ['pipeline', 'powerbi']:
            continue
        if service_type in ['bigquery', 'postgres']:
            existing = om.client.get_by_name(DatabaseService, 
                                             service_name)
            if existing:
                services[service_key] = {"entity": existing,
                                         "name": service_name
                                    }
                continue
        elif service_type == 'gcs':

            existing = om.client.get_by_name(StorageService, 
                                             service_name)
            if existing:
                services[service_key] = {"entity": existing,
                                         "name": service_name
                                    }
                continue
        if service_type in ['bigquery', 'postgres']:
            service = om.get_or_create_service(service_type = service_type,
                                               service_key = service_key,
                                               service_config = service_info
                                             )
            services[service_key] = {"entity": service,
                                     "name": service_name
                                }
        elif service_type == 'gcs':
            create_request = CreateStorageServiceRequest(name = service_name,
                                                         serviceType = "GCS",
                                                         connection = StorageConnection(config = GcsConnection(type = "GCS",
                                                                                                               credentials = {"gcpConfig": {
                                                                                                                              "type": "service_account",
                                                                                                                                        }
                                                                                                                        }                                
                                                                                                            )
                                                                                    ),
                                                         description = service_info.get('description', '')
                                                    )
            new_service = om.client.create(create_request)
            services[service_key] = {"entity": new_service,
                                     "name": service_name
                                }

    #  GCS containers for each layer
    containers = {}
    for layer in ['gcs_bronze', 
                  'gcs_silver', 
                  'gcs_gold']:
        if layer in services:
            layer_name = layer.replace('gcs_', '')

            container = om.create_container(container_name = f"{layer_name}_layer",
                                            storage_service = services[layer]["entity"],
                                            layer = layer_name,
                                            description = f"{layer_name.capitalize()} layer container"
                                        )
            containers[layer_name] = container

    # Postgres tables for gold layer
    tables = {}
    postgres_service = services.get("neon_postgres")
    if postgres_service:
        postgres = services["neon_postgres"]

        schema = om.ensure_postgres_schema(service_name = postgres["name"],
                                           database_name = "neondb",
                                           schema_name = "public"
                                        )
        schema_fqn = (schema.fullyQualifiedName.root
                      if hasattr(schema.fullyQualifiedName, "root")
                      else str(schema.fullyQualifiedName)
                    )

        for asset in lineage_config['assets']:
            if asset['layer'] == "gold":
                table = om.create_table(table_name = asset['name'],
                                        schema_fqn = schema_fqn,
                                        columns = asset['columns'],
                                        tags = asset.get('tags', []),
                                        description = asset.get("description", "")
                                    )
                tables[asset['name']] = table

    # Create lineage connections 
    # Container lineage
    if 'gcs_bronze' in services and 'gcs_silver' in services:
        om.add_lineage(from_entity = containers["bronze"],
                       to_entity = containers["silver"],
                       description = "Data moves from bronze to silver layer"
                    )

    if 'gcs_silver' in services and 'gcs_gold' in services:
        om.add_lineage(from_entity = containers["silver"],
                       to_entity = containers["gold"],
                       description = "Data moves from silver to gold layer"
        )

    # Gold container to Postgres table lineage
    if 'gcs_gold' in services and 'gold_data' in tables:
        om.add_lineage(from_entity = containers["gold"],
                       to_entity = tables["gold_data"],
                       description = "Gold data loaded to Postgres"
        )
    print("View at: http://localhost:8585")


if __name__ == "__main__":
    register_project()