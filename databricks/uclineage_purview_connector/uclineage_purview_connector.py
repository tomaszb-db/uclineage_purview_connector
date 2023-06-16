import os
from service.purview_service import PurviewService
from service.uc_service import UCService
import json


class UCLineagePurviewConnector:
    purview_service: PurviewService = None
    uc_service: UCService = None
    purview_collection_name: str = None
    databricks_host: str = None

    # initialization of connector, need to provide Azure account name for where the Purview instance resides
    # environment variables defined as follows: DATABRICKS_HOST_LINEAGE (ex.adb-5343834423590926.6.azuredatabricks.net or demo-field-eng-test.cloud.databricks.com)
    #                                           DATABRICKS_ACCESS_TOKEN_LINEAGE
    #                                           AZURE_TENANT_ID
    #                                           AZURE_CLIENT_ID
    #                                           AZURE_CLIENT_SECRET
    def __init__(self, warehouse_http_host_path, azure_account_name="purviewUC"):
        self.databricks_host = os.environ.get("DATABRICKS_HOST_LINEAGE")

        self.purview_service = PurviewService(os.environ.get("AZURE_TENANT_ID"),
                                              os.environ.get("AZURE_CLIENT_ID"),
                                              os.environ.get("AZURE_CLIENT_SECRET"),
                                              azure_account_name)

        self.uc_service = UCService(os.getenv("DATABRICKS_HOST_LINEAGE"), os.getenv("DATABRICKS_ACCESS_TOKEN_LINEAGE"),
                                    warehouse_http_host_path)

    def migrate_lineage(self, purview_collection):
        purview_tables, purview_catalogs = self.purview_service.get_collection(purview_collection)

        catalog_names = [catalog['name'] for catalog in purview_catalogs]
        print(f'Catalogs pulled from Purview: {catalog_names}')

        catalog_table_lineage, catalog_column_lineage = self.uc_service.get_system_table_lineage(catalog_names)

        for catalog_name, table_lineage_item in catalog_table_lineage.items():
            lineage_to_purview = []

            for table_lineage_row in table_lineage_item:
                source_table_qual_name = UCLineagePurviewConnector._get_full_table_name(table_lineage_row["source_table_full_name"], purview_tables)
                target_table_qual_name = UCLineagePurviewConnector._get_full_table_name(table_lineage_row["target_table_full_name"], purview_tables)
                if source_table_qual_name and target_table_qual_name:
                    process_entity_to_create = None

                    proc_input = [{
                        "typeName": "databricks_table",
                        "uniqueAttributes": {"qualifiedName": source_table_qual_name}
                    }]
                    proc_output = [{
                        "typeName": "databricks_table",
                        "uniqueAttributes": {"qualifiedName": target_table_qual_name}
                    }]

                    if table_lineage_row["entity_type"] == "NOTEBOOK":
                        process_entity_to_create = PurviewService.create_notebook_entity(table_lineage_row["entity_id"],
                                                                                         table_lineage_row["metastore_id"],
                                                                                         self._create_notebook_link(table_lineage_row),
                                                                                         proc_input,
                                                                                         proc_output,
                                                                                         table_lineage_row["source_table_name"],
                                                                                         table_lineage_row["target_table_name"]
                                                                                         )

                    elif table_lineage_row["entity_type"] == "JOB":
                        process_entity_to_create = PurviewService.create_notebook_entity(table_lineage_row["entity_id"],
                                                                                         table_lineage_row["metastore_id"],
                                                                                         self._create_notebook_link(table_lineage_row),
                                                                                         proc_input,
                                                                                         proc_output,
                                                                                         table_lineage_row["source_table_name"],
                                                                                         table_lineage_row["target_table_name"]
                                                                                         )
                    else:
                        continue  # just in case there's other entity types

                    column_lineage_map = UCLineagePurviewConnector._create_lineage_map(source_table_qual_name, target_table_qual_name, catalog_column_lineage[catalog_name])
                    if column_lineage_map and process_entity_to_create:
                        process_entity_to_create.attributes["columnMapping"] = json.dumps(column_lineage_map)
                        lineage_to_purview.append(process_entity_to_create)

                    if lineage_to_purview:
                        result = self.purview_service.upload_entities(lineage_to_purview)

                        print(result)

    # creates column lineage map in Purview format
    @staticmethod
    def _create_lineage_map(source_table_qual_name, target_table_qual_name, catalog_column_lineage):
        print(f'Creating lineage map for {source_table_qual_name} and {target_table_qual_name}')
        column_map_cols = []

        source_table_uc_name = UCLineagePurviewConnector._get_uc_full_table_name(source_table_qual_name)
        target_table_uc_name = UCLineagePurviewConnector._get_uc_full_table_name(target_table_qual_name)

        for col_lineage_row in catalog_column_lineage:
            if source_table_uc_name == col_lineage_row["source_table_full_name"] and target_table_uc_name == col_lineage_row["target_table_full_name"]:
                column_map_cols.append({"Source": col_lineage_row["source_column_name"], "Sink": col_lineage_row["target_column_name"]})

        if column_map_cols:
            full_column_json = [{
                "DatasetMapping": {
                    "Source": source_table_qual_name, "Sink": target_table_qual_name
                },
                "ColumnMapping": column_map_cols
            }]

            return full_column_json
        else:
            return None

    # creates URL link so that it can be found in the Purview properties of the entity
    def _create_notebook_link(self, table_lineage_item):
        return f'https://{self.databricks_host}/?o={table_lineage_item["workspace_id"]}#notebook/{table_lineage_item["entity_id"]}'

    @staticmethod
    def _get_full_table_name(unity_full_name, purview_tables):
        for each in purview_tables:
            if f'{each["catalog"]}.{each["schema"]}.{each["table"]}' == unity_full_name:
                return each['qualifiedName']

    @staticmethod
    def _get_catalog_schema_table_names(table_qualified_name):
        split_str = table_qualified_name.split("/")
        return {"qualified_name": table_qualified_name, "catalog": split_str[4], "schema": split_str[6],
                "table": split_str[8]}

    @staticmethod
    def _get_uc_full_table_name(table_qualified_name):
        split_str = table_qualified_name.split("/")
        return f'{split_str[4]}.{split_str[6]}.{split_str[8]}'
