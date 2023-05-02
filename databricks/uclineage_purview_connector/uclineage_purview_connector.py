import os
from service.purview_service import PurviewService
from service.uc_service import UCService
import json


class UCLineagePurviewConnector:
    purview_service: PurviewService = None
    uc_service: UCService = None
    purview_collection_name: str = None
    databricks_host: str = None

    def __init__(self, warehouse_http_host_path, purview_collection_name, azure_account_name="purviewUC"):
        self.databricks_host = os.environ.get("DATABRICKS_HOST_LINEAGE")

        self.purview_service = PurviewService(os.environ.get("AZURE_TENANT_ID"),
                                              os.environ.get("AZURE_CLIENT_ID"),
                                              os.environ.get("AZURE_CLIENT_SECRET"),
                                              azure_account_name)

        self.uc_service = UCService(os.getenv("DATABRICKS_HOST_LINEAGE"), os.getenv("DATABRICKS_ACCESS_TOKEN_LINEAGE"),
                                    warehouse_http_host_path)

        self.purview_collection_name = purview_collection_name

    def migrate_lineage(self, purview_collection):
        purview_tables, purview_catalogs = self.purview_service.get_collection(purview_collection)

        catalog_names = [catalog['name'] for catalog in purview_catalogs]
        catalog_table_lineage, catalog_column_lineage = self.uc_service.get_system_table_lineage(catalog_names)

        for catalog_name, table_lineage_item in catalog_table_lineage.items():
            processes_to_create = {}
            lineage_to_purview = []

            for table_lineage_row in table_lineage_item:
                if table_lineage_row["source_table_full_name"] and table_lineage_row["target_table_full_name"]:

                    process_entity_to_create = None

                    source_table_qual_name = UCLineagePurviewConnector._get_full_table_name(table_lineage_row["source_table_full_name"], purview_tables)
                    target_table_qual_name = UCLineagePurviewConnector._get_full_table_name(table_lineage_row["target_table_full_name"], purview_tables)

                    if table_lineage_row["entity_type"] == "NOTEBOOK":
                        # todo: get notebook name
                        process_entity_to_create = PurviewService.create_notebook_entity(table_lineage_row["entity_id"],
                                                                                        table_lineage_row["metastore_id"],
                                                                                        self._create_notebook_link(table_lineage_row),
                                                                                        source_table_qual_name,
                                                                                        target_table_qual_name)

                    elif table_lineage_row["entity_type"] == "JOB":
                        process_entity_to_create = PurviewService.create_notebook_entity(table_lineage_row["entity_id"],
                                                                                        table_lineage_row["metastore_id"],
                                                                                        self._create_notebook_link(table_lineage_row),
                                                                                        source_table_qual_name,
                                                                                        target_table_qual_name)
                    else:
                        continue  # just in case there's other entity types

                    column_lineage_map = UCLineagePurviewConnector._create_lineage_map(source_table_qual_name, target_table_qual_name, catalog_column_lineage[catalog_name])
                    if column_lineage_map:
                        process_entity_to_create.attributes["columnMapping"] = json.dumps(column_lineage_map)
                    if process_entity_to_create:
                        lineage_to_purview.append(process_entity_to_create)

                    # todo: upload lineage to purview actually

    @staticmethod
    def _create_lineage_map(source_table_qual_name, target_table_qual_name, catalog_column_lineage):
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

    def _create_notebook_link(self, table_lineage_item):
        return f'https://{self.databricks_host}/o={table_lineage_item["workspace_id"]}#notebook/{table_lineage_item["entity_id"]}'

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
