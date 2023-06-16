from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess


class PurviewService:
    client: PurviewClient = None

    def __init__(self, tenant_id, client_id, client_secret, account_name):
        oauth = ServicePrincipalAuthentication(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        self.client = PurviewClient(account_name=account_name, authentication=oauth)
        self.collection_pulled = None

    def upload_entities(self, entities):
        return self.client.upload_entities(entities)

    # finds tables where lineage needs to be pulled from Unity Catalog
    def get_collection(self, collection_name):
        tables_collection = []
        catalogs_collection = []
        catalogs_name_list = []

        coll_gen = self.client.collections.list_collections()

        collection_id = None
        for coll in coll_gen:
            if coll["friendlyName"] == collection_name:
                collection_id = coll["name"]

        collection_pulled = self.client.discovery.query(filter={"collectionId": collection_id})
        for each in collection_pulled['value']:
            if each['entityType'] == "databricks_table":
                each.update(PurviewService._get_catalog_schema_table_names(each['qualifiedName']))

                tables_collection.append(each)

                if each["catalog"] not in catalogs_name_list:
                    catalogs_name_list.append(each["catalog"])

        catalogs_collection = PurviewService._get_catalog_collection_from_names(catalogs_name_list, collection_pulled["value"])

        #            elif each['entityType'] == "databricks_catalog":
        #                catalogs_collection.append(each)

        return tables_collection, catalogs_collection

    @staticmethod
    def _get_catalog_collection_from_names(catalogs_name_list, full_collection):
        catalogs_to_return = []
        for name in catalogs_name_list:
            temp_catalog = PurviewService._find_catalog_from_name(name, full_collection)
            if temp_catalog:
                catalogs_to_return.append(temp_catalog)

        return catalogs_to_return

    @staticmethod
    def _get_catalog_schema_table_names(table_qualified_name):
        split_str = table_qualified_name.split("/")
        return {"catalog": split_str[4], "schema": split_str[6], "table": split_str[8]}

    @staticmethod
    def _find_catalog_from_name(catalog_name, collection_list):
        for coll_item in collection_list:
            if catalog_name == coll_item["name"]:
                return coll_item

        return None

    @staticmethod
    def create_notebook_entity(name, ms_id, full_link, inputs, outputs, input_table, output_table):
        return AtlasProcess(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/notebook/{name}/{input_table}->{output_table}',
            typeName="databricks_notebook_custom",
            inputs=inputs,
            outputs=outputs,
            attributes={
                "asset_link": full_link
            }
        )

    @staticmethod
    def create_job_entity(name, ms_id, full_link, inputs, outputs, input_table, output_table):
        return AtlasProcess(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/job/{name}/{input_table}->{output_table}',
            typeName="databricks_job_custom",
            inputs=inputs,
            outputs=outputs,
            attributeDefs={
                "asset_link": full_link
            }
        )

    # currently unsused, will add migration of the basic assets in the future
    @staticmethod
    def create_catalog_entity(name, ms_id):
        return AtlasEntity(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/catalogs/{name}',
            typeName="databricks_catalog",
            attributes={"assetType": ["Databricks UC"]}
        )

    @staticmethod
    def create_schema_entity(name, ms_id):
        return AtlasEntity(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/catalogs/{name}',
            typeName="databricks_schema",
            attributes={"assetType": ["Databricks UC"]}
        )

    @staticmethod
    def create_table_entity(name, ms_id):
        return AtlasEntity(
            name=name,
            qualified_name="data"
        )