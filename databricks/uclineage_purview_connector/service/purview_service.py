from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import PurviewClient, AtlasEntity, AtlasProcess


class PurviewService:
    client: PurviewClient = None
    tables_collection: list = []
    catalogs_collection: list = []

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

    def get_collection(self, collection_name):
        coll_gen = self.client.collections.list_collections()

        collection_id = None
        for coll in coll_gen:
            if coll["friendlyName"] == collection_name:
                collection_id = coll["name"]

        collection_pulled = self.client.discovery.query(filter={"collectionId": collection_id})

        for each in collection_pulled['value']:
            if each['entityType'] == "databricks_table":
                each.update(PurviewService._get_catalog_schema_table_names(each['qualifiedName']))

                self.tables_collection.append(each)

            elif each['entityType'] == "databricks_catalog":
                self.catalogs_collection.append(each)

        return self.tables_collection, self.catalogs_collection

    @staticmethod
    def _get_catalog_schema_table_names(table_qualified_name):
        split_str = table_qualified_name.split("/")
        return {"catalog": split_str[4], "schema": split_str[6], "table": split_str[8]}

    @staticmethod
    def create_notebook_entity(name, ms_id, full_link, inputs, outputs):
        return AtlasProcess(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/notebook/{name}',
            typeName="databricks_notebook",
            inputs=inputs,
            outputs=outputs,
            attributes={
                "asset_link": full_link
            }
        )

    @staticmethod
    def create_job_entity(name, ms_id, full_link, inputs, outputs):
        return AtlasProcess(
            name=name,
            qualified_name="databricks://" + f'{ms_id}/job/{name}',
            typeName="databricks_job",
            inputs=inputs,
            outputs=outputs,
            attributes={
                "asset_link": full_link
            }
        )