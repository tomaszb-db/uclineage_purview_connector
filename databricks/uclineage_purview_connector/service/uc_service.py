import os
from databricks import sql


class UCService:
    sql_client: sql = None
    table_lineage: list = []
    column_lineage: list = []

    # uc_serv = UCService(os.getenv("DATABRICKS_HOST_LINEAGE"), os.getenv("DATABRICKS_ACCESS_TOKEN_LINEAGE"), "/sql/1.0/warehouses/a8ea19c60e483430")
    def __init__(self, db_host, db_access_token, sql_warehouse_http_path):
        self.sql_client = sql.connect(db_host, sql_warehouse_http_path, db_access_token)

    def get_system_table_lineage(self, target_table_catalog):
        lineage_tables = {}
        column_lineage_tables = {}

        for catalog in target_table_catalog:
            with self.sql_client.cursor() as cursor:
                cursor.execute(
                    f'SELECT * from system.lineage.table_lineage WHERE target_table_catalog = "{catalog}"')
                # check if not empty
                temp_table_lineage = cursor.fetchall()
                if temp_table_lineage:
                    lineage_tables[catalog] = temp_table_lineage

                cursor.execute(
                    f'SELECT * FROM system.lineage.column_lineage WHERE target_table_catalog = "{catalog}"')
                # check if not empty
                temp_column_lineage = cursor.fetchall()
                if temp_column_lineage:
                    column_lineage_tables[catalog] = temp_column_lineage

        return lineage_tables, column_lineage_tables
