# uclineage_purview_connector

## Purpose

This connector will use a Databricks SQL Warehouse to import lineage from Unity Catalog using system tables into a Purview instance. The script is meant to be used after the native Purview connector which migrates Unity Catalog assets (catalogs, schemas, tables, columns).

## Usage

1) Install requirements (from requirements.txt)
2) Set the following environment variables:
  - DATABRICKS_HOST_LINEAGE (ex.adb-5343834423590926.6.azuredatabricks.net)
  - DATABRICKS_ACCESS_TOKEN_LINEAGE (Databricks PAT token)
  - AZURE_TENANT_ID
  - AZURE_CLIENT_ID
  - AZURE_CLIENT_SECRET

3) Start a Databricks SQL Warehouse in your Databricks workspace and copy the HTTP Path of the Warehouse
4) Import the connector in your Python interpreter (in directory databricks/uclineage_purview_connector) and initialize the connector using the warehouse http path and Azure account name (where the Purview instance resides):
```python          
          from uclineage_purview_connector import UCLineagePurviewConnector
          connector = UCLineagePurviewConnector(warehouse_http_host_path=<warehouse_http_host_path>, azure_account_name=<azure_account_name>)
```
5) Run the migration specifying the Purview collection which contains the already migrated UC assets (without lineage):
```python
        connector.migrate_lineage(purview_collection=<collection_name>)
```
