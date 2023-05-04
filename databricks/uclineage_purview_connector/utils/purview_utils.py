from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef


def create_delete_entities(purview_client, delete=False):
    # notebook process
    notebook_entity = EntityTypeDef(
        name="databricks_notebook_custom",
        superTypes=["databricks_notebook"],
        serviceType="Databricks Unity Catalog",
        attributeDefs=[
            AtlasAttributeDef("columnMapping"),
            AtlasAttributeDef("asset_link", type="string", isOptional=True)
        ]
    )

    # job process
    job_entity = EntityTypeDef(
        name="databricks_job_custom",
        superTypes=["databricks_job"],
        serviceType="Databricks Unity Catalog",
        attributeDefs=[
            AtlasAttributeDef("columnMapping"),
            AtlasAttributeDef("asset_link", type="string", isOptional=True)
        ]
    )

    entities = [notebook_entity, job_entity]

    if not delete:
        return purview_client.client.upload_typedefs(entityDefs=entities)
    else:
        purview_client.client.delete_typedefs(entityDefs=entities)
