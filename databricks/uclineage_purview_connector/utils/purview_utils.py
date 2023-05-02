from pyapacheatlas.core.typedef import AtlasAttributeDef, EntityTypeDef


def create_delete_entities(purview_client, delete=False):
    # notebook process
    notebook_entity = EntityTypeDef(
        name="databricks_uc_notebook",
        superTypes=["Process"],
        serviceType="Databricks Unity Catalog",
        attributeDefs=[
            AtlasAttributeDef("columnMapping"),
            AtlasAttributeDef("asset_link", type="string", isOptional=True)
        ]
    )

    # job process
    job_entity = EntityTypeDef(
        name="databricks_uc_job",
        superTypes=["Process"],
        serviceType="Databricks Unity Catalog",
        attributeDefs=[
            AtlasAttributeDef("columnMapping"),
            AtlasAttributeDef("asset_link", type="string", isOptional=True)
        ]
    )

    entities = [notebook_entity, job_entity]

    if not delete:
        return purview_client.upload_typdefs(entityDefs=entities)
    else:
        purview_client.delete_typedefs(entityDefs=entities)
