from mlflow.tracking import MlflowClient

client = MlflowClient()

def apply_registry_decision(model_name, run_id, decision):
    versions = client.search_model_versions(
        f"name='{model_name}' and run_id='{run_id}'"
    )
    model_version = versions[0].version

    if decision == "PROMOTE":
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Production",
            archive_existing_versions=True
        )

    elif decision == "STAGING":
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Staging"
        )

    elif decision == "REJECT":
        client.transition_model_version_stage(
            name=model_name,
            version=model_version,
            stage="Archived"
        )
