from mlflow.tracking import MlflowClient

client = MlflowClient()

def get_production_metrics(model_name):
    try:
        versions = client.get_latest_versions(model_name, stages=["Production"])
        if not versions:
            return None

        run_id = versions[0].run_id
        run = client.get_run(run_id)

        return run.data.metrics

    except Exception:
        return None


def should_promote(new_metrics, current_metrics=None):
    """
    Conditional promotion of model to Production
    """

    if new_metrics["auc"] < new_metrics["min_auc"]:
        return "REJECT"

    if new_metrics["recall_churn"] < new_metrics["min_recall"]:
        return "REJECT"

    # First model runs
    if current_metrics is None:
        return "PROMOTE"

    auc_better = new_metrics["auc"] >= (
        current_metrics["auc"] + new_metrics["auc_improvement"]
    )

    recall_better = new_metrics["recall_churn"] >= (
        current_metrics["recall_churn"] + new_metrics["recall_improvement"]
    )

    if auc_better or recall_better:
        return "PROMOTE"

    return "STAGING"
