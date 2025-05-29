
# mlflow resource definition
from dagster import resource, Field
from mlflow import MlflowClient
from typing import Dict, Any
@resource(
    config_schema={
        "tracking_uri": Field(str, is_required=True, description="MLflow tracking URI"),
        "experiment_name": Field(str, is_required=True, description="MLflow experiment name"),
    },
    description="Resource for managing MLflow tracking",
)
def mlflow_resource(init_context) -> MlflowClient:
    """
    MLflow resource for tracking experiments and models.
    
    Args:
        init_context: Initialization context containing resource configuration.
    
    Returns:
        MlflowClient instance configured with the provided tracking URI and experiment name.
    """
    tracking_uri = init_context.resource_config["tracking_uri"]
    experiment_name = init_context.resource_config["experiment_name"]
    
    # Set the tracking URI
    mlflow.set_tracking_uri(tracking_uri)
    
    # Create or get the experiment
    mlflow.set_experiment(experiment_name)
    
    return MlflowClient()