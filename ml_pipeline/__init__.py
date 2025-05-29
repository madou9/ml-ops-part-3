# ml_pipeline/__init__.py
from dagster import Definitions
from .assets import data, features, model, evaluation
from .io_managers.lakefs_io_manager import lakefs_io_manager
from .config.pipeline_config import get_lakefs_config
import os


defs = Definitions(
    assets=[*data.assets],
     resources={
        "lakefs_io_manager": lakefs_io_manager.configured(get_lakefs_config()),
        "mlflow": {
            "config": {
                "tracking_uri": os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"),
                "experiment_name": os.environ.get("MLFLOW_EXPERIMENT_NAME", "spotify_experiment"),
            }
        }
    }
)
