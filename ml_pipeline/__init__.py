# ml_pipeline/__init__.py
from dagster import Definitions
from .assets import data, features, model, evaluation
from .io_managers.lakefs_io_manager import lakefs_io_manager
from .config.pipeline_config import get_lakefs_config

defs = Definitions(
    assets=[*data.assets],
     resources={
        "lakefs_io_manager": lakefs_io_manager.configured(get_lakefs_config())
    }
)
