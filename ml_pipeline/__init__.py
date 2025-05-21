# ml_pipeline/__init__.py
from dagster import Definitions
from .assets import data, features, model, evaluation

defs = Definitions(
    assets=[*data.assets],
)