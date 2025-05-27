# ml_pipeline/io_managers/lakefs_io_manager.py
import pandas as pd
import os
from typing import Any
from dagster import IOManager, InputContext, OutputContext, io_manager, Field
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models import ObjectStats
import tempfile
import os


class LakeFSIOManager(IOManager):
    def __init__(self, endpoint_url: str, access_key_id: str, secret_access_key: str, repository: str, branch: str = "main"):
        self.endpoint_url = endpoint_url
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.repository = repository
        self.branch = branch
        
        # Initialize LakeFS client
        configuration = lakefs_sdk.Configuration(
            host=endpoint_url,
            username=access_key_id,
            password=secret_access_key,
        )
        self.client = LakeFSClient(configuration)


@io_manager(config_schema={
    "endpoint_url": str,
    "access_key_id": str, 
    "secret_access_key": str,
    "repository": str,
    "branch": Field(str, default_value="main")
})
def lakefs_io_manager(init_context) -> LakeFSIOManager:
    """Factory function for LakeFS IO Manager"""
    return LakeFSIOManager(
        endpoint_url=init_context.resource_config["endpoint_url"],
        access_key_id=init_context.resource_config["access_key_id"],
        secret_access_key=init_context.resource_config["secret_access_key"],
        repository=init_context.resource_config["repository"],
        branch=init_context.resource_config.get("branch", "main")
    )