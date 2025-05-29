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
        """
        Initialize LakeFS IO Manager
        
        Args:
            endpoint_url: LakeFS server URL (e.g., "http://localhost:8000")
            access_key_id: LakeFS access key
            secret_access_key: LakeFS secret key
            repository: LakeFS repository name
            branch: LakeFS branch name (default: "main")
        """
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
    
    def _get_path(self, context) -> str:
        """Generate path for the asset in LakeFS"""
        return f"data/{context.asset_key.path[-1]}.parquet"
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Store data in LakeFS"""
        path = self._get_path(context)
        
        try:
            # Convert to DataFrame if it's not already
            if not isinstance(obj, pd.DataFrame):
                if hasattr(obj, 'to_pandas'):
                    df = obj.to_pandas()
                else:
                    df = pd.DataFrame(obj)
            else:
                df = obj
            
            # Save to temporary file
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                df.to_parquet(tmp_file.name, index=False)
                
                # Upload to LakeFS
                with open(tmp_file.name, 'rb') as f:
                    self.client.objects_api.upload_object(
                        repository=self.repository,
                        branch=self.branch,
                        path=path,
                        content=f.read()
                    )
                
                # Clean up temporary file
                os.unlink(tmp_file.name)
            
            context.log.info(f"Stored data at lakefs://{self.repository}/{self.branch}/{path}")
            
        except Exception as e:
            context.log.error(f"Failed to store data in LakeFS: {str(e)}")
            raise
    
    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load data from LakeFS"""
        path = self._get_path(context)
        
        try:
            # Download object from LakeFS
            response = self.client.objects_api.get_object(
                repository=self.repository,
                ref=self.branch,
                path=path
            )
            
            # Save to temporary file and read
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                tmp_file.write(response)
                tmp_file.flush()
                df = pd.read_parquet(tmp_file.name)
                # Clean up temporary file
                os.unlink(tmp_file.name)
            return df
            
        except Exception as e:
            context.log.error(f"Failed to load data from LakeFS: {str(e)}")
            raise


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