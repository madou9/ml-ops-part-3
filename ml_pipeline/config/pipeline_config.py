import os
from dagster import RunConfig
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get credentials from environment
endpoint_url = os.getenv("LAKEFS_ENDPOINT_URL")
access_key_id = os.getenv("LAKEFS_ACCESS_KEY_ID")
secret_access_key = os.getenv("LAKEFS_SECRET_ACCESS_KEY")
repository = os.getenv("LAKEFS_REPOSITORY")

print(f"Testing connection to LakeFS at {endpoint_url}")
print(f"Repository: {repository}")


def get_lakefs_config():
    """Get LakeFS configuration from environment variables"""
    return {
        "endpoint_url": os.getenv("LAKEFS_ENDPOINT_URL"),
        "access_key_id": os.getenv("LAKEFS_ACCESS_KEY_ID"),
        "secret_access_key": os.getenv("LAKEFS_SECRET_ACCESS_KEY"),
        "repository": os.getenv("LAKEFS_REPOSITORY"),
        "branch": os.getenv("LAKEFS_BRANCH")
    }


def get_data_config():
    """Get data configuration"""
    return {
        "csv_file_path": os.getenv("CSV_FILE_PATH", "data.csv")
    }


# Default run configuration
default_run_config = RunConfig(
    resources={
        "lakefs_io_manager": get_lakefs_config()
    },
    ops={
        "raw_data": get_data_config()
    }
)