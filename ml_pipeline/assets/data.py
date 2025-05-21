# ml_pipeline/assets/data.py
from dagster import asset

@asset
def raw_data():
    """
    Load data
    """
    # implementation here
    return "raw data"

@asset
def processed_data(raw_data):
    # implementation here
    return f"processed {raw_data}"

# Important: expose your assets as a list
assets = [raw_data, processed_data]