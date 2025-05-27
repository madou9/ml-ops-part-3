import pandas as pd
import os
from dagster import asset, Config
from typing import Dict, Any, Tuple
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report

class DataConfig(Config):
    csv_file_path: str = "ml_pipeline/assets/spotify_data.csv"

@asset(io_manager_key="lakefs_io_manager")
def raw_data(config: DataConfig) -> pd.DataFrame:
    """
    Load raw data from CSV file into a DataFrame.
    This asset reads a CSV file specified in the configuration and returns a DataFrame.
    """
    if not os.path.exists(config.csv_file_path):
        raise FileNotFoundError(f"CSV file not found at {config.csv_file_path}")
    df = pd.read_csv(config.csv_file_path)
    return df

@asset(io_manager_key="lakefs_io_manager")
def processed_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    """
    Process raw data to create a binary target variable.
    This asset processes the raw data by dropping missing values and creating a binary target variable
    based on the 'popularity' column.
    """
    df = raw_data.copy()
    df = df.dropna()
    df['is_popular'] = (df['popularity'] >= 50).astype(int)
    return df

@asset(io_manager_key="lakefs_io_manager")
def features(processed_data: pd.DataFrame) -> pd.DataFrame:
    """
    Extract features from the processed data.
    This asset selects relevant features from the processed data for model training.
    """
    NUMERIC_FEATURES = [
        'instrumentalness', 'duration_ms', 'danceability', 'loudness',
        'acousticness', 'energy', 'valence', 'speechiness'
    ]
    CATEGORICAL_FEATURES = ['year']
    FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    return processed_data[FEATURES]

@asset(io_manager_key="lakefs_io_manager")
def target(processed_data: pd.DataFrame) -> pd.Series:
    """
    Create target variable from processed data.
    This asset extracts the target variable 'is_popular' from the processed data.
    """
    return processed_data['is_popular']

    

assets = [
    raw_data,
    processed_data,
    features,
    target,
]