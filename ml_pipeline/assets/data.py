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
import mlflow
import mlflow.sklearn 

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
    TARGET = 'is_popular'
    FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    return processed_data[FEATURES + [TARGET]]


@asset(io_manager_key="lakefs_io_manager", required_resource_keys={"mlflow"})
def train_with_mlflow(context, features: pd.DataFrame) -> None:
    """
    Train a logistic regression model using MLflow and log the results.
    """
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score

    X = features.drop(columns=["is_popular"])
    y = features["is_popular"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run():
        model = LogisticRegression(class_weight='balanced', max_iter=1000, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)
        acc = accuracy_score(y_test, y_pred)
        context.log.info(f"Run ID: {mlflow.active_run().info.run_id}")
        context.log.info(f"experiment ID: {mlflow.active_run().info.experiment_id}")
        context.log.info(f"experiment Name: {mlflow.get_experiment(mlflow.active_run().info.experiment_id).name}")
        context.log.info(f"mlflow host: {mlflow.get_tracking_uri()}")

        mlflow.log_param("model_type", "LogisticRegression")
        mlflow.log_metric("accuracy", acc)
        mlflow.sklearn.log_model(model, "model")
        mlfow.sklearn.autolog()
        print(f"Logged model with accuracy: {acc}")

# Add to your assets list
assets = [
    raw_data,
    processed_data,
    features,
    train_with_mlflow,
]