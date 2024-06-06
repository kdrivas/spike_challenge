"""
    This file contains functions for model training and validation
"""
import os
import joblib
import json
import pandas as pd

from pipelines.music_model.constants import TARGET_COL
from sklearn.ensemble import GradientBoostingClassifier


def training_model(data_path: str, artifact_path: str) -> None:
    """
        This function will train the model using the preprocessed data
        Once the model is trained, the serialized model is stored in the artifact_path
    """
    train = pd.read_csv(os.path.join(data_path, "preprocessed/prec_merged.csv"))

    with open(os.path.join(artifact_path, "best_params.json")) as f:
        params = json.load(f)

    # Prediction pipeline
    # I"d prefer to keep just the model
    classifier_GB = GradientBoostingClassifier(**params)

    X_train, y_train = train.drop(TARGET_COL, axis=1), train[TARGET_COL]

    model = classifier_GB.fit(X_train, y_train)

    joblib.dump(model, os.path.join(artifact_path, "model_staging.pkl"))
    

def validate_promote_model(artifact_path: str) -> None:
    """
        Implements a set of validation for the model to promote it 
    """
    with open(os.path.join(artifact_path, "model_metrics.json")) as json_file:
        metrics = json.load(json_file)

    model = joblib.load(os.path.join(artifact_path, "model_staging.pkl"))

    # Dummy validation for RMSE and R2
    assert metrics["RMSE"] > 0 and metrics["r2"] > 0

    # TO-DO: Validate the last model perform better than the previous model
    print("Everythin Ok :D")

    joblib.dump(model, os.path.join(artifact_path, "model_prod.pkl"))

def batch_prediction_model(artifact_path: str, data_path: str) -> None:
    """
        Predict batch data
    """

    # Read historic features from our feature store
    features = pd.read_csv(os.path.join(data_path, "feature_store", "super_efficient_feature_store.csv"))

    model = joblib.load(os.path.join(artifact_path, "model_prod.pkl"))

    pred = model.predict(features.drop("Periodo", axis=1))
    features["preds"] = pred

    features[["Periodo", "preds"]].to_csv(os.path.join(data_path, "batch_predictions", "predictions.csv"), index=False)
